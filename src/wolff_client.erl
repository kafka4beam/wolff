%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(wolff_client).

-include_lib("kafka_protocol/include/kpro.hrl").
-include("wolff.hrl").

%% APIs
-export([start_link/3, stop/1]).
-export([get_leader_connections/2,
         get_leader_connections/3,
         recv_leader_connection/5,
         get_id/1,
         delete_producers_metadata/2]).
-export([check_connectivity/1, check_connectivity/2]).
-export([check_topic_exists_with_client_pid/2]).

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

-export_type([config/0, producer_alias/0, alias_and_topic/0, topic_or_alias/0, max_partitions/0]).

%% deprecated
-export([check_if_topic_exists/3]).

-type config() :: map().
-type topic() :: kpro:topic().
-type producer_alias() :: binary().
-type alias_and_topic() :: {producer_alias() | ?NO_ALIAS, topic()}.
-type topic_or_alias() :: topic() | alias_and_topic().
-type partition() :: kpro:partition().
-type connection() :: kpro:connection().
-type host() :: wolff:host().
-type conn_id() :: {topic(), partition()} | host().
-type max_partitions() :: pos_integer() | ?all_partitions.

-type state() ::
      #{client_id := wolff:client_id(),
        seed_hosts := [host()],
        config := config(),
        conn_config := kpro:conn_config(),
        conns := #{conn_id() => connection()},
        metadata_conn := pid() | not_initialized | down,
        metadata_ts := #{topic() => erlang:timestamp()},
        %% only applicable when connection strategy is per_broker
        %% because in this case the connections are keyed by host()
        %% but we need to find connection by {topic(), partition()}
        leaders => #{{topic(), partition()} => connection()},
        %% Reference counting so we may drop connection metadata when no longer required.
        known_topics := #{topic() => #{producer_alias() | ?NO_ALIAS => true}}
       }.

-define(DEFAULT_METADATA_TIMEOUT, 10000).
-define(MIN_METADATA_REFRESH_INTERVAL, 1000).

%% @doc Start a client.
%% The started `gen_server' manages connections towards kafka brokers.
%% Besides `kpro_connection' configs, `Config' also support below entries:
%% * `reg_name' register the client process to a name. e.g. `{local, client_1}'
%% * `connection_strategy', default `per_partition', can also be `per_broker'.
%%   This is to tell client how to establish connections: one connection
%%   per-partition or one connection per-broker.
%% * `min_metadata_refresh_interval', default 3 seconds.
%%   This is to avoid excessive metadata refresh and partition leader reconnect
%%   when a lot of connections restart around the same moment
-spec start_link(wolff:client_id(), [host()], config()) ->
  {ok, pid()} | {error, any()}.
start_link(ClientId, Hosts, Config) ->
  {ConnCfg0, MyCfg} = split_config(Config),
  ConnCfg = ConnCfg0#{client_id => ClientId},
  St = #{client_id => ClientId,
         seed_hosts => Hosts,
         config => MyCfg,
         conn_config => ConnCfg,
         conns => #{},
         metadata_conn => not_initialized,
         metadata_ts => #{},
         leaders => #{},
         known_topics => #{}
        },
  case maps:get(reg_name, Config, false) of
    false -> gen_server:start_link(?MODULE, St, []);
    Name -> gen_server:start_link({local, Name}, ?MODULE, St, [])
  end.

stop(Pid) ->
  gen_server:call(Pid, stop, infinity).

get_id(Pid) ->
  gen_server:call(Pid, get_id, infinity).

-spec get_leader_connections(pid(), topic_or_alias()) ->
        {ok, [{partition(), pid() | ?conn_down(_)}]} | {error, any()}.
get_leader_connections(Client, TopicOrAlias0) ->
   TopicOrAlias = ensure_has_alias(TopicOrAlias0),
   safe_call(Client, {get_leader_connections, TopicOrAlias, all_partitions}).

-spec get_leader_connections(pid(), topic_or_alias(), all_partitions | pos_integer()) ->
        {ok, [{partition(), pid() | ?conn_down(_)}]} | {error, any()}.
get_leader_connections(Client, TopicOrAlias0, MaxPartitions) ->
   TopicOrAlias = ensure_has_alias(TopicOrAlias0),
   safe_call(Client, {get_leader_connections, TopicOrAlias, MaxPartitions}).

%% @doc Check if client has a metadata connection alive.
%% Trigger a reconnect if the connection is down for whatever reason.
-spec check_connectivity(pid()) -> ok | {error, any()}.
check_connectivity(Pid) ->
  safe_call(Pid, check_connectivity).

%% @doc Connect to any host in the list and immediately disconnect.
-spec check_connectivity([host()], kpro:conn_config()) -> ok | {error, any()}.
check_connectivity(Hosts, ConnConfig) when Hosts =/= [] ->
  case kpro:connect_any(Hosts, ConnConfig) of
    {ok, Conn} ->
      ok = close_connection(Conn);
    {error, Reasons} ->
      {error, tr_reasons(Reasons)}
  end.

%% @hidden Deprecated. Check if a topic exists by creating a temp connecton to any of the seed hosts.
-spec check_if_topic_exists([host()], kpro:conn_config(), topic()) ->
        ok | {error, unknown_topic_or_partition | [#{host := binary(), reason := term()}] | any()}.
check_if_topic_exists(Hosts, ConnConfig, Topic) when Hosts =/= [] ->
  case get_metadata(Hosts, ConnConfig, Topic) of
    {ok, {Pid, _}} ->
      ok = close_connection(Pid);
    {error, Errors} ->
      {error, Errors}
  end.

%% @doc Check if a topic exists by calling a already started client process.
%% In contrast, check_if_topic_exists/3 creates a temp connection to do the work.
-spec check_topic_exists_with_client_pid(pid(), topic()) ->
        ok | {error, unknown_topic_or_partition | any()}.
check_topic_exists_with_client_pid(Pid, Topic) ->
    safe_call(Pid, {check_if_topic_exists, Topic}).

safe_call(Pid, Call) ->
  try gen_server:call(Pid, Call, infinity)
  catch exit : Reason -> {error, Reason}
  end.

%% request client to send Pid the leader connection.
recv_leader_connection(Client, Topic, Partition, Pid, MaxPartitions) ->
  gen_server:cast(Client, {recv_leader_connection, Topic, Partition, Pid, MaxPartitions}).

delete_producers_metadata(Client, TopicOrAlias) ->
    gen_server:cast(Client, {delete_producers_metadata, TopicOrAlias}).

init(#{client_id := ClientID} = St) ->
  erlang:process_flag(trap_exit, true),
  ok = wolff_client_sup:register_client(ClientID),
  {ok, St}.

handle_call(Call, From, #{connect := _Fun} = St) ->
    handle_call(Call, From, upgrade(St));
handle_call(get_id, _From, #{client_id := Id} = St) ->
  {reply, Id, St};
handle_call({check_if_topic_exists, Topic}, _From, #{conn_config := ConnConfig} = St0) ->
  case ensure_metadata_conn(St0) of
    {ok, #{metadata_conn := ConnPid} = St} ->
      Timeout = maps:get(request_timeout, ConnConfig, ?DEFAULT_METADATA_TIMEOUT),
      {reply, check_if_topic_exists2(ConnPid, Topic, Timeout), St};
    {error, Reason} ->
      {reply, {error, Reason}, St0}
  end;
handle_call({get_leader_connections, TopicOrAlias, MaxPartitions}, _From, St0) ->
  case ensure_leader_connections(St0, TopicOrAlias, MaxPartitions) of
    {ok, St} ->
      Result = do_get_leader_connections(St, TopicOrAlias),
      {reply, {ok, Result}, St};
    {error, Reason} ->
      {reply, {error, Reason}, St0}
  end;
handle_call(stop, From, #{conns := Conns} = St) ->
  ok = close_connections(Conns),
  gen_server:reply(From, ok),
  {stop, normal, St#{conns := #{}}};
handle_call(check_connectivity, _From, St0) ->
  case ensure_metadata_conn(St0) of
    {ok, St} ->
      {reply, ok, St};
    {error, Reason} ->
      {reply, {error, Reason}, St0}
  end;
handle_call(Call, _From, St) ->
  {reply, {error, {unknown_call, Call}}, St}.

handle_info(_Info, St) ->
  {noreply, upgrade(St)}.

handle_cast(Cast, #{connect := _Fun} = St) ->
    handle_cast(Cast, upgrade(St));
handle_cast({recv_leader_connection, Topic, Partition, Caller, MaxConnections}, St0) ->
  case ensure_leader_connections(St0, Topic, MaxConnections) of
    {ok, St} ->
      Partitions = do_get_leader_connections(St, Topic),
      %% the Partition in argument is a result of ensure_leader_connections
      %% so here the lists:keyfind must succeeded, otherwise a bug
      {_, MaybePid} = lists:keyfind(Partition, 1, Partitions),
      _ = erlang:send(Caller, ?leader_connection(MaybePid)),
      {noreply, St};
    {error, Reason} ->
      _ = erlang:send(Caller, ?leader_connection({error, Reason})),
      {noreply, St0}
  end;
handle_cast({delete_producers_metadata, TopicOrAlias}, St0) ->
  #{metadata_ts := Topics0,
    conns := Conns0,
    known_topics := KnownTopics0} = St0,
  Topic = get_topic(TopicOrAlias),
  Alias = get_alias(TopicOrAlias),
  case KnownTopics0 of
      #{Topic := #{Alias := true} = KnownProducers} when map_size(KnownProducers) =:= 1 ->
          %% Last entry: we may drop the connection metadata
          KnownTopics = maps:remove(Topic, KnownTopics0),
          Conns = maps:without( [K || K = {K1, _} <- maps:keys(Conns0), K1 =:= Topic], Conns0),
          Topics = maps:remove(Alias, Topics0),
          St = St0#{metadata_ts := Topics, conns := Conns, known_topics := KnownTopics},
          {noreply, St};
      #{Topic := #{Alias := true} = KnownProducers0} ->
          %% Connection is still being used by other producers.
          KnownProducers = maps:remove(Alias, KnownProducers0),
          KnownTopics = KnownTopics0#{Topic := KnownProducers},
          St = St0#{known_topics := KnownTopics},
          {noreply, St};
      _ ->
          %% Already gone; nothing to do.
          {noreply, St0}
  end;
handle_cast(_Cast, St) ->
  {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
  {ok, St}.

terminate(_, #{client_id := ClientID, conns := Conns} = St) ->
  ok = wolff_client_sup:deregister_client(ClientID),
  MetadataConn = maps:get(metadata_conn, St, none),
  ok = close_connections(Conns),
  ok = close_connection(MetadataConn),
  {ok, St#{conns := #{}}}.

%% == internals ======================================================

ensure_metadata_conn(#{seed_hosts := Hosts, conn_config := ConnConfig, metadata_conn := Pid} = St) ->
  case is_alive(Pid) of
    true ->
      {ok, St};
    false ->
      case kpro:connect_any(Hosts, ConnConfig) of
        {ok, NewPid} ->
          {ok, St#{metadata_conn => NewPid}};
        {error, Reasons} ->
          {error, tr_reasons(Reasons)}
      end
  end.

check_if_topic_exists2(Pid, Topic, Timeout) when is_pid(Pid) ->
  case do_get_metadata(Pid, Topic, Timeout) of
    {ok, _} ->
      ok;
    {error, Reason} ->
      {error, Reason}
  end.

close_connections(Conns) ->
  lists:foreach(fun({_, Pid}) -> close_connection(Pid) end, maps:to_list(Conns)).

close_connection(Conn) when is_pid(Conn) ->
  _ = erlang:spawn(fun() -> do_close_connection(Conn) end),
  ok;
close_connection(_DownConn) ->
  ok.

%% This is a copy of kpro_connection:stop which supports kill after a timeout
do_close_connection(Pid) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {{self(), Mref}, stop}),
  receive
    {Mref, _Reply} -> ok;
    {'DOWN', Mref, _, _, _Reason} -> ok
  after
    5000 ->
      exit(Pid, kill)
  end.

do_get_leader_connections(#{conns := Conns} = St, TopicOrAlias) ->
  Topic = get_topic(TopicOrAlias),
  FindInMap = case get_connection_strategy(St) of
                per_partition -> Conns;
                per_broker -> maps:get(leaders, St)
              end,
  F = fun({T, P}, MaybePid, Acc) when T =:= Topic ->
          case is_alive(MaybePid) of
            true ->
                  [{P, MaybePid} | Acc];
            false when is_pid(MaybePid) ->
                  [{P, ?conn_down(noproc)} | Acc];
            false ->
                  [{P, ?conn_down(MaybePid)} | Acc]
          end;
         (_, _, Acc) -> Acc
      end,
  maps:fold(F, [], FindInMap).

%% return true if there is no need to refresh metadata because the last one is fresh enough
is_metadata_fresh(#{metadata_ts := Topics, config := Config}, TopicOrAlias) ->
  MinInterval = maps:get(min_metadata_refresh_interval, Config, ?MIN_METADATA_REFRESH_INTERVAL),
  case maps:get(TopicOrAlias, Topics, false) of
    false -> false;
    Ts -> timer:now_diff(erlang:timestamp(), Ts) < MinInterval * 1000
  end.

-spec ensure_leader_connections(state(), topic_or_alias(), all_partitions | pos_integer()) ->
  {ok, state()} | {error, any()}.
ensure_leader_connections(St, TopicOrAlias, MaxPartitions) ->
  Topic = get_topic(TopicOrAlias),
  case is_metadata_fresh(St, Topic) of
    true -> {ok, St};
    false -> ensure_leader_connections2(St, TopicOrAlias, MaxPartitions)
  end.

-spec ensure_leader_connections2(state(), topic_or_alias(), max_partitions()) ->
          {ok, state()} | {error, term()}.
ensure_leader_connections2(#{metadata_conn := Pid, conn_config := ConnConfig} = St, TopicOrAlias, MaxPartitions) when is_pid(Pid) ->
  Topic = get_topic(TopicOrAlias),
  Timeout = maps:get(request_timeout, ConnConfig, ?DEFAULT_METADATA_TIMEOUT),
  case do_get_metadata(Pid, Topic, Timeout) of
    {ok, {Brokers, PartitionMetaList}} ->
      ensure_leader_connections3(St, TopicOrAlias, Pid, Brokers, PartitionMetaList, MaxPartitions);
    {error, _Reason} ->
      %% ensure metadata connection is down, try to establish a new one in the next clause,
      %% reason is discarded here, because the next clause will log error if the immediate retry fails
      exit(Pid, kill),
      ensure_leader_connections2(St#{metadata_conn => down}, TopicOrAlias, MaxPartitions)
  end;
ensure_leader_connections2(#{conn_config := ConnConfig,
                             seed_hosts := SeedHosts} = St, TopicOrAlias, MaxPartitions) ->
  Topic = get_topic(TopicOrAlias),
  case get_metadata(SeedHosts, ConnConfig, Topic, []) of
    {ok, {ConnPid, {Brokers, PartitionMetaList}}} ->
      ensure_leader_connections3(St, TopicOrAlias, ConnPid, Brokers, PartitionMetaList, MaxPartitions);
    {error, Errors} ->
      log_warn(failed_to_fetch_metadata, #{topic => get_topic(TopicOrAlias), alias => get_alias(TopicOrAlias), errors => Errors}),
      {error, failed_to_fetch_metadata}
  end.

-spec ensure_leader_connections3(state(), topic_or_alias(), pid(), _Brokers,
                                 _PartitionMetaList, max_partitions()) ->
          {ok, state()}.
ensure_leader_connections3(#{metadata_ts := MetadataTs} = St0, TopicOrAlias,
                           ConnPid, Brokers, PartitionMetaList0, MaxPartitions) ->
  PartitionMetaList = limit_partitions_count(PartitionMetaList0, MaxPartitions),
  St = lists:foldl(fun(PartitionMeta, StIn) ->
                       ensure_leader_connection(StIn, Brokers, TopicOrAlias, PartitionMeta)
                   end, St0, PartitionMetaList),
  {ok, St#{metadata_ts := MetadataTs#{TopicOrAlias => erlang:timestamp()},
           metadata_conn => ConnPid
          }}.

limit_partitions_count(PartitionMetaList, Max) when is_integer(Max) andalso Max < length(PartitionMetaList) ->
  lists:sublist(PartitionMetaList, Max);
limit_partitions_count(PartitionMetaList, _) ->
  PartitionMetaList.

%% This function ensures each Topic-Partition pair has a connection record
%% either a pid when the leader is healthy, or the error reason
%% if failed to discover the leader or failed to connect to the leader
-spec ensure_leader_connection(state(), _Brokers, topic_or_alias(), _PartitionMetaList) -> state().
ensure_leader_connection(St, Brokers, TopicOrAlias, P_Meta) ->
  PartitionNum = kpro:find(partition_index, P_Meta),
  ErrorCode = kpro:find(error_code, P_Meta),
  case ErrorCode =:= ?no_error of
    true ->
      do_ensure_leader_connection(St, Brokers, TopicOrAlias, PartitionNum, P_Meta);
    false ->
      maybe_disconnect_old_leader(St, TopicOrAlias, PartitionNum, ErrorCode)
  end.

-spec do_ensure_leader_connection(state(), _Brokers, topic_or_alias(), _Partition, _PartitionMetaList) ->
          state().
do_ensure_leader_connection(#{conn_config := ConnConfig,
                              conns := Connections0,
                              known_topics := KnownTopics0
                             } = St0, Brokers, TopicOrAlias, PartitionNum, P_Meta) ->
  Topic = get_topic(TopicOrAlias),
  Alias = get_alias(TopicOrAlias),
  LeaderBrokerId = kpro:find(leader_id, P_Meta),
  {_, Host} = lists:keyfind(LeaderBrokerId, 1, Brokers),
  Strategy = get_connection_strategy(St0),
  ConnId = case Strategy of
             per_partition -> {Topic, PartitionNum};
             per_broker -> Host
           end,
  Connections =
    case get_connected(ConnId, Host, Connections0) of
      already_connected ->
        Connections0;
      {needs_reconnect, OldConn} ->
        %% delegate to a process to speed up
        ok = close_connection(OldConn),
        add_conn(do_connect(Host, ConnConfig), ConnId, Connections0);
      false ->
        add_conn(do_connect(Host, ConnConfig), ConnId, Connections0)
    end,
  KnownTopics =
        maps:update_with(
          Topic,
          fun(KnownAliases) -> KnownAliases#{Alias => true} end,
          #{Alias => true},
          KnownTopics0),
  St = St0#{conns := Connections, known_topics := KnownTopics},
  Leaders0 = maps:get(leaders, St0, #{}),
  case Strategy of
    per_broker ->
      Leaders = Leaders0#{{Topic, PartitionNum} => maps:get(ConnId, Connections)},
      St#{leaders => Leaders};
    _ ->
      St
  end.

%% Handle error code in partition metadata.
maybe_disconnect_old_leader(#{conns := Connections} = St, TopicOrAlias, PartitionNum, ErrorCode) ->
  Topic = get_topic(TopicOrAlias),
  Strategy = get_connection_strategy(St),
  case Strategy of
    per_partition ->
      %% partition metadata has error code, there is no need to keep the old connection alive
      ConnId = {Topic, PartitionNum},
      MaybePid = maps:get(ConnId, Connections, false),
      is_pid(MaybePid) andalso close_connection(MaybePid),
      St#{conns := add_conn({error, ErrorCode}, ConnId, Connections)};
    _ ->
      %% the connection is shared by producers (for different topics)
      %% so we do not close the connection here.
      %% Also, since we do not know which Host the current leader resides
      %% (due to the error code returned) there is no way to know which
      %% connection to close anyway.
      Leaders0 = maps:get(leaders, St, #{}),
      Leaders = Leaders0#{{Topic, PartitionNum} => ErrorCode},
      St#{leaders => Leaders}
  end.

get_connection_strategy(#{config := Config}) ->
  maps:get(connection_strategy, Config, per_partition).

get_connected(Host, Host, Conns) ->
  %% keyed by host
  Pid = maps:get(Host, Conns, false),
  is_alive(Pid) andalso already_connected;
get_connected(ConnId, Host, Conns) ->
  %% keyed by topic-partition
  %% may need to reconnect when leader is found in another broker
  Pid = maps:get(ConnId, Conns, false),
  case is_connected(Pid, Host) of
    true -> already_connected;
    false when is_pid(Pid) -> {needs_reconnect, Pid};
    false -> false
  end.

%% return true if MaybePid is a connection towards the given host.
is_connected(MaybePid, {Host, Port}) ->
  is_alive(MaybePid) andalso
  case kpro_connection:get_endpoint(MaybePid) of
    {ok, {Host1, Port}} ->
      iolist_to_binary(Host) =:= iolist_to_binary(Host1);
    _ ->
      false
  end.

is_alive(Pid) -> is_pid(Pid) andalso erlang:is_process_alive(Pid).

add_conn({ok, Pid}, ConnId, Conns) ->
    true = is_pid(Pid), %% assert
    Conns#{ConnId => Pid};
add_conn({error, Reason}, ConnId, Conns) ->
    false = is_pid(Reason), %% assert
    Conns#{ConnId => Reason}.

split_config(Config) ->
  ConnCfgKeys = kpro_connection:all_cfg_keys(),
  Pred = fun({K, _V}) -> lists:member(K, ConnCfgKeys) end,
  {ConnCfg, MyCfg} = lists:partition(Pred, maps:to_list(Config)),
  {maps:from_list(ConnCfg), maps:from_list(MyCfg)}.

-spec get_metadata([_Host], _ConnConfig, topic()) ->
          {ok, {pid(), term()}} | {error, term()}.
get_metadata(Hosts, _ConnectFun, _Topic) when Hosts =:= [] ->
  {error, no_hosts};
get_metadata(Hosts, ConnectFun, Topic) ->
  get_metadata(Hosts, ConnectFun, Topic, []).

-spec get_metadata([_Host], _ConnConfig, topic(), [Error]) ->
          {ok, {pid(), term()}} | {error, [Error] | term()}.
get_metadata([], _ConnConfig, _Topic, Errors) ->
  {error, Errors};
get_metadata([Host | Rest], ConnConfig, Topic, Errors) ->
  case do_connect(Host, ConnConfig) of
    {ok, Pid} ->
      Timeout = maps:get(request_timeout, ConnConfig, ?DEFAULT_METADATA_TIMEOUT),
      case do_get_metadata(Pid, Topic, Timeout) of
        {ok, Result} ->
          {ok, {Pid, Result}};
        {error, Reason} ->
          %% failed to fetch metadata, make sure this connection is closed
          ok = close_connection(Pid),
          {error, Reason}
      end;
    {error, Reason} ->
      get_metadata(Rest, ConnConfig, Topic, [Reason | Errors])
  end.

-spec do_get_metadata(connection(), topic(), timeout()) ->
          {ok, {_Brokers, _Partitions}} | {error, term()}.
do_get_metadata(Connection, Topic, Timeout) ->
  case kpro:get_api_versions(Connection) of
    {ok, Vsns} ->
      {_, Vsn} = maps:get(metadata, Vsns),
      do_get_metadata2(Vsn, Connection, Topic, Timeout);
    {error, Reason} ->
      {error, Reason}
  end.

-spec do_get_metadata2(_Vsn, connection(), topic(), timeout()) -> {ok, {_, _}} | {error, term()}.
do_get_metadata2(Vsn, Connection, Topic, Timeout) ->
  Req = kpro_req_lib:metadata(Vsn, [Topic], _IsAutoCreateAllowed = false),
  case kpro:request_sync(Connection, Req, Timeout) of
    {ok, #kpro_rsp{msg = Meta}} ->
      BrokersMeta = kpro:find(brokers, Meta),
      Brokers = [parse_broker_meta(M) || M <- BrokersMeta],
      [TopicMeta] = kpro:find(topics, Meta),
      ErrorCode = kpro:find(error_code, TopicMeta),
      Partitions = kpro:find(partitions, TopicMeta),
      case ErrorCode =:= ?no_error of
        true ->
          {ok, {Brokers, Partitions}};
        false ->
          {error, ErrorCode} %% no such topic ?
      end;
    {error, Reason} ->
      {error, Reason}
  end.

-spec parse_broker_meta(kpro:struct()) -> {integer(), host()}.
parse_broker_meta(BrokerMeta) ->
  BrokerId = kpro:find(node_id, BrokerMeta),
  Host = kpro:find(host, BrokerMeta),
  Port = kpro:find(port, BrokerMeta),
  {BrokerId, {Host, Port}}.

log_warn(Msg, Report) -> logger:warning(Report#{msg => Msg}).

do_connect(Host, ConnConfig) ->
    case kpro:connect(Host, ConnConfig) of
        {ok, Pid} -> {ok, Pid};
        {error, Reason} -> {error, tr_reason({Host, Reason})}
    end.

%% prior to 1.5.2, the connect config is hidden in an anonymous function
%% which will cause 'badfun' exception after beam is purged.
%% this function is to find the connect config from supervisor.
%% NOTE: only works when it's supervised client.
upgrade(#{conn_config := _} = St) -> St;
upgrade(#{connect := _Fun} = St) ->
  ClientId = maps:get(client_id, St),
  maps:without([connect], St#{conn_config => find_config(ClientId)}).

find_config(ClientId) ->
  case supervisor:get_childspec(wolff_client_sup, ClientId) of
    {ok, ChildSpec} -> get_config(ChildSpec);
    {error, Reason} -> exit({cannot_find_connection_config, Reason})
  end.

get_config(#{start := {_Module, _StartLink, Args}}) ->
    [_ClinetID, _Hosts, Config] = Args,
    {ConnConfig, _MyConfig} = split_config(Config),
    ConnConfig.

tr_reason({{Host, Port}, Reason}) ->
    #{host => bin([bin(Host), ":", bin(Port)]),
      reason => do_tr_reason(Reason)
     }.

do_tr_reason({{#kpro_req{}, Reason}, Stack}) -> do_tr_reason({Reason, Stack});
do_tr_reason({timeout, _Stack}) -> connection_timed_out;
do_tr_reason({enetunreach, _Stack}) -> unreachable_host;
do_tr_reason({econnrefused, _Stack}) -> connection_refused;
do_tr_reason({nxdomain, _Stack}) -> unresolvable_hostname;
do_tr_reason({R, Stack}) when is_atom(R) ->
    case inet:format_error(R) of
        "unknown " ++ _ -> {R, Stack};
        POSIX -> {POSIX, Stack}
    end;
do_tr_reason(Other) -> Other.

tr_reasons(L) ->
    lists:map(fun tr_reason/1, L).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8); %% hostname can be atom like 'localhost'
bin(L) when is_list(L) -> iolist_to_binary(L);
bin(B) when is_binary(B) -> B;
bin(P) when is_integer(P) -> integer_to_binary(P); %% port number
bin(X) ->
    case inet:ntoa(X) of
        {error, _} -> bin(io_lib:format("~0p", [X]));
        Addr -> bin(Addr)
    end.

-spec get_topic(topic_or_alias()) -> topic().
get_topic({_Alias, Topic}) -> Topic;
get_topic(Topic) -> Topic.

-spec get_alias(topic_or_alias()) -> producer_alias().
get_alias({Alias, _Topic}) -> Alias;
get_alias(_Topic) -> ?NO_ALIAS.

-spec ensure_has_alias(topic_or_alias()) -> alias_and_topic().
ensure_has_alias({Alias, Topic}) -> {Alias, Topic};
ensure_has_alias(Topic) -> {?NO_ALIAS, Topic}.
