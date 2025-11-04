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
-export([get_leader_connections/3,
         get_leader_connections/4,
         recv_leader_connection/6,
         get_id/1,
         release_leader_conns/3]).
-export([check_connectivity/1, check_connectivity/2]).
-export([check_topic_exists_with_client_pid/2]).

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

-export_type([config/0, producer_group/0, max_partitions/0]).

%% deprecated
-export([check_if_topic_exists/3]).

-type config() :: map().
-type topic() :: kpro:topic().
-type producer_group() :: binary().
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
        known_topics := #{topic() => #{producer_group() => true}}
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

-spec get_leader_connections(pid(), producer_group(), topic()) ->
        {ok, [{partition(), pid() | ?conn_down(_)}]} | {error, any()}.
get_leader_connections(Client, Group, Topic) ->
   get_leader_connections(Client, Group, Topic, all_partitions).

-spec get_leader_connections(pid(), producer_group(), topic(), all_partitions | pos_integer()) ->
        {ok, [{partition(), pid() | ?conn_down(_)}]} | {error, any()}.
get_leader_connections(Client, Group, Topic, MaxPartitions) ->
   safe_call(Client, {get_leader_connections, Group, Topic, MaxPartitions}).

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
  case get_metadata(Hosts, ConnConfig, Topic, _IsAutoCreateAllowed = false) of
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

%% @doc request client to send Pid the leader connection.
%% The caller will receive message `{leader_connection, Pid}' if the leader is successfully connected,
%% otherwise `{leader_connection, {down, Reason}}'.
-spec recv_leader_connection(pid(), ?NO_GROUP | producer_group(), topic(), partition(), pid(), all_partitions | pos_integer()) -> ok.
recv_leader_connection(Client, Group, Topic, Partition, Caller, MaxPartitions) ->
  gen_server:cast(Client, {recv_leader_connection, Group, Topic, Partition, Caller, MaxPartitions}).

release_leader_conns(Client, Group, Topic) ->
    gen_server:cast(Client, {release_leader_conns, Group, Topic}).

init(#{client_id := ClientID} = St) ->
  erlang:process_flag(trap_exit, true),
  ok = wolff_client_sup:register_client(ClientID),
  {ok, St}.

handle_call(Call, From, #{connect := _Fun} = St) ->
    handle_call(Call, From, upgrade(St));
handle_call(get_id, _From, #{client_id := Id} = St) ->
  {reply, Id, St};
handle_call({check_if_topic_exists, Topic}, _From, #{config := Config, conn_config := ConnConfig} = St0) ->
  IsAutoCreateAllowed = maps:get(allow_auto_topic_creation, Config, false),
  case ensure_metadata_conn(St0) of
    {ok, #{metadata_conn := ConnPid} = St} ->
      Timeout = maps:get(request_timeout, ConnConfig, ?DEFAULT_METADATA_TIMEOUT),
      {reply, check_if_topic_exists2(ConnPid, Topic, Timeout, IsAutoCreateAllowed), St};
    {error, Reason} ->
      {reply, {error, Reason}, St0}
  end;
handle_call({get_leader_connections, Group, Topic, MaxPartitions}, _From, St0) ->
  case ensure_leader_connections(St0, Group, Topic, MaxPartitions) of
    {ok, St} ->
      Result = do_get_leader_connections(St, Group, Topic),
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

handle_info({'EXIT', Pid, Reason}, St) ->
  {noreply, flush_exit_signals(St, Pid, Reason)};
handle_info(_Info, St) ->
  {noreply, upgrade(St)}.

handle_cast(Cast, #{connect := _Fun} = St) ->
    handle_cast(Cast, upgrade(St));
handle_cast({recv_leader_connection, Group, Topic, Partition, Caller, MaxConnections}, St0) ->
  case ensure_leader_connections(St0, Group, Topic, MaxConnections) of
    {ok, St1} ->
      %% in case the connection is down right after connected
      %% e.g. api query error, authentication error
      %% we try the best to return the error immediately.
      %% This however does not eliminate race condition completely, if EXIT happens
      %% after the leader connections are sent, but before the caller (wolff_producer)
      %% can monitor the pid, the 'EXIT' signal will be handled in handle_info.
      %% menaing wolff_producer will get noproc as DOWN reason for the first attempt
      %% then get the real EXIT reason in the retry attempts
      St = flush_exit_signals(St1),
      Partitions = do_get_leader_connections(St, Group, Topic),
      _ = case lists:keyfind(Partition, 1, Partitions) of
        {_, MaybePid} ->
          erlang:send(Caller, ?leader_connection(MaybePid));
        false ->
          log_warn("partition_missing_in_metadata_response", #{topic => Topic, partition => Partition}),
          %% This happens as a race between metadata refresh and partition producer shutdown
          %% partition producer will be shutdown by wolff_producers after metadata refresh is complete
          %% Or a malformed metadata response with the last partition missing.
          Reason = partition_missing_in_metadata_response,
          erlang:send(Caller, ?leader_connection(?conn_down(Reason)))
      end,
      {noreply, St};
    {error, Reason} ->
      _ = erlang:send(Caller, ?leader_connection(?conn_down(Reason))),
      {noreply, St0}
  end;
handle_cast({release_leader_conns, Group, Topic}, St0) ->
  St = do_release_leader_conns(Group, Topic, St0),
  {noreply, St};
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

do_release_leader_conns(Group, ?DYNAMIC, St) ->
  #{known_topics := KnownTopics} = St,
  Topics = maps:keys(KnownTopics),
  lists:foldl(fun(T, StIn) -> do_release_leader_conns(Group, T, StIn) end, St, Topics);
do_release_leader_conns(Group, Topic, St) ->
  #{metadata_ts := Topics0,
    conns := Conns0,
    known_topics := KnownTopics0
   } = St,
  case KnownTopics0 of
      #{Topic := #{Group := true} = KnownProducers} when map_size(KnownProducers) =:= 1 ->
          %% Last entry: we may drop the connection metadata
          KnownTopics = maps:remove(Topic, KnownTopics0),
          Conns = close_connections(Conns0, Topic),
          Topics = maps:remove(Topic, Topics0),
          St#{metadata_ts := Topics, conns := Conns, known_topics := KnownTopics};
      #{Topic := #{Group := true} = KnownProducers0} ->
          %% Connection is still being used by other producers.
          KnownProducers = maps:remove(Group, KnownProducers0),
          KnownTopics = KnownTopics0#{Topic := KnownProducers},
          St#{known_topics := KnownTopics};
      _ ->
          %% Already gone; nothing to do.
          St
  end.

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

check_if_topic_exists2(Pid, Topic, Timeout, IsAutoCreateAllowed) when is_pid(Pid) ->
  case do_get_metadata(Pid, Topic, Timeout, IsAutoCreateAllowed) of
    {ok, _} ->
      ok;
    {error, Reason} ->
      {error, Reason}
  end.

close_connections(Conns, Topic) ->
  Pred = fun({T, _P}) ->
             T =:= Topic;
            (_) ->
             false
         end,
  do_close_connections(maps:to_list(Conns), Pred, #{}).

flush_exit_signals(St0) ->
  receive
    {'EXIT', Pid, Reason} ->
      flush_exit_signals(St0, Pid, Reason)
  after
    0 ->
      St0
  end.

%% Keep the connection process EXIT reason in the state.
%% If the pid is not found in the conns map, it's a no-op.
%% For example, the metadata connection is not in the conns map.
flush_exit_signals(#{conns := Conns0} = St0, Pid, Reason) ->
  Conns = maps:to_list(Conns0),
  St = case lists:keyfind(Pid, 2, Conns) of
    false ->
      St0;
    {ConnId, Pid} ->
      St0#{conns => maps:put(ConnId, Reason, Conns0)}
  end,
  flush_exit_signals(St).

close_connections(Conns) ->
  _ = do_close_connections(maps:to_list(Conns), fun(_) -> true end, #{}),
  ok.

do_close_connections([], _Pred, Acc) ->
  Acc;
do_close_connections([{Key, Pid} | More], Pred, Acc) ->
  case Pred(Key) of
    true ->
      _ = close_connection(Pid),
      do_close_connections(More, Pred, Acc);
    false ->
      do_close_connections(More, Pred, Acc#{Key => Pid})
  end.

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

do_get_leader_connections(#{conns := Conns} = St, _Group, Topic) ->
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
is_metadata_fresh(#{metadata_ts := Topics, config := Config}, Topic) ->
  MinInterval = maps:get(min_metadata_refresh_interval, Config, ?MIN_METADATA_REFRESH_INTERVAL),
  case maps:get(Topic, Topics, false) of
    false -> false;
    Ts -> timer:now_diff(erlang:timestamp(), Ts) < MinInterval * 1000
  end.

-spec ensure_leader_connections(state(), producer_group(), topic(), all_partitions | pos_integer()) ->
  {ok, state()} | {error, any()}.
ensure_leader_connections(St, Group, Topic, MaxPartitions) ->
  case is_metadata_fresh(St, Topic) of
    true -> {ok, ensure_group_is_recorded(St, Group, Topic)};
    false -> ensure_leader_connections2(St, Group, Topic, MaxPartitions)
  end.

-spec ensure_leader_connections2(state(), producer_group(), topic(), max_partitions()) ->
          {ok, state()} | {error, term()}.
ensure_leader_connections2(#{metadata_conn := Pid,
                             conn_config := ConnConfig,
                             config := Config
                            } = St, Group, Topic, MaxPartitions) when is_pid(Pid) ->
  Timeout = metadata_request_timeout(ConnConfig),
  IsAutoCreateAllowed = maps:get(allow_auto_topic_creation, Config, false),
  case do_get_metadata(Pid, Topic, Timeout, IsAutoCreateAllowed) of
    {ok, {Brokers, PartitionMetaList}} ->
      ensure_leader_connections3(St, Group, Topic, Pid, Brokers, PartitionMetaList, MaxPartitions);
    {error, _Reason} ->
      %% ensure metadata connection is down, try to establish a new one in the next clause,
      %% reason is discarded here, because the next clause will log error if the immediate retry fails
      exit(Pid, kill),
      ensure_leader_connections2(St#{metadata_conn => down}, Group, Topic, MaxPartitions)
  end;
ensure_leader_connections2(#{conn_config := ConnConfig,
                             seed_hosts := SeedHosts,
                             config := Config
                            } = St, Group, Topic, MaxPartitions) ->
  IsAutoCreateAllowed = maps:get(allow_auto_topic_creation, Config, false),
  case get_metadata(SeedHosts, ConnConfig, Topic, IsAutoCreateAllowed, []) of
    {ok, {ConnPid, {Brokers, PartitionMetaList}}} ->
      ensure_leader_connections3(St, Group, Topic, ConnPid, Brokers, PartitionMetaList, MaxPartitions);
    {error, unknown_topic_or_partition} ->
      log_warn(failed_to_fetch_metadata,
               #{topic => Topic,
                 description => "The topic does not exist, or client is not authorized to access"
                }),
      {error, unknown_topic_or_partition};
    {error, Errors} ->
      log_warn(failed_to_fetch_metadata, #{topic => Topic, errors => Errors}),
      {error, failed_to_fetch_metadata}
  end.

-spec ensure_leader_connections3(state(), producer_group(), topic(), pid(), _Brokers,
                                 _PartitionMetaList, max_partitions()) ->
          {ok, state()}.
ensure_leader_connections3(#{metadata_ts := MetadataTs} = St0, Group, Topic,
                           ConnPid, Brokers, PartitionMetaList0, MaxPartitions) ->
  OldPartitions = lists:map(fun({P, _}) -> P end, do_get_leader_connections(St0, Group, Topic)),
  PartitionMetaList = limit_partitions_count(PartitionMetaList0, MaxPartitions),
  NewPartitions = lists:map(fun(PartitionMeta) ->
                                kpro:find(partition_index, PartitionMeta)
                            end, PartitionMetaList),
  DeletedPartitions = lists:subtract(OldPartitions, NewPartitions),
  St1 = lists:foldl(fun(PartitionMeta, StIn) ->
                       ensure_leader_connection(StIn, Brokers, Group, Topic, PartitionMeta)
                   end, St0, PartitionMetaList),
  St = lists:foldl(fun(Partition, StIn) ->
                      maybe_disconnect_old_leader(StIn, Topic, Partition, partition_missing_in_metadata_response)
                  end, St1, DeletedPartitions),
  {ok, St#{metadata_ts := MetadataTs#{Topic => erlang:timestamp()},
           metadata_conn => ConnPid
          }}.

limit_partitions_count(PartitionMetaList, Max) when is_integer(Max) andalso Max < length(PartitionMetaList) ->
  lists:sublist(PartitionMetaList, Max);
limit_partitions_count(PartitionMetaList, _) ->
  PartitionMetaList.

%% This function ensures each Topic-Partition pair has a connection record
%% either a pid when the leader is healthy, or the error reason
%% if failed to discover the leader or failed to connect to the leader
-spec ensure_leader_connection(state(), _Brokers, producer_group(), topic(), _PartitionMetaList) -> state().
ensure_leader_connection(St, Brokers, Group, Topic, P_Meta) ->
  PartitionNum = kpro:find(partition_index, P_Meta),
  ErrorCode = kpro:find(error_code, P_Meta),
  case ErrorCode =:= ?no_error of
    true ->
      do_ensure_leader_connection(St, Brokers, Group, Topic, PartitionNum, P_Meta);
    false ->
      maybe_disconnect_old_leader(St, Topic, PartitionNum, ErrorCode)
  end.

-spec do_ensure_leader_connection(state(), _Brokers, producer_group(), topic(), _Partition, _PartitionMetaList) ->
          state().
do_ensure_leader_connection(#{conn_config := ConnConfig,
                              conns := Connections0
                             } = St0, Brokers, Group, Topic, PartitionNum, P_Meta) ->
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
        update_conn(do_connect(Host, ConnConfig), ConnId, Connections0);
      false ->
        update_conn(do_connect(Host, ConnConfig), ConnId, Connections0)
    end,
  Leaders0 = maps:get(leaders, St0, #{}),
  St = ensure_group_is_recorded(St0#{conns := Connections}, Group, Topic),
  case Strategy of
    per_broker ->
      Leaders = Leaders0#{{Topic, PartitionNum} => maps:get(ConnId, Connections)},
      St#{leaders => Leaders};
    _ ->
      St
  end.

ensure_group_is_recorded(#{known_topics := KnownTopics0} = St, Group, Topic) ->
  KnownTopics =
        maps:update_with(
          Topic,
          fun(KnownGroups) -> KnownGroups#{Group => true} end,
          #{Group => true},
          KnownTopics0),
  St#{known_topics := KnownTopics}.

%% Handle error code in partition metadata.
maybe_disconnect_old_leader(#{conns := Connections} = St, Topic, PartitionNum, ErrorCode) ->
  Strategy = get_connection_strategy(St),
  case Strategy of
    per_partition ->
      %% partition metadata has error code, there is no need to keep the old connection alive
      ConnId = {Topic, PartitionNum},
      MaybePid = maps:get(ConnId, Connections, false),
      is_pid(MaybePid) andalso close_connection(MaybePid),
      St#{conns := update_conn({error, ErrorCode}, ConnId, Connections)};
    _ ->
      %% the connection is shared by producers (for different topics)
      %% so we do not close the connection here.
      %% Also, since we do not know which Host the current leader resides
      %% (due to the error code returned) there is no way to know which
      %% connection to close anyway.
      Leaders0 = maps:get(leaders, St, #{}),
      Leaders = case ErrorCode of
                  partition_missing_in_metadata_response ->
                    maps:remove({Topic, PartitionNum}, Leaders0);
                  _ -> Leaders0#{{Topic, PartitionNum} => ErrorCode}
                end,
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

update_conn({ok, Pid}, ConnId, Conns) ->
  true = is_pid(Pid), %% assert
  Conns#{ConnId => Pid};
update_conn({error, partition_missing_in_metadata_response}, ConnId, Conns) ->
  maps:remove(ConnId, Conns);
update_conn({error, Reason}, ConnId, Conns) ->
  false = is_pid(Reason), %% assert
  Conns#{ConnId => Reason}.

split_config(Config) ->
  ConnCfgKeys = kpro_connection:all_cfg_keys(),
  Pred = fun({K, _V}) -> lists:member(K, ConnCfgKeys) end,
  {ConnCfg, MyCfg} = lists:partition(Pred, maps:to_list(Config)),
  {maps:from_list(ConnCfg), maps:from_list(MyCfg)}.

-spec get_metadata([_Host], _ConnConfig, topic(), boolean()) ->
          {ok, {pid(), term()}} | {error, term()}.
get_metadata(Hosts, _ConnectFun, _Topic, _IsAutoCreateAllowed) when Hosts =:= [] ->
  {error, no_hosts};
get_metadata(Hosts, ConnectFun, Topic, IsAutoCreateAllowed) ->
  get_metadata(Hosts, ConnectFun, Topic, IsAutoCreateAllowed, []).

-spec get_metadata([_Host], _ConnConfig, topic(), boolean(), [Error]) ->
          {ok, {pid(), term()}} | {error, [Error] | term()}.
get_metadata([], _ConnConfig, _Topic, _IsAutoCreateAllowed, Errors) ->
  {error, Errors};
get_metadata([Host | Rest], ConnConfig, Topic, IsAutoCreateAllowed, Errors) ->
  case do_connect(Host, ConnConfig) of
    {ok, Pid} ->
      Timeout = metadata_request_timeout(ConnConfig),
      case do_get_metadata(Pid, Topic, Timeout, IsAutoCreateAllowed) of
        {ok, Result} ->
          {ok, {Pid, Result}};
        {error, Reason} ->
          %% failed to fetch metadata, make sure this connection is closed
          ok = close_connection(Pid),
          {error, Reason}
      end;
    {error, Reason} ->
      get_metadata(Rest, ConnConfig, Topic, IsAutoCreateAllowed, [Reason | Errors])
  end.

-spec do_get_metadata(connection(), topic(), timeout(), boolean()) ->
          {ok, {_Brokers, _Partitions}} | {error, term()}.
do_get_metadata(Connection, Topic, Timeout, IsAutoCreateAllowed) ->
  case kpro:get_api_versions(Connection) of
    {ok, Vsns} ->
      {_, Vsn} = maps:get(metadata, Vsns),
      do_get_metadata2(Vsn, Connection, Topic, Timeout, IsAutoCreateAllowed);
    {error, Reason} ->
      {error, Reason}
  end.

-spec retry(fun(() -> {ok, term()} | {error, term()}), timeout()) -> {ok, term()} | {error, term()}.
retry(Fun, Timeout) ->
  Deadline = erlang:monotonic_time(millisecond) + Timeout,
  retry_loop(Fun, Deadline).

-spec retry_loop(fun(() -> {ok, term()} | {error, term()}), integer()) -> {ok, term()} | {error, term()}.
retry_loop(Fun, Deadline) ->
  case Fun() of
    {ok, Result} ->
      {ok, Result};
    {error, R} when R =:= unknown_topic_or_partition orelse R =:= leader_not_available ->
      CurrentTime = erlang:monotonic_time(millisecond),
      case CurrentTime >= Deadline of
        true ->
          {error, timeout};
        false ->
          %% Sleep for a short interval before retrying
          RemainingTime = Deadline - CurrentTime,
          SleepTime = min(1000, RemainingTime),
          timer:sleep(SleepTime),
          retry_loop(Fun, Deadline)
      end;
    {error, Reason} ->
      {error, Reason}
  end.

-spec do_get_metadata2(_Vsn, connection(), topic(), timeout(), boolean()) -> {ok, {_, _}} | {error, term()}.
do_get_metadata2(Vsn, Connection, Topic, Timeout, _IsAutoCreateAllowed = true) ->
  retry(fun() -> do_get_metadata3(Vsn, Connection, Topic, Timeout, true) end, Timeout);
do_get_metadata2(Vsn, Connection, Topic, Timeout, _IsAutoCreateAllowed = false) ->
  do_get_metadata3(Vsn, Connection, Topic, Timeout, false).

-spec do_get_metadata3(_Vsn, connection(), topic(), timeout(), boolean()) -> {ok, {_, _}} | {error, term()}.
do_get_metadata3(Vsn, Connection, Topic, Timeout, IsAutoCreateAllowed) ->
  Req = kpro_req_lib:metadata(Vsn, [Topic], IsAutoCreateAllowed),
  case kpro:request_sync(Connection, Req, Timeout) of
    {ok, #kpro_rsp{msg = Meta}} ->
      BrokersMeta = kpro:find(brokers, Meta),
      Brokers = [parse_broker_meta(M) || M <- BrokersMeta],
      [TopicMeta] = kpro:find(topics, Meta),
      ErrorCode = kpro:find(error_code, TopicMeta),
      Partitions0 = kpro:find(partitions, TopicMeta),
      case ErrorCode =:= ?no_error of
        true when Partitions0 =:= [] ->
          %% unsure if this is possible
          {error, no_partitions_metadata};
        true ->
          Partitions = fix_partition_metadata(Topic, Partitions0),
          {ok, {Brokers, Partitions}};
        false ->
          {error, ErrorCode} %% no such topic ?
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% The partitions count cache only caches the total count but not
%% a list of partition numbers, so we need to fix the "holes" in
%% the partition sequence [0, N), if any.
%% There is no partition count returned in metadata response,
%% so we must rely on the max partition number to guess the total
%% number of partitions.
fix_partition_metadata(Topic, PartitionMetaList) ->
  Partitions = lists:sort(lists:map(fun(M) -> kpro:find(partition_index, M) end, PartitionMetaList)),
  Missing = fix_partition_metadata_loop(Partitions, 0, []),
  case Missing =:= [] of
    true ->
      PartitionMetaList;
    false ->
      log_warn("partitions_missing_in_metadata_response", #{topic => Topic, partitions => Missing}),
      PartitionMetaList ++
      lists:map(fun(P) ->
        #{partition_index => P,
          error_code => partition_missing_in_metadata_response
        }
      end, Missing)
  end.

fix_partition_metadata_loop([], _, Missing) ->
  lists:reverse(Missing);
fix_partition_metadata_loop([P | Partitions], P, Missing) ->
  fix_partition_metadata_loop(Partitions, P + 1, Missing);
fix_partition_metadata_loop([P0 | _] = Partitions, P, Missing) ->
  true = (P0 > P),
  fix_partition_metadata_loop(Partitions, P + 1, [P | Missing]).

-spec parse_broker_meta(kpro:struct()) -> {integer(), host()}.
parse_broker_meta(BrokerMeta) ->
  BrokerId = kpro:find(node_id, BrokerMeta),
  Host = kpro:find(host, BrokerMeta),
  Port = kpro:find(port, BrokerMeta),
  {BrokerId, {Host, Port}}.

log_warn(Msg, Report) -> logger:warning(Report#{msg => Msg, pid => self()}).

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

metadata_request_timeout(#{request_timeout := infinity}) ->
  ?DEFAULT_METADATA_TIMEOUT * 3;
metadata_request_timeout(#{request_timeout := Timeout}) ->
  Timeout;
metadata_request_timeout(_) ->
  ?DEFAULT_METADATA_TIMEOUT.
