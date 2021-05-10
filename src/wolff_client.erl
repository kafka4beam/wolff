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
-export([get_leader_connections/2, recv_leader_connection/4, get_id/1, delete_producers_metadata/2]).

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

-export_type([config/0]).

-type config() :: map().
-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type connection() :: kpro:connection().
-type host() :: wolff:host().
-type conn_id() :: {topic(), partition()} | host().

-type state() ::
      #{client_id := wolff:client_id(),
        seed_hosts := host(),
        config := config(),
        connect := fun((host()) -> {ok, connection()} | {error, any()}),
        conns := #{conn_id() => connection()},
        metadata_ts := #{topic() => erlang:timestamp()},
        %% only applicable when connection strategy is per_broker
        %% because in this case the connections are keyed by host()
        %% but we need to find connection by {topic(), partition()}
        leaders => #{{topic(), partition()} => connection()}
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
  % Split out connection config and keep it in an anonymous function
  % instead of keeping it in my gen_server looping state
  % this is an easy trick to avoid passwords in connection config getting dumpped to crash logs
  {ConnCfg0, MyCfg} = split_config(Config),
  ConnCfg = ConnCfg0#{client_id => ClientId},
  St = #{client_id => ClientId,
         seed_hosts => Hosts,
         config => MyCfg,
         connect => fun(Host) -> kpro:connect(Host, ConnCfg) end,
         conns => #{},
         metadata_ts => #{},
         leaders => #{}
        },
  case maps:get(reg_name, Config, false) of
    false -> gen_server:start_link(?MODULE, St, []);
    Name -> gen_server:start_link({local, Name}, ?MODULE, St, [])
  end.

stop(Pid) ->
  gen_server:call(Pid, stop, infinity).

get_id(Pid) ->
  gen_server:call(Pid, get_id, infinity).

-spec get_leader_connections(pid(), topic()) ->
        {ok, [{partition(), pid() | ?conn_down(_)}]} | {error, any()}.
get_leader_connections(Client, Topic) ->
  gen_server:call(Client, {get_leader_connections, Topic}, infinity).

%% request client to send Pid the leader connection.
recv_leader_connection(Client, Topic, Partition, Pid) ->
  gen_server:cast(Client, {recv_leader_connection, Topic, Partition, Pid}).

delete_producers_metadata(Client, Topic) ->
    gen_server:cast(Client, {delete_producers_metadata, Topic}).

init(St) ->
  erlang:process_flag(trap_exit, true),
  {ok, St}.

handle_call(get_id, _From, #{client_id := Id} = St) ->
  {reply, Id, St};
handle_call({get_leader_connections, Topic}, _From, St0) ->
  case ensure_leader_connections(St0, Topic) of
    {ok, St} ->
      Result = do_get_leader_connections(St, Topic),
      {reply, {ok, Result}, St};
    {error, Reason} ->
      {reply, {error, Reason}, St0}
  end;
handle_call(stop, From, #{conns := Conns} = St) ->
  ok = close_connections(Conns),
  gen_server:reply(From, ok),
  {stop, normal, St#{conns := #{}}};
handle_call(_Call, _From, St) ->
  {noreply, St}.

handle_info(_Info, St) ->
  {noreply, St}.

handle_cast({recv_leader_connection, Topic, Partition, Caller}, St0) ->
  case ensure_leader_connections(St0, Topic) of
    {ok, St} ->
      Partitions = do_get_leader_connections(St, Topic),
      {_, MaybePid} = lists:keyfind(Partition, 1, Partitions),
      _ = erlang:send(Caller, ?leader_connection(MaybePid)),
      {noreply, St};
    {error, Reason} ->
      _ = erlang:send(Caller, ?leader_connection({error, Reason})),
      {noreply, St0}
  end;

handle_cast({delete_producers_metadata, Topic}, #{metadata_ts := Topics, conns := Conns} = St) ->
  Conns1 = maps:without( [K || K = {K1, _} <- maps:keys(Conns), K1 =:= Topic ], Conns),
  {noreply, St#{metadata_ts => maps:remove(Topic, Topics), conns => Conns1}};

handle_cast(_Cast, St) ->
  {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
  {ok, St}.

terminate(_, #{conns := Conns} = St) ->
  ok = close_connections(Conns),
  {ok, St#{conns := #{}}}.

%% == internals ======================================================

close_connections(Conns) ->
  lists:foreach(fun({_, Pid}) -> close_connection(Pid) end, maps:to_list(Conns)).

close_connection(Conn) ->
  _ = spawn(fun() -> do_close_connection(Conn) end),
  ok.

%% This is a copy of kpro_connection:stop which supports kill after a timeout
do_close_connection(Pid) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {{self(), Mref}, stop}),
  receive
    {Mref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {connection_down, Reason}}
  after
    5000 ->
      exit(Pid, kill)
  end.

do_get_leader_connections(#{conns := Conns} = St, Topic) ->
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

%% retrun true if there is no need to refresh metadata because the last one is fresh enough
is_metadata_fresh(#{metadata_ts := Topics, config := Config}, Topic) ->
  MinInterval = maps:get(min_metadata_refresh_interval, Config, ?MIN_METADATA_REFRESH_INTERVAL),
  case maps:get(Topic, Topics, false) of
    false -> false;
    Ts -> timer:now_diff(erlang:timestamp(), Ts) < MinInterval * 1000
  end.

-spec ensure_leader_connections(state(), topic()) ->
  {ok, state()} | {error, any()}.
ensure_leader_connections(St, Topic) ->
  case is_metadata_fresh(St, Topic) of
    true -> {ok, St};
    false -> do_ensure_leader_connections(St, Topic)
  end.

do_ensure_leader_connections(#{connect := ConnectFun,
                               seed_hosts := SeedHosts,
                               metadata_ts := MetadataTs
                              } = St0, Topic) ->
  case get_metadata(SeedHosts, ConnectFun, Topic, []) of
    {ok, {Brokers, Partitions}} ->
      St = lists:foldl(
             fun(Partition, StIn) ->
                 try
                   ensure_leader_connection(StIn, Brokers, Topic, Partition)
                 catch
                   error : Reason ->
                     log_warn("Bad metadata for ~p-~p\nreason=~p", [Topic, Partition, Reason]),
                     StIn
                 end
             end, St0, Partitions),
      {ok, St#{metadata_ts := MetadataTs#{Topic => erlang:timestamp()}}};
    {error, Reason} ->
      log_warn("Failed to get metadata\nreason: ~p", [Reason]),
      {error, failed_to_fetch_metadata}
  end.

ensure_leader_connection(#{connect := ConnectFun,
                           conns := Connections0
                          } = St0, Brokers, Topic, P_Meta) ->
  Leaders0 = maps:get(leaders, St0, #{}),
  ErrorCode = kpro:find(error_code, P_Meta),
  ErrorCode =:= ?no_error orelse erlang:error(ErrorCode),
  PartitionNum = kpro:find(partition, P_Meta),
  LeaderBrokerId = kpro:find(leader, P_Meta),
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
        add_conn(ConnectFun(Host), ConnId, Connections0);
      false ->
        add_conn(ConnectFun(Host), ConnId, Connections0)
    end,
  St = St0#{conns := Connections},
  case Strategy of
    per_broker ->
      Leaders = Leaders0#{{Topic, PartitionNum} => maps:get(ConnId, Connections)},
      St#{leaders => Leaders};
    _ ->
      St
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

add_conn({ok, Pid}, ConnId, Conns) -> Conns#{ConnId => Pid};
add_conn({error, Reason}, ConnId, Conns) -> Conns#{ConnId => Reason}.

split_config(Config) ->
  ConnCfgKeys = kpro_connection:all_cfg_keys(),
  Pred = fun({K, _V}) -> lists:member(K, ConnCfgKeys) end,
  {ConnCfg, MyCfg} = lists:partition(Pred, maps:to_list(Config)),
  {maps:from_list(ConnCfg), maps:from_list(MyCfg)}.

get_metadata([], _ConnectFun, _Topic, Errors) ->
  %% failed to connect to ALL seed hosts, crash instead of return {error, Reason}
  {error, Errors};
get_metadata([Host | Rest], ConnectFun, Topic, Errors) ->
  case ConnectFun(Host) of
    {ok, Pid} ->
      try
        {ok, Vsns} = kpro:get_api_versions(Pid),
        {_, Vsn} = maps:get(metadata, Vsns),
        do_get_metadata(Vsn, Pid, Topic)
      after
        _ = close_connection(Pid)
      end;
    {error, Reason} ->
      get_metadata(Rest, ConnectFun, Topic, [{Host, Reason} | Errors])
  end.

do_get_metadata(Vsn, Connection, Topic) ->
  Req = kpro:make_request(metadata, Vsn, [{topics, [Topic]},
                                          {allow_auto_topic_creation, false}]),
  case kpro:request_sync(Connection, Req, ?DEFAULT_METADATA_TIMEOUT) of
    {ok, #kpro_rsp{msg = Meta}} ->
      BrokersMeta = kpro:find(brokers, Meta),
      Brokers = [parse_broker_meta(M) || M <- BrokersMeta],
      [TopicMeta] = kpro:find(topic_metadata, Meta),
      ErrorCode = kpro:find(error_code, TopicMeta),
      Partitions = kpro:find(partition_metadata, TopicMeta),
      case ErrorCode =:= ?no_error of
        true  -> {ok, {Brokers, Partitions}};
        false -> {error, ErrorCode} %% no such topic ?
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

log_warn(Fmt, Args) -> error_logger:warning_msg(Fmt, Args).

