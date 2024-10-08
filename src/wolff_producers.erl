%% Copyright (c) 2018-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% A per-topic gen_server which manages a number of per-partition wolff_producer workers.
-module(wolff_producers).

%% APIs
-export([start_link/3]).
-export([start_linked_producers/3, stop_linked/1]).
-export([start_supervised/3, stop_supervised/1, stop_supervised/2]).
-export([pick_producer/2, lookup_producer/2, cleanup_workers_table/1]).
%% Dynamic topics
-export([start_supervised_dynamic/2, pick_producer2/3]).
-export([add_topic/2, remove_topic/2]).

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

%% tests
-export([find_producers_by_client_topic/3]).
-export([find_producer_by_partition/4]).

-export_type([producers/0]).
-export_type([id/0, config/0, partitioner/0, max_partitions/0, gname/0]).

-include("wolff.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-opaque producers() :: dynamic_topic_producers() | topic_producers() | linked_topic_producers().

%% When calling APIs with dynamic producers, topic must be provided as function argument.
-type dynamic_topic_producers() ::
        #{partitioner := partitioner(),
          client_id := client_id(),
          group := gname(),
          topic := ?DYNAMIC
         }.

-type topic_producers() ::
        #{partitioner := partitioner(),
          client_id := client_id(),
          group := gname(),
          topic := topic()
         }.

-type linked_topic_producers() ::
        #{workers := #{partition() => pid()},
          partitioner := partitioner(),
          client_id := client_id(),
          group := gname(),
          topic := topic()
         }.

-type client_id() :: wolff:client_id().
-type gname() :: ?NO_GROUP | wolff_client:producer_group().
-type id() :: ?NS_TOPIC({client, client_id()}, topic()) %% No group
            | ?NS_TOPIC(gname(), topic()) %% With group-name, but not dynamic
            | ?NS_TOPIC(gname(), ?DYNAMIC). %% Dynamic producers
-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type config_key() :: name |
                      partitioner |
                      partition_count_refresh_interval_seconds |
                      reinit_max_attempts |
                      wolff_producer:config_key().
-type config() :: #{config_key() => term()}.
-type partitioner() :: random %% default
                     | first_key_dispatch
                     | fun((PartitionCount :: pos_integer(), [wolff:msg()]) -> partition())
                     | partition().

-define(down(Reason), {down, Reason}).
-define(rediscover_client, rediscover_client).
-define(rediscover_client_tref, rediscover_client_tref).
-define(rediscover_client_delay, 1000).
-define(init_producers, init_producers).
-define(init_producers_delay, 1000).
-define(reinit_tref, reinit_terf).
-define(not_initialized(Attempts, Reason), {not_initialized, Attempts, Reason}).
-define(initialized, initialized).
-define(partition_count_refresh_interval_seconds, 300).
-define(refresh_partition_count, refresh_partition_count).
-define(partition_count_unavailable, -1).
-define(no_timer, no_timer).
-define(reinit_max_attempts, reinit_max_attempts).
-define(reinit_max_attempts_default, 30).
-define(retry_msg, "will retry 30 times").
-define(unknown_topic_cache_expire_seconds, 30).

-type max_partitions() :: wolff_client:max_partitions().

%% @doc Called by wolff_producers_sup to start wolff_producers process.
start_link(ClientId, ID, Config) ->
  Name = maps:get(name, Config, <<>>),
  case is_atom(Name) of
    true ->
      gen_server:start_link({local, Name}, ?MODULE, {ClientId, ID, Config}, []);
    false ->
      gen_server:start_link(?MODULE, {ClientId, ID, Config}, [])
  end.

%% @doc Start wolff_producer processes linked to caller.
-spec start_linked_producers(client_id() | pid(), topic(), config()) ->
  {ok, producers()} | {error, any()}.
start_linked_producers(ClientId, Topic, ProducerCfg) when is_binary(ClientId) ->
  {ok, ClientPid} = wolff_client_sup:find_client(ClientId),
  start_linked_producers(ClientId, ClientPid, Topic, ProducerCfg);
start_linked_producers(ClientPid, Topic, ProducerCfg) when is_pid(ClientPid) ->
  ClientId = wolff_client:get_id(ClientPid),
  start_linked_producers(ClientId, ClientPid, Topic, ProducerCfg).

-spec start_linked_producers(client_id(), pid(), topic(), config()) ->
  {ok, linked_topic_producers()} | {error, any()}.
start_linked_producers(ClientId, ClientPid, Topic, ProducerCfg) ->
  MaxPartitions = maps:get(max_partitions, ProducerCfg, ?all_partitions),
  Group = get_group(ProducerCfg),
  case wolff_client:get_leader_connections(ClientPid, Group, Topic, MaxPartitions) of
    {ok, Connections} ->
      Workers = start_link_producers(ClientId, Topic, Connections, ProducerCfg),
      ok = put_partition_cnt(ClientId, Group, Topic, maps:size(Workers)),
      Partitioner = maps:get(partitioner, ProducerCfg, random),
      {ok, #{client_id => ClientId,
             group => Group,
             topic => Topic,
             workers => Workers,
             partitioner => Partitioner
            }};
    {error, Reason} ->
      {error, Reason}
  end.

stop_linked(#{workers := Workers}) when is_map(Workers) ->
  lists:foreach(
    fun({_, Pid}) ->
      wolff_producer:stop(Pid) end,
                maps:to_list(Workers)).

get_group(ProducerCfg) ->
  maps:get(group, ProducerCfg, ?NO_GROUP).

%% @doc Start supervised producers.
-spec start_supervised(client_id(), topic(), config()) -> {ok, producers()} | {error, any()}.
start_supervised(ClientId, Topic, ProducerCfg) ->
  Group = get_group(ProducerCfg),
  NS = resolve_ns(ClientId, Group),
  ID = ?NS_TOPIC(NS, Topic),
  case wolff_producers_sup:ensure_present(ClientId, ID, ProducerCfg) of
    {ok, Pid} ->
      case gen_server:call(Pid, get_status, infinity) of
        #{Topic := ?initialized} ->
          {ok, #{client_id => ClientId,
                 group => Group,
                 topic => Topic,
                 partitioner => maps:get(partitioner, ProducerCfg, random)
                }};
        Status ->
          %% This means wolff_client failed to fetch metadata
          %% for this topic.
          _ = wolff_producers_sup:ensure_absence(ID),
          case maps:find(Topic, Status) of
            {ok, ?not_initialized(_Attempts, Reason)} ->
              {error, Reason};
            error ->
              {error, unknown_topic_or_partition}
          end
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Start supervised dynamic producers.
-spec start_supervised_dynamic(client_id(), config()) -> {ok, producers()} | {error, any()}.
start_supervised_dynamic(ClientId, Config) ->
  Group = get_group(Config),
  ID = ?NS_TOPIC(Group, ?DYNAMIC),
  case wolff_producers_sup:ensure_present(ClientId, ID, Config) of
    {ok, _Pid} ->
      {ok, #{client_id => ClientId,
             group => Group,
             topic => ?DYNAMIC,
             partitioner => maps:get(partitioner, Config, random)
            }};
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Ensure workers and clean up meta data.
-spec stop_supervised(producers()) -> ok.
stop_supervised(#{client_id := ClientId, group := Group, topic := Topic}) ->
  ID = ?NS_TOPIC(resolve_ns(ClientId, Group), Topic),
  stop_supervised(ClientId, ID).

%% @doc Ensure workers and clean up meta data.
-spec stop_supervised(client_id(), id()) -> ok.
stop_supervised(ClientId, ?NS_TOPIC(NS, Topic) = ID) ->
  wolff_producers_sup:ensure_absence(ID),
  case wolff_client_sup:find_client(ClientId) of
    {ok, Pid} ->
      Group =
        case NS of
          {client, _} ->
            ?NO_GROUP;
          _ when is_binary(NS) ->
            NS
       end,
       ok = wolff_client:release_leader_conns(Pid, Group, Topic);
    {error, _} ->
       %% not running
       ok
  end.

-spec throw_unknown(_, _, _, _, _) -> no_return().
throw_unknown(ClientId, Group, Topic, Reason, RespFrom) ->
  throw(#{client_id => ClientId,
          group => Group,
          topic => Topic,
          cause => Reason,
          response => RespFrom
         }).

%% @doc Add a topic to dynamic producer.
%% Returns `ok' if the topic is already addded.
-spec add_topic(producers() | gname(), topic()) -> ok | {error, any()}.
add_topic(#{group := Group}, Topic) ->
  add_topic(Group, Topic);
add_topic(Group, Topic) ->
  ID = ?NS_TOPIC(Group, ?DYNAMIC),
  Pid = wolff_producers_sup:get_producers_pid(ID),
  gen_server:call(Pid, {add_topic, Topic}, infinity).

%% @doc Ensure a topic is removed from dynamic producer.
%% Returns `ok' if the topic is already removed.
-spec remove_topic(producers() | gname(), topic()) -> ok.
remove_topic(#{group := Group}, Topic) ->
  remove_topic(Group, Topic);
remove_topic(Group, Topic) ->
  ID = ?NS_TOPIC(Group, ?DYNAMIC),
  ok = cleanup_workers_table(Group, Topic),
  Pid = wolff_producers_sup:get_producers_pid(ID),
  gen_server:call(Pid, {remove_topic, Topic}, infinity).

%% @hidden Lookup producer pid (for test only).
lookup_producer(#{workers := Workers}, Partition) ->
  maps:get(Partition, Workers);
lookup_producer(#{group := Group, topic := Topic} = Producers, Partition) when is_binary(Topic) ->
  find_producer_by_partition2(Producers, Group, Topic, Partition).

%% @doc Retrieve the per-partition producer pid.
-spec pick_producer(producers(), [wolff:msg()]) -> {partition(), pid()}.
pick_producer(#{workers := Workers,
                partitioner := Partitioner
               }, Batch) ->
  Count = maps:size(Workers),
  Partition = pick_partition(Count, Partitioner, Batch),
  LookupFn = fun(P) -> maps:get(P, Workers) end,
  do_pick_producer(Partitioner, Partition, Count, LookupFn);
pick_producer(#{client_id := ClientId, group := Group, topic := Topic} = Producers, Batch) ->
  Count = get_partition_cnt(ClientId, Group, Topic),
  pick_producer3(Producers, Topic, Batch, Count).

%% @doc Retrieve the per-partition producer pid.
%% If topic was not added before, this function will try to added it.
-spec pick_producer2(producers(), topic(), [wolff:msg()]) -> {partition(), pid()}.
pick_producer2(#{client_id := ClientId,
                 group := Group,
                 topic := T0
                } = Producers, Topic, Batch) ->
  T0 =/= ?DYNAMIC andalso error("cannot_add_topic_to_non_dynamic_producer"),
  Count0 = get_partition_cnt(ClientId, Group, Topic),
  Count = resolve_partitions_cnt(ClientId, Group, Topic, Count0),
  pick_producer3(Producers, Topic, Batch, Count).

resolve_partitions_cnt(_ClientId, _Group, _Topic, C) when is_integer(C) andalso C > 0 ->
  C;
resolve_partitions_cnt(ClientId, Group, Topic, ?partition_count_unavailable) ->
  resolve_partitions_cnt(ClientId, Group, Topic, ?UNKNOWN(0));
resolve_partitions_cnt(ClientId, Group, Topic, ?UNKNOWN(Since)) ->
  case (now_ts() - Since) > timer:seconds(?unknown_topic_cache_expire_seconds) of
    true ->
      case add_topic(Group, Topic) of
        ok ->
          get_partition_cnt(ClientId, Group, Topic);
        {error, unknown_topic_or_partition} ->
          throw_unknown(ClientId, Group, Topic, unknown_topic_or_partition, refreshed);
        {error, Reason} ->
          throw(#{client_id => ClientId, group => Group, topics => Topic, error => Reason})
      end;
    false ->
      throw_unknown(ClientId, Group, Topic, unknown_topic_or_partition, cached)
  end.

pick_producer3(#{client_id := ClientId,
                 group := Group,
                 partitioner := Partitioner}, Topic, Batch, Count) ->
  LookupFn = fun(P) -> find_producer_by_partition2(ClientId, Group, Topic, P) end,
  try
    Partition = pick_partition(Count, Partitioner, Batch),
    do_pick_producer(Partitioner, Partition, Count, LookupFn)
  catch
    throw:Reason ->
      erlang:throw(Reason#{
                     group => Group,
                     topic => Topic,
                     client => ClientId
                   })
  end.

do_pick_producer(Partitioner, Partition0, Count, LookupFn) ->
  Pid0 = LookupFn(Partition0),
  case is_alive(Pid0) of
    true -> {Partition0, Pid0};
    false when Partitioner =:= random ->
      pick_next_alive(LookupFn, Partition0, Count);
    false ->
      throw(#{cause => producer_down, partition => Partition0})
  end.

pick_next_alive(LookupFn, Partition, Count) ->
  pick_next_alive(LookupFn, (Partition + 1) rem Count, Count, _Tried = 1).

pick_next_alive(_LookupFn, _Partition, Count, Count) ->
  throw(#{cause => all_producers_down, count => Count});
pick_next_alive(LookupFn, Partition, Count, Tried) ->
  Pid = LookupFn(Partition),
  case is_alive(Pid) of
    true -> {Partition, Pid};
    false -> pick_next_alive(LookupFn, (Partition + 1) rem Count, Count, Tried + 1)
  end.

is_alive(Pid) -> is_pid(Pid) andalso is_process_alive(Pid).

pick_partition(Count, Partitioner, _) when not is_integer(Count);
                                           Count =< 0 ->
  throw(#{cause => invalid_partition_count,
          count => Count,
          partitioner => Partitioner
         });
pick_partition(_Count, Partition, _) when is_integer(Partition) ->
  Partition;
pick_partition(Count, F, Batch) when is_function(F) ->
  F(Count, Batch);
pick_partition(Count, random, _) ->
  rand:uniform(Count) - 1;
pick_partition(Count, first_key_dispatch, [#{key := Key} | _]) ->
  erlang:phash2(Key) rem Count.

-spec init({client_id(), id(), config()}) -> {ok, map()}.
init({ClientId, ?NS_TOPIC(_, Topic) = ID, Config}) ->
  erlang:process_flag(trap_exit, true),
  self() ! ?rediscover_client,
  Status =
    case Topic =:= ?DYNAMIC of
      true ->
        #{};
      false ->
        #{Topic => ?not_initialized(0, pending)}
    end,
  {ok, #{client_id => ClientId,
         client_pid => false,
         config => Config,
         my_id => ID,
         producers_status => Status,
         refresh_tref => start_partition_refresh_timer(Config),
         ?reinit_tref => ?no_timer
        }}.

handle_info(?refresh_partition_count, #{refresh_tref := Tref, config := Config} = St) ->
    ok = ensure_timer_cancelled(Tref, ?partition_count_refresh_interval_seconds),
    ok = refresh_partition_count(St),
    {noreply, St#{refresh_tref := start_partition_refresh_timer(Config)}};
handle_info(?rediscover_client, #{client_pid := false, client_id := ClientId} = St0) ->
  St1 = St0#{?rediscover_client_tref => ?no_timer},
  case wolff_client_sup:find_client(ClientId) of
    {ok, Pid} ->
      _ = erlang:monitor(process, Pid),
      St2 = St1#{client_pid := Pid},
      St = init_producers(St2),
      ok = maybe_restart_producers(St),
      {noreply, St};
    {error, Reason} ->
      log_error("failed_to_discover_client",
                #{reason => Reason,
                  producer_id => producer_id(St0)}),
      {noreply, ensure_rediscover_client_timer(St1)}
  end;
handle_info(?init_producers, St) ->
  %% this is a retry of last failure when initializing producer procs
  {noreply, init_producers(St)};
handle_info({'DOWN', _, process, Pid, Reason}, #{client_id := ClientId,
                                                 client_pid := Pid
                                                } = St) ->
  log_error("client_pid_down", #{client_id => ClientId,
                                 client_pid => Pid,
                                 producer_id => producer_id(St),
                                 reason => Reason}),
  %% client down, try to discover it after a delay
  %% producers should all monitor client pid,
  %% expect their 'EXIT' signals soon
  {noreply, ensure_rediscover_client_timer(St#{client_pid := false})};
handle_info({'EXIT', Pid, Reason},
            #{client_id := ClientId,
              client_pid := ClientPid,
              config := Config
             } = St) ->
  case find_topic_partition_by_pid(Pid) of
    [] ->
      %% this should not happen, hence error level
      log_error("unknown_EXIT_message",
                #{pid => Pid,
                  reason => Reason,
                  producer_id => producer_id(St)});
    [{Topic, Partition}] ->
      Group = get_group(Config),
      case is_alive(ClientPid) of
        true ->
          %% wolff_producer is not designed to crash & restart
          %% if this happens, it's likely a bug in wolff_producer module
          log_error("producer_down",
                    #{producer_id => producer_id(St),
                      topic => Topic,
                      partition => Partition,
                      partition_worker => Pid,
                      reason => Reason}),
          ok = start_producer_and_insert_pid(ClientId, Group, Topic, Partition, Config);
        false ->
          %% no client, restart will be triggered when client connection is back.
          insert_producers(ClientId, Group, Topic, #{Partition => ?down(Reason)})
      end
  end,
  {noreply, St};
handle_info(Info, St) ->
  log_error("unknown_info", #{info => Info, producer_id => producer_id(St)}),
  {noreply, St}.

handle_call(get_status, _From, #{producers_status := Status} = St) ->
  {reply, Status, St};
handle_call({add_topic, Topic}, _From, #{producers_status := Status} = St) ->
  case Status of
    #{Topic := _} ->
      %% race condition with other callers
      {reply, ok, St};
    _ ->
      {NewSt, Reply} = do_add_topic(St, Topic),
      {reply, Reply, NewSt}
  end;
handle_call({remove_topic, Topic}, _From,
            #{client_id := ClientId,
              producers_status := Status,
              config := Config} = St) ->
  Group = get_group(Config),
  NewStatus = maps:remove(Topic, Status),
  case wolff_client_sup:find_client(ClientId) of
    {ok, Pid} ->
      wolff_client:release_leader_conns(Pid, Group, Topic);
    _ ->
      %% client process down or restarting,
      %% nothing to do in this case
      %% because it should have the connections released already
      ok
  end,
  {reply, ok, St#{producers_status := NewStatus}};
handle_call(Call, From, St) ->
  log_error("unknown_call", #{call => Call, from => From, producer_id => producer_id(St)}),
  {reply, {error, unknown_call}, St}.

handle_cast(Cast, St) ->
  log_error("unknown_cast", #{cast => Cast}),
  {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
  {ok, St}.

terminate(_, #{my_id := ID}) ->
  ok = cleanup_workers_table(ID).

do_add_topic(#{my_id := ?NS_TOPIC(Group, ?DYNAMIC),
               producers_status := Status} = St, Topic) ->
  {TopicStatus, Reply} =
    case init_one_topic_producers(St, Topic, _Attempts = 0) of
      ok ->
        {#{Topic => ?initialized}, ok};
      discard ->
        ok = mark_topic_unknown(Group, Topic),
        {#{}, {error, unknown_topic_or_partition}};
      {error, ?not_initialized(_, Reason) = Error} ->
        {#{Topic => Error}, {error, Reason}}
    end,
  {St#{producers_status := maps:merge(Status, TopicStatus)}, Reply}.

ensure_rediscover_client_timer(#{?rediscover_client_tref := ?no_timer} = St) ->
  Tref = erlang:send_after(?rediscover_client_delay, self(), ?rediscover_client),
  St#{?rediscover_client_tref := Tref}.

log(Level, Msg, Args) -> logger:log(Level, Args#{msg => Msg}).

log_error(Msg, Args) -> log(error, Msg, Args).

log_warning(Msg, Args) -> log(warning, Msg, Args).

log_info(Msg, Args) -> log(info, Msg, Args).

start_link_producers(ClientId, Topic, Connections, Config) ->
  lists:foldl(
    fun({Partition, MaybeConnPid}, Acc) ->
        {ok, WorkerPid} =
          wolff_producer:start_link(ClientId, Topic, Partition,
                                    MaybeConnPid, Config),
        Acc#{Partition => WorkerPid}
    end, #{}, Connections).

init_producers(#{producers_status := Status, ?reinit_tref := Tref0} = St) ->
  ok = ensure_timer_cancelled(Tref0, ?init_producers),
  {OK, ERR} = init_producers_loop(St, maps:to_list(Status)),
  NewStatus = maps:merge(OK, ERR),
  Tref = case map_size(ERR) of
    0 ->
      ?no_timer;
    _ ->
      erlang:send_after(?init_producers_delay, self(), ?init_producers)
  end,
  St#{producers_status := NewStatus,
      ?reinit_tref := Tref
     }.

%% loop over the topics and try to initialize producers for them
%% returns a tuple of {OK, ERR} where:
%%  - OK for succeeded init results
%%  - ERR for failed
%% A topic is discarded if:
%%  - It is a unknown (non-existing, or not authorized to access)
%%  - Reached maximum re-init attempts
init_producers_loop(St, Status) ->
  init_producers_loop(St, Status, #{}, #{}).

init_producers_loop(_St, [], OK, ERR) ->
  {OK, ERR};
init_producers_loop(St, [{Topic, ?not_initialized(Attempts, _)} | More], OK, ERR) ->
  case init_one_topic_producers(St, Topic, Attempts) of
    ok ->
      init_producers_loop(St, More, OK#{Topic => ?initialized}, ERR);
    {error, Status} ->
      init_producers_loop(St, More, OK, ERR#{Topic => Status});
    discard ->
      init_producers_loop(St, More, OK, ERR)
  end;
init_producers_loop(St, [{Topic, ?initialized} | More], OK, ERR) ->
  init_producers_loop(St, More, OK#{Topic => ?initialized}, ERR).

init_one_topic_producers(St, Topic, Attempts) ->
  #{client_id := ClientId, config := Config} = St,
  case start_linked_producers(ClientId, Topic, Config) of
    {ok, #{workers := Workers}} ->
      ok = insert_producers(ClientId, get_group(Config), Topic, Workers),
      log_info("kafka_producers_started_ok",
               #{topic => Topic, producer_id => producer_id(St), partitions => maps:size(Workers)}),
      ok;
    {error, unknown_topic_or_partition} ->
      discard;
    {error, Reason} ->
      Max = maps:get(?reinit_max_attempts, Config, ?reinit_max_attempts_default),
      case Attempts + 1 of
        1 ->
          log_error("failed_to_init_producers",
                    #{producer_id => producer_id(St),
                      topic => Topic,
                      reason => Reason,
                      note => ?retry_msg
                    }),
          {error, ?not_initialized(1, Reason)};
        N when N < Max ->
          {error, ?not_initialized(N, Reason)};
        N ->
          log_error("failed_to_init_producers",
                    #{producer_id => producer_id(St),
                      topic => Topic,
                      reason => Reason,
                      attempts => N,
                      note => "reached max attempts"
                    }),
          discard
      end
  end.

maybe_restart_producers(#{producers_status := Status} = St) ->
  ok = maybe_restart_producers(St, maps:to_list(Status)).

maybe_restart_producers(_St, []) ->
  ok;
maybe_restart_producers(St, [{_Topic, ?not_initialized(_Attempts, _Reason)} | More]) ->
  %% This producers of topic is yet to be initialized, ignore for now
  maybe_restart_producers(St, More);
maybe_restart_producers(#{client_id := ClientId, config := Config} = St, [{Topic, ?initialized} | More]) ->
  Producers = find_producers_by_client_topic(ClientId, get_group(Config), Topic),
  lists:foreach(
    fun({Partition, Pid}) ->
        case is_alive(Pid) of
          true -> ok;
          false -> start_producer_and_insert_pid(ClientId, get_group(Config), Topic, Partition, Config)
        end
    end, Producers),
  maybe_restart_producers(St, More).

-spec cleanup_workers_table(id()) -> ok.
cleanup_workers_table(?NS_TOPIC(NS, Topic)) ->
    cleanup_workers_table(NS, Topic).

cleanup_workers_table(NS, Topic) ->
  Ms =
    case Topic =:= ?DYNAMIC of
      true ->
        ets:fun2ms(fun({{N, _, _}, _}) when N =:= NS -> true end);
      false ->
        ets:fun2ms(fun({{N, T, _}, _}) when N =:= NS andalso T =:= Topic -> true end)
    end,
  ets:select_delete(?WOLFF_PRODUCERS_GLOBAL_TABLE, Ms),
  ok.

find_producer_by_partition(ClientId, Group, Topic, Partition) when is_integer(Partition) ->
  NS = resolve_ns(ClientId, Group),
  case ets_lookup_val(?WOLFF_PRODUCERS_GLOBAL_TABLE, {NS, Topic, Partition}, false) of
    false ->
      {error, #{reason => producer_not_found,
                client => ClientId,
                topic => Topic,
                group => Group,
                partition => Partition}};
    Pid ->
      {ok, Pid}
  end.

find_producer_by_partition2(ClientId, Group, Topic, Partition) ->
  case find_producer_by_partition(ClientId, Group, Topic, Partition) of
    {ok, Pid} ->
      Pid;
    {error, Reason} ->
      throw(Reason)
  end.

find_producers_by_client_topic(ClientId, Group, Topic) ->
  NS = resolve_ns(ClientId, Group),
  Ms = ets:fun2ms(
         fun({{N, T, P}, Pid}) when N =:= NS andalso T =:= Topic andalso is_integer(P)->
                 {P, Pid}
         end),
  ets:select(?WOLFF_PRODUCERS_GLOBAL_TABLE, Ms).

find_topic_partition_by_pid(Pid) ->
  Ms = ets:fun2ms(fun({{_NS, Topic, Partition}, P}) when P =:= Pid -> {Topic, Partition} end),
  ets:select(?WOLFF_PRODUCERS_GLOBAL_TABLE, Ms).

insert_producers(ClientId, Group, Topic, Workers0) ->
  NS = resolve_ns(ClientId, Group),
  Workers = lists:map(fun({Partition, Pid}) -> {{NS, Topic, Partition}, Pid} end, maps:to_list(Workers0)),
  true = ets:insert(?WOLFF_PRODUCERS_GLOBAL_TABLE, Workers),
  ok.

start_producer_and_insert_pid(ClientId, Group, Topic, Partition, Config) ->
  {ok, Pid} = wolff_producer:start_link(ClientId, Topic, Partition,
                                        ?conn_down(to_be_discovered), Config),
  ok = insert_producers(ClientId, Group, Topic, #{Partition => Pid}).

%% Config is not used so far.
start_partition_refresh_timer(Config) ->
  IntervalSeconds = maps:get(partition_count_refresh_interval_seconds, Config,
                             ?partition_count_refresh_interval_seconds),
  case IntervalSeconds of
      0 ->
          ?no_timer;
      _ ->
          Interval = timer:seconds(IntervalSeconds),
          erlang:send_after(Interval, self(), ?refresh_partition_count)
  end.

refresh_partition_count(#{client_pid := Pid}) when not is_pid(Pid) ->
  %% client is to be (re)discovered
  ok;
refresh_partition_count(#{producers_status := Status} = St) ->
  ok = refresh_partition_count(St, maps:to_list(Status)).

refresh_partition_count(_St, []) ->
  ok;
refresh_partition_count(St, [{_Topic, ?not_initialized(_Attempts, _Reason)} | More]) ->
  %% to be initialized
  refresh_partition_count(St, More);
refresh_partition_count(#{client_pid := Pid, config := Config} = St, [{Topic, ?initialized} | More]) ->
  Group = get_group(Config),
  MaxPartitions = maps:get(max_partitions, Config, ?all_partitions),
  case wolff_client:get_leader_connections(Pid, Group, Topic, MaxPartitions) of
    {ok, Connections} ->
      start_new_producers(St, Topic, Connections);
    {error, Reason} ->
      log_warning("failed_to_refresh_partition_count_will_retry",
                  #{topic => Topic, group => Group, reason => Reason})
  end,
  refresh_partition_count(St, More).

start_new_producers(#{client_id := ClientId,
                      config := Config
                     } = St, Topic, Connections0) ->
  Group = get_group(Config),
  NowCount = length(Connections0),
  %% process only the newly discovered connections
  F = fun({Partition, _MaybeConnPid} = New, {OldCnt, NewAcc}) ->
        case find_producer_by_partition(ClientId, Group, Topic, Partition) of
          {ok, _} ->
            {OldCnt + 1, NewAcc};
          {error, #{reason := producer_not_found}} ->
            {OldCnt, [New | NewAcc]}
        end
      end,
  {OldCount, Connections} = lists:foldl(F, {0, []}, Connections0),
  Workers = start_link_producers(ClientId, Topic, Connections, Config),
  ok = insert_producers(ClientId, Group, Topic, Workers),
  case NowCount - OldCount of
    Diff when Diff > 0 ->
      log_info("started_producers_for_newly_discovered_partitions",
               #{diff => Diff, total => NowCount, producer_id => producer_id(St)}),
      ok = put_partition_cnt(ClientId, Group, Topic, NowCount);
    _ ->
      ok
  end.

-if(?OTP_RELEASE >= 26).
ets_lookup_val(Tab, Key, Default) ->
  ets:lookup_element(Tab, Key, 2, Default).
-else.
ets_lookup_val(Tab, Key, Default) ->
  try
    ets:lookup_element(Tab, Key, 2)
  catch
    error:badarg ->
      Default
  end.
-endif.

get_partition_cnt(ClientId, Group, Topic) ->
  NS = resolve_ns(ClientId, Group),
  ets_lookup_val(?WOLFF_PRODUCERS_GLOBAL_TABLE,
                 {NS, Topic, partition_count},
                 ?partition_count_unavailable).

put_partition_cnt(ClientId, Group, Topic, Count) ->
  NS = resolve_ns(ClientId, Group),
  true = ets:insert(?WOLFF_PRODUCERS_GLOBAL_TABLE, {{NS, Topic, partition_count}, Count}),
  ok.

%% Insert a special cache value for the 'unknown' status of a topic.
%% This is to avoid overwhelming wolff_producers_sup process by calling which_children
%% concurrently from many callers.
mark_topic_unknown(Group, Topic) ->
  true = ets:insert(?WOLFF_PRODUCERS_GLOBAL_TABLE,
                    {{Group, Topic, partition_count},
                    ?UNKNOWN(now_ts())}),
  ok.

ensure_timer_cancelled(Tref, Msg) when is_reference(Tref) ->
  _ = erlang:cancel_timer(Tref),
  receive
    Msg ->
      ok
  after
    0 ->
      ok
  end,
  ok;
ensure_timer_cancelled(_Tref, _Msg) ->
  ok.

producer_id(#{my_id := ?NS_TOPIC(NS, Topic)}) ->
  producer_id(NS, Topic).

producer_id({client, NS}, Topic) ->
  producer_id(NS, Topic);
producer_id(NS, ?DYNAMIC) ->
  NS;
producer_id(NS, Topic) ->
  <<NS/binary, $:, Topic/binary>>.

%% Get the namespace for wolff_producers_sup and ets table.
-compile({inline, resolve_ns/2}).
resolve_ns(ClientId, ?NO_GROUP) ->
  {client, ClientId};
resolve_ns(_ClientId, Group) when is_binary(Group) ->
  Group.

now_ts() ->
  erlang:system_time(millisecond).
