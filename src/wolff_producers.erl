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

%% A per-topic gen_server which manages a number of per-partition wolff_producer workers.
-module(wolff_producers).

%% APIs
-export([start_link/3]).
-export([start_linked_producers/3, stop_linked/1]).
-export([start_supervised/3, stop_supervised/1, stop_supervised/2]).
-export([pick_producer/2, lookup_producer/2, cleanup_workers_table/2]).
-export([find_producers_by_client_topic/2]).

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

%% tests
-export([find_producer_by_partition/3]).

-export_type([producers/0, config/0, partitioner/0, producer_alias/0, topic_or_alias/0, alias_and_topic/0, max_partitions/0]).

-include("wolff.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-opaque producers() ::
        #{workers => #{partition() => pid()},
          partitioner := partitioner(),
          client_id := wolff:client_id(),
          topic := alias_and_topic()
         }.

-type producer_alias() :: binary().
-type alias_and_topic() :: wolff_client:alias_and_topic().
-type topic() :: kpro:topic().
-type topic_or_alias() :: wolff_client:topic_or_alias().
-type partition() :: kpro:partition().
-type config_key() :: name | partitioner | partition_count_refresh_interval_seconds |
                      alias | wolff_producer:config_key().
-type config() :: #{config_key() => term()}.
-type partitioner() :: random %% default
                     | roundrobin
                     | first_key_dispatch
                     | fun((PartitionCount :: pos_integer(), [wolff:msg()]) -> partition())
                     | partition().

-define(down(Reason), {down, Reason}).
-define(rediscover_client, rediscover_client).
-define(rediscover_client_tref, rediscover_client_tref).
-define(rediscover_client_delay, 1000).
-define(init_producers, init_producers).
-define(init_producers_delay, 1000).
-define(not_initialized, not_initialized).
-define(initialized, initialized).
-define(partition_count_refresh_interval_seconds, 300).
-define(refresh_partition_count, refresh_partition_count).
-define(partition_count_unavailable, -1).

-type max_partitions() :: wolff_client:max_partitions().

%% @doc Called by wolff_producers_sup to start wolff_producers process.
start_link(ClientId, TopicOrAlias, Config) ->
  Name = maps:get(name, Config, <<>>),
  case is_atom(Name) of
    true ->
      gen_server:start_link({local, Name}, ?MODULE, {ClientId, TopicOrAlias, Config}, []);
    false ->
      gen_server:start_link(?MODULE, {ClientId, TopicOrAlias, Config}, [])
  end.

%% @doc Start wolff_producer processes linked to caller.
-spec start_linked_producers(wolff:client_id() | pid(), topic(), config()) ->
  {ok, producers()} | {error, any()}.
start_linked_producers(ClientId, Topic, ProducerCfg) when is_binary(ClientId) ->
  {ok, ClientPid} = wolff_client_sup:find_client(ClientId),
  start_linked_producers(ClientId, ClientPid, Topic, ProducerCfg);
start_linked_producers(ClientPid, Topic, ProducerCfg) when is_pid(ClientPid) ->
  ClientId = wolff_client:get_id(ClientPid),
  start_linked_producers(ClientId, ClientPid, Topic, ProducerCfg).

-spec start_linked_producers(wolff:client_id(), pid(), topic_or_alias(), config()) ->
  {ok, producers()} | {error, any()}.
start_linked_producers(ClientId, ClientPid, TopicOrAlias, ProducerCfg) ->
  MaxPartitions = maps:get(max_partitions, ProducerCfg, ?all_partitions),
  AliasTopic = case TopicOrAlias of
                   {_Alias, _Topic} ->
                       TopicOrAlias;
                   Topic0 ->
                       Alias = maps:get(alias, ProducerCfg, ?NO_ALIAS),
                       {Alias, Topic0}
               end,
  case wolff_client:get_leader_connections(ClientPid, AliasTopic, MaxPartitions) of
    {ok, Connections} ->
      Workers = start_link_producers(ClientId, AliasTopic, Connections, ProducerCfg),
      ok = put_partition_cnt(ClientId, AliasTopic, maps:size(Workers)),
      Partitioner = maps:get(partitioner, ProducerCfg, random),
      {ok, #{client_id => ClientId,
             topic => AliasTopic,
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

%% @doc Start supervised producers.
-spec start_supervised(wolff:client_id(), topic(), config()) -> {ok, producers()} | {error, any()}.
start_supervised(ClientId, Topic, ProducerCfg) ->
  case wolff_producers_sup:ensure_present(ClientId, Topic, ProducerCfg) of
    {ok, Pid} ->
      case gen_server:call(Pid, get_workers, infinity) of
        ?not_initialized ->
          %% This means wolff_client failed to fetch metadata
          %% for this topic.
          Alias = maps:get(alias, ProducerCfg, ?NO_ALIAS),
          AliasTopic = {Alias, Topic},
          _ = wolff_producers_sup:ensure_absence(ClientId, AliasTopic),
          {error, failed_to_initialize_producers_in_time};
        _ ->
          Alias = maps:get(alias, ProducerCfg, ?NO_ALIAS),
          AliasTopic = {Alias, Topic},
          {ok, #{client_id => ClientId,
                 topic => AliasTopic,
                 partitioner => maps:get(partitioner, ProducerCfg, random)
                }}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Ensure workers and clean up meta data.
-spec stop_supervised(producers()) -> ok.
stop_supervised(#{client_id := ClientId, topic := AliasTopic}) ->
  stop_supervised(ClientId, AliasTopic).

%% @doc Ensure workers and clean up meta data.
-spec stop_supervised(wolff:client_id(), topic_or_alias()) -> ok.
stop_supervised(ClientId, TopicOrAlias0) ->
  TopicOrAlias = ensure_has_alias(TopicOrAlias0),
  wolff_producers_sup:ensure_absence(ClientId, TopicOrAlias),
  case wolff_client_sup:find_client(ClientId) of
    {ok, Pid} ->
       ok = wolff_client:delete_producers_metadata(Pid, TopicOrAlias);
    {error, _} ->
       %% not running
       ok
  end.

%% @doc Lookup producer pid.
lookup_producer(#{workers := Workers}, Partition) ->
  maps:get(Partition, Workers);
lookup_producer(#{client_id := ClientId, topic := TopicOrAlias}, Partition) ->
  {ok, Pid} = find_producer_by_partition(ClientId, TopicOrAlias, Partition),
  Pid.

%% @doc Retrieve the per-partition producer pid.
-spec pick_producer(producers(), [wolff:msg()]) -> {partition(), pid()}.
pick_producer(#{workers := Workers,
                partitioner := Partitioner
               }, Batch) ->
  Count = maps:size(Workers),
  Partition = pick_partition(Count, Partitioner, Batch),
  LookupFn = fun(P) -> maps:get(P, Workers) end,
  do_pick_producer(Partitioner, Partition, Count, LookupFn);
pick_producer(#{partitioner := Partitioner,
                client_id := ClientId,
                topic := TopicOrAlias
               }, Batch) ->
  Count = get_partition_cnt(ClientId, TopicOrAlias),
  Partition = pick_partition(Count, Partitioner, Batch),
  LookupFn = fun(P) ->
    {ok, Pid} = find_producer_by_partition(ClientId, TopicOrAlias, P),
    Pid
  end,
  try
    do_pick_producer(Partitioner, Partition, Count, LookupFn)
  catch
    throw:Reason ->
      erlang:throw(#{reason => Reason,
                     topic => get_topic(TopicOrAlias),
                     alias => get_alias(TopicOrAlias),
                     partition => Partition,
                     client => ClientId
                    })
  end.

do_pick_producer(Partitioner, Partition0, Count, LookupFn) ->
  Pid0 = LookupFn(Partition0),
  case is_alive(Pid0) of
    true -> {Partition0, Pid0};
    false when Partitioner =:= random ->
      pick_next_alive(LookupFn, Partition0, Count);
    false when Partitioner =:= roundrobin ->
      R = {Partition1, _Pid1} = pick_next_alive(LookupFn, Partition0, Count),
      _ = put(wolff_roundrobin, (Partition1 + 1) rem Count),
      R;
    false ->
      throw(producer_down)
  end.

pick_next_alive(LookupFn, Partition, Count) ->
  pick_next_alive(LookupFn, (Partition + 1) rem Count, Count, _Tried = 1).

pick_next_alive(_LookupFn, _Partition, Count, Count) ->
  throw(all_producers_down);
pick_next_alive(LookupFn, Partition, Count, Tried) ->
  Pid = LookupFn(Partition),
  case is_alive(Pid) of
    true -> {Partition, Pid};
    false -> pick_next_alive(LookupFn, (Partition + 1) rem Count, Count, Tried + 1)
  end.

is_alive(Pid) -> is_pid(Pid) andalso is_process_alive(Pid).

pick_partition(_Count, Partition, _) when is_integer(Partition) ->
  Partition;
pick_partition(Count, F, Batch) when is_function(F) ->
  F(Count, Batch);
pick_partition(Count, Partitioner, _) when not is_integer(Count);
                                           Count =< 0 ->
  error({invalid_partition_count, Count, Partitioner});
pick_partition(Count, random, _) ->
  rand:uniform(Count) - 1;
pick_partition(Count, roundrobin, _) ->
  Partition = case get(wolff_roundrobin) of
                undefined -> 0;
                Number    -> Number
              end,
  _ = put(wolff_roundrobin, (Partition + 1) rem Count),
  Partition;
pick_partition(Count, first_key_dispatch, [#{key := Key} | _]) ->
  erlang:phash2(Key) rem Count.

-spec init({wolff:client_id(), topic(), config()}) -> {ok, map()}.
init({ClientId, Topic, Config}) ->
  erlang:process_flag(trap_exit, true),
  self() ! ?rediscover_client,
  {ok, #{client_id => ClientId,
         client_pid => false,
         topic => Topic,
         config => Config,
         producers_status => ?not_initialized,
         refresh_tref => start_partition_refresh_timer(Config)
        }}.

handle_info(?refresh_partition_count, #{refresh_tref := Tref, config := Config} = St0) ->
    %% this message can be sent from anywhere,
    %% so we should ensure the timer is cancelled before starting a new one
    ok = ensure_timer_cancelled(Tref),
    St = refresh_partition_count(St0),
    {noreply, St#{refresh_tref := start_partition_refresh_timer(Config)}};
handle_info(?rediscover_client, #{client_id := ClientId,
                                  client_pid := false,
                                  topic := TopicOrAlias
                                 } = St0) ->
  St1 = St0#{?rediscover_client_tref => false},
  case wolff_client_sup:find_client(ClientId) of
    {ok, Pid} ->
      _ = erlang:monitor(process, Pid),
      St2 = St1#{client_pid := Pid},
      St3 = maybe_init_producers(St2),
      St = maybe_restart_producers(St3),
      {noreply, St};
    {error, Reason} ->
      log_error("failed_to_discover_client",
                #{reason => Reason, topic => get_topic(TopicOrAlias),
                  alias => get_alias(TopicOrAlias), client_id => ClientId}),
      {noreply, ensure_rediscover_client_timer(St1)}
  end;
handle_info(?init_producers, St) ->
  %% this is a retry of last failure when initializing producer procs
  {noreply, maybe_init_producers(St)};
handle_info({'DOWN', _, process, Pid, Reason}, #{client_id := ClientId,
                                                 client_pid := Pid,
                                                 topic := TopicOrAlias
                                                } = St) ->
  log_error("client_pid_down", #{client_id => ClientId,
                                 topic => get_topic(TopicOrAlias),
                                 alias => get_alias(TopicOrAlias),
                                 client_pid => Pid,
                                 reason => Reason}),
  %% client down, try to discover it after a delay
  %% producers should all monitor client pid,
  %% expect their 'EXIT' signals soon
  {noreply, ensure_rediscover_client_timer(St#{client_pid := false})};
handle_info({'EXIT', Pid, Reason},
            #{topic := TopicOrAlias,
              client_id := ClientId,
              client_pid := ClientPid,
              config := Config
             } = St) ->
  case find_partition_by_pid(Pid) of
    [] ->
      %% this should not happen, hence error level
      log_error("unknown_EXIT_message", #{pid => Pid, reason => Reason});
    [Partition] ->
      case is_alive(ClientPid) of
        true ->
          %% wolff_producer is not designed to crash & restart
          %% if this happens, it's likely a bug in wolff_producer module
          log_error("producer_down",
                    #{topic => get_topic(TopicOrAlias), alias => get_alias(TopicOrAlias),
                      partition => Partition,
                      partition_worker => Pid, reason => Reason}),
          ok = start_producer_and_insert_pid(ClientId, TopicOrAlias, Partition, Config);
        false ->
          %% no client, restart will be triggered when client connection is back.
          insert_producers(ClientId, TopicOrAlias, #{Partition => ?down(Reason)})
      end
  end,
  {noreply, St};
handle_info(Info, St) ->
  log_error("unknown_info", #{info => Info}),
  {noreply, St}.

handle_call(get_workers, _From, #{producers_status := Status} = St) ->
  {reply, Status, St};
handle_call(Call, From, St) ->
  log_error("unknown_call", #{call => Call, from => From}),
  {reply, {error, unknown_call}, St}.

handle_cast(Cast, St) ->
  log_error("unknown_cast", #{cast => Cast}),
  {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
  {ok, St}.

terminate(_, #{client_id := ClientId, topic := Topic}) ->
  ok = cleanup_workers_table(ClientId, Topic).

ensure_rediscover_client_timer(#{?rediscover_client_tref := false} = St) ->
  Tref = erlang:send_after(?rediscover_client_delay, self(), ?rediscover_client),
  St#{?rediscover_client_tref := Tref}.

log(Level, Msg, Args) -> logger:log(Level, Args#{msg => Msg}).

log_error(Msg, Args) -> log(error, Msg, Args).

log_warning(Msg, Args) -> log(warning, Msg, Args).

log_info(Msg, Args) -> log(info, Msg, Args).

start_link_producers(ClientId, TopicOrAlias, Connections, Config) ->
  Topic = get_topic(TopicOrAlias),
  lists:foldl(
    fun({Partition, MaybeConnPid}, Acc) ->
        {ok, WorkerPid} =
          wolff_producer:start_link(ClientId, Topic, Partition,
                                    MaybeConnPid, Config),
        Acc#{Partition => WorkerPid}
    end, #{}, Connections).

maybe_init_producers(#{producers_status := ?not_initialized,
                       topic := TopicOrAlias,
                       client_id := ClientId,
                       config := Config
                      } = St) ->
  case start_linked_producers(ClientId, TopicOrAlias, Config) of
    {ok, #{workers := Workers}} ->
      ok = insert_producers(ClientId, TopicOrAlias, Workers),
      St#{producers_status := ?initialized};
    {error, Reason} ->
      log_error("failed_to_init_producers", #{topic => get_topic(TopicOrAlias), alias => get_alias(TopicOrAlias), reason => Reason}),
      erlang:send_after(?init_producers_delay, self(), ?init_producers),
      St
  end;
maybe_init_producers(St) ->
  St.

maybe_restart_producers(#{producers_status := ?not_initialized} = St) -> St;
maybe_restart_producers(#{client_id := ClientId,
                          topic := TopicOrAlias,
                          config := Config
                         } = St) ->
  Producers = find_producers_by_client_topic(ClientId, TopicOrAlias),
  lists:foreach(
    fun({Partition, Pid}) ->
        case is_alive(Pid) of
          true -> ok;
          false -> start_producer_and_insert_pid(ClientId, TopicOrAlias, Partition, Config)
        end
    end, Producers),
  St.

-spec cleanup_workers_table(wolff:client_id(), topic_or_alias()) -> ok.
cleanup_workers_table(ClientId, TopicOrAlias) ->
  Ms = ets:fun2ms(fun({{C, T, _}, _}) when C =:= ClientId andalso T =:= TopicOrAlias -> true end),
  ets:select_delete(?WOLFF_PRODUCERS_GLOBAL_TABLE, Ms),
  ok.

find_producer_by_partition(ClientId, TopicOrAlias0, Partition) when is_integer(Partition) ->
  TopicOrAlias = ensure_has_alias(TopicOrAlias0),
  case ets:lookup(?WOLFF_PRODUCERS_GLOBAL_TABLE, {ClientId, TopicOrAlias, Partition}) of
    [{{_, _, _}, Pid}] ->
      {ok, Pid};
    [] ->
      {error, #{reason => producer_not_found,
                client => ClientId,
                topic => get_topic(TopicOrAlias),
                alias => get_alias(TopicOrAlias),
                partition => Partition}}
  end.

find_producers_by_client_topic(ClientId, TopicOrAlias0) ->
  TopicOrAlias = ensure_has_alias(TopicOrAlias0),
  Ms = ets:fun2ms(
         fun({{C, T, P}, Pid}) when C =:= ClientId andalso T =:= TopicOrAlias andalso is_integer(P)->
                 {P, Pid}
         end),
  ets:select(?WOLFF_PRODUCERS_GLOBAL_TABLE, Ms).

find_partition_by_pid(Pid) ->
  Ms = ets:fun2ms(fun({{_, _, Partition}, P}) when P =:= Pid -> Partition end),
  ets:select(?WOLFF_PRODUCERS_GLOBAL_TABLE, Ms).

insert_producers(ClientId, TopicOrAlias, Workers0) ->
  Workers = lists:map(fun({Partition, Pid}) ->
    {{ClientId, TopicOrAlias, Partition}, Pid}
  end, maps:to_list(Workers0)),
  true = ets:insert(?WOLFF_PRODUCERS_GLOBAL_TABLE, Workers),
  ok.

start_producer_and_insert_pid(ClientId, TopicOrAlias, Partition, Config) ->
  Topic = get_topic(TopicOrAlias),
  {ok, Pid} = wolff_producer:start_link(ClientId, Topic, Partition,
                                        ?conn_down(to_be_discovered), Config),
  ok = insert_producers(ClientId, TopicOrAlias, #{Partition => Pid}).

%% Config is not used so far.
start_partition_refresh_timer(Config) ->
  IntervalSeconds = maps:get(partition_count_refresh_interval_seconds, Config,
                             ?partition_count_refresh_interval_seconds),
  case IntervalSeconds of
      0 ->
          undefined;
      _ ->
          Interval = timer:seconds(IntervalSeconds),
          erlang:send_after(Interval, self(), ?refresh_partition_count)
  end.

refresh_partition_count(#{client_pid := Pid} = St) when not is_pid(Pid) ->
  %% client is to be (re)discovered
  St;
refresh_partition_count(#{producers_status := ?not_initialized} = St) ->
  %% to be initialized
  St;
refresh_partition_count(#{client_pid := Pid, topic := TopicOrAlias, config := Config} = St) ->
  MaxPartitions = maps:get(max_partitions, Config, ?all_partitions),
  case wolff_client:get_leader_connections(Pid, TopicOrAlias, MaxPartitions) of
    {ok, Connections} ->
      start_new_producers(St, Connections);
    {error, Reason} ->
      log_warning("failed_to_refresh_partition_count_will_retry",
                  #{topic => get_topic(TopicOrAlias), alias => get_alias(TopicOrAlias), reason => Reason}),
      St
  end.

start_new_producers(#{client_id := ClientId,
                      topic := TopicOrAlias,
                      config := Config
                     } = St, Connections0) ->
  NowCount = length(Connections0),
  %% process only the newly discovered connections
  F = fun({Partition, _MaybeConnPid} = New, {OldCnt, NewAcc}) ->
        case find_producer_by_partition(ClientId, TopicOrAlias, Partition) of
          {ok, _} ->
            {OldCnt + 1, NewAcc};
          {error, #{reason := producer_not_found}} ->
            {OldCnt, [New | NewAcc]}
        end
      end,
  {OldCount, Connections} = lists:foldl(F, {0, []}, Connections0),
  Workers = start_link_producers(ClientId, TopicOrAlias, Connections, Config),
  ok = insert_producers(ClientId, TopicOrAlias, Workers),
  case OldCount < NowCount of
    true ->
      log_info("started_producers_for_newly_discovered_partitions",
               #{workers => Workers}),
      ok = put_partition_cnt(ClientId, TopicOrAlias, NowCount);
    false ->
      ok
  end,
  St.


-if(OTP_RELEASE >= "26").
get_partition_cnt(ClientId, Topic) ->
  ets:lookup_element(?WOLFF_PRODUCERS_GLOBAL_TABLE, {ClientId, Topic, partition_count},
                     2, ?partition_count_unavailable).
-else.
get_partition_cnt(ClientId, Topic) ->
  try ets:lookup_element(?WOLFF_PRODUCERS_GLOBAL_TABLE, {ClientId, Topic, partition_count},
                     2)
  catch
    error:badarg ->
      ?partition_count_unavailable
  end.
-endif.

put_partition_cnt(ClientId, Topic, Count) ->
  _ = ets:insert(?WOLFF_PRODUCERS_GLOBAL_TABLE, {{ClientId, Topic, partition_count}, Count}),
  ok.

ensure_timer_cancelled(Tref) when is_reference(Tref) ->
  _ = erlang:cancel_timer(Tref),
  ok;
ensure_timer_cancelled(_) ->
  ok.

-spec get_topic(topic_or_alias()) -> topic().
get_topic({_Alias, Topic}) -> Topic;
get_topic(Topic) -> Topic.

-spec get_alias(topic_or_alias()) -> producer_alias().
get_alias({Alias, _Topic}) -> Alias;
get_alias(_Topic) -> ?NO_ALIAS.

-spec ensure_has_alias(topic_or_alias()) -> alias_and_topic().
ensure_has_alias({Alias, Topic}) -> {Alias, Topic};
ensure_has_alias(Topic) -> {?NO_ALIAS, Topic}.
