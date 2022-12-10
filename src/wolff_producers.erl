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
-export([start_supervised/3, stop_supervised/1, stop_supervised/3]).
-export([pick_producer/2, lookup_producer/2]).

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

-export_type([producers/0, config/0]).

-include("wolff.hrl").

-opaque producers() ::
        #{workers := #{partition() => pid()} | ets:tab(),
          partitioner := partitioner(),
          client_id => wolff:client_id(),
          topic => kpro:topic()
         }.

-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type config_key() :: name | partitioner | partition_count_refresh_interval_seconds |
                      wolff_producer:config_key().
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
-define(partition_count_refresh_interval_seconds, 300).
-define(refresh_partition_count, refresh_partition_count).

%% @doc start wolff_producdrs gen_server
start_link(ClientId, Topic, Config) ->
  Name = get_name(Config),
  gen_server:start_link({local, Name}, ?MODULE, {ClientId, Topic, Config}, []).

%% @doc start wolff_producdrs gen_server
-spec start_linked_producers(wolff:client_id() | pid(), topic(), config()) ->
  {ok, producers()} | {error, any()}.
start_linked_producers(ClientId, Topic, ProducerCfg) when is_binary(ClientId) ->
  {ok, ClientPid} = wolff_client_sup:find_client(ClientId),
  start_linked_producers(ClientId, ClientPid, Topic, ProducerCfg);
start_linked_producers(ClientPid, Topic, ProducerCfg) when is_pid(ClientPid) ->
  ClientId = wolff_client:get_id(ClientPid),
  start_linked_producers(ClientId, ClientPid, Topic, ProducerCfg).

start_linked_producers(ClientId, ClientPid, Topic, ProducerCfg) ->
  case wolff_client:get_leader_connections(ClientPid, Topic) of
    {ok, Connections} ->
      Workers = start_link_producers(ClientId, Topic, Connections, ProducerCfg),
      ok = put_partition_cnt(ClientId, Topic, maps:size(Workers)),
      Partitioner = maps:get(partitioner, ProducerCfg, random),
      {ok, #{client_id => ClientId,
             topic => Topic,
             workers => Workers,
             partitioner => Partitioner}};
    {error, Reason} ->
      {error, Reason}
  end.

stop_linked(#{workers := Workers}) when is_map(Workers) ->
  lists:foreach(fun({_, Pid}) -> wolff_producer:stop(Pid) end,
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
          _ = wolff_producers_sup:ensure_absence(ClientId, get_name(ProducerCfg)),
          {error, failed_to_initialize_producers_in_time};
        Ets ->
          {ok, #{client_id => ClientId,
                topic => Topic,
                workers => Ets,
                partitioner => maps:get(partitioner, ProducerCfg, random)
                }}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Ensure workers and clean up meta data.
-spec stop_supervised(producers()) -> ok.
stop_supervised(#{client_id := ClientId, workers := NamedEts, topic := Topic}) ->
  stop_supervised(ClientId, Topic, NamedEts).

%% @doc Ensure workers and clean up meta data.
-spec stop_supervised(wolff:client_id(), topic(), wolff:name()) -> ok.
stop_supervised(ClientId, Topic, NamedEts) ->
  wolff_producers_sup:ensure_absence(ClientId, NamedEts),
  case wolff_client_sup:find_client(ClientId) of
    {ok, Pid} ->
       ok = wolff_client:delete_producers_metadata(Pid, Topic);
    {error, _} ->
       %% not running
       ok
  end.

%% @doc Retrieve the per-partition producer pid.
-spec pick_producer(producers(), [wolff:msg()]) -> {partition(), pid()}.
pick_producer(#{workers := Workers,
                partitioner := Partitioner,
                client_id := ClientId,
                topic := Topic
               }, Batch) ->
  Count = partition_cnt(ClientId, Topic),
  Partition = pick_partition(Count, Partitioner, Batch),
  do_pick_producer(Partitioner, Partition, Count, Workers).

do_pick_producer(Partitioner, Partition0, Count, Workers) ->
  Pid0 = lookup_producer(Workers, Partition0),
  case is_pid(Pid0) andalso is_process_alive(Pid0) of
    true -> {Partition0, Pid0};
    false when Partitioner =:= random ->
      pick_next_alive(Workers, Partition0, Count);
    false when Partitioner =:= roundrobin ->
      R = {Partition1, _Pid1} = pick_next_alive(Workers, Partition0, Count),
      _ = put(wolff_roundrobin, (Partition1 + 1) rem Count),
      R;
    false ->
      erlang:error({producer_down, Pid0})
  end.

pick_next_alive(Workers, Partition, Count) ->
  pick_next_alive(Workers, (Partition + 1) rem Count, Count, _Tried = 1).

pick_next_alive(_Workers, _Partition, Count, Count) ->
  erlang:error(all_producers_down);
pick_next_alive(Workers, Partition, Count, Tried) ->
  Pid = lookup_producer(Workers, Partition),
  case is_alive(Pid) of
    true -> {Partition, Pid};
    false -> pick_next_alive(Workers, (Partition + 1) rem Count, Count, Tried + 1)
  end.

is_alive(Pid) -> is_pid(Pid) andalso is_process_alive(Pid).

lookup_producer(#{workers := Workers}, Partition) ->
  lookup_producer(Workers, Partition);
lookup_producer(Workers, Partition) when is_map(Workers) ->
  maps:get(Partition, Workers);
lookup_producer(Workers, Partition) ->
  [{Partition, Pid}] = ets:lookup(Workers, Partition),
  Pid.

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

-spec init({wolff:client_id(), wolff:topic(), config()}) -> {ok, map()}.
init({ClientId, Topic, Config}) ->
  erlang:process_flag(trap_exit, true),
  self() ! ?rediscover_client,
  {ok, #{client_id => ClientId,
         client_pid => false,
         topic => Topic,
         config => Config,
         ets => ?not_initialized,
         refresh_tref => start_partition_refresh_timer(Config)
        }}.

handle_info(?refresh_partition_count, #{refresh_tref := Tref, config := Config} = St0) ->
    %% this message can be sent from anywhere,
    %% so we should ensure the timer is cancelled before starting a new one
    ok = ensure_timer_cancelled(Tref),
    St = refresh_partition_count(St0),
    {noreply, St#{refresh_tref := start_partition_refresh_timer(Config)}};
handle_info(?rediscover_client, #{client_id := ClientId,
                                  client_pid := false
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
      log_error("Failed to discover client, reason = ~p", [Reason]),
      {noreply, ensure_rediscover_client_timer(St1)}
  end;
handle_info(?init_producers, St) ->
  %% this is a retry of last failure when initializing producer procs
  {noreply, maybe_init_producers(St)};
handle_info({'DOWN', _, process, Pid, Reason}, #{client_id := ClientId,
                                                 client_pid := Pid
                                                } = St) ->
  log_error("Client ~p (pid = ~p) down, reason: ~p", [ClientId, Pid, Reason]),
  %% client down, try to discover it after a delay
  %% producers should all monitor client pid,
  %% expect their 'EXIT' signals soon
  {noreply, ensure_rediscover_client_timer(St#{client_pid := false})};
handle_info({'EXIT', Pid, Reason},
            #{ets := Ets,
              topic := Topic,
              client_id := ClientId,
              client_pid := ClientPid,
              config := Config
             } = St) ->
  case ets:match(Ets, {'$1', Pid}) of
    [] ->
      log_error("Unknown EXIT message of pid ~p reason: ~p", [Pid, Reason]);
    [[Partition]] ->
      case is_alive(ClientPid) of
        true ->
          log_error("Producer ~s-~p (pid = ~p) down\nreason: ~p",
                    [Topic, Partition, Pid, Reason]),
          ok = start_producer_and_insert_pid(Ets, ClientId, Topic, Partition, Config);
        false ->
          %% no client, restart will be triggered when client connection is back.
          ets:insert(Ets, {Partition, ?down(Reason)})
      end
  end,
  {noreply, St};
handle_info(Info, St) ->
  log_error("Unknown info ~p", [Info]),
  {noreply, St}.

handle_call(get_workers, _From, #{ets := Ets} = St) ->
  {reply, Ets, St};
handle_call(Call, From, St) ->
  log_error("Unknown call ~p from ~p", [Call, From]),
  {reply, {error, unknown_call}, St}.

handle_cast(Cast, St) ->
  log_error("Unknown cast ~p", [Cast]),
  {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
  {ok, St}.

terminate(_, _St) -> ok.

ensure_rediscover_client_timer(#{?rediscover_client_tref := false} = St) ->
  Tref = erlang:send_after(?rediscover_client_delay, self(), ?rediscover_client),
  St#{?rediscover_client_tref := Tref}.

log_error(Fmt, Args) -> error_logger:error_msg(Fmt, Args).

log_warning(Fmt, Args) -> error_logger:warning_msg(Fmt, Args).

log_info(Fmt, Args) -> error_logger:info_msg(Fmt, Args).

start_link_producers(ClientId, Topic, Connections, Config) ->
  lists:foldl(
    fun({Partition, MaybeConnPid}, Acc) ->
        {ok, WorkerPid} =
          wolff_producer:start_link(ClientId, Topic, Partition,
                                    MaybeConnPid, Config),
        Acc#{Partition => WorkerPid}
    end, #{}, Connections).

maybe_init_producers(#{ets := ?not_initialized,
                       topic := Topic,
                       client_id := ClientId,
                       config := Config
                      } = St) ->
  case start_linked_producers(ClientId, Topic, Config) of
    {ok, #{workers := Workers}} ->
      Ets = ets:new(get_name(Config), [protected, named_table, {read_concurrency, true}]),
      true = ets:insert(Ets, maps:to_list(Workers)),
      St#{ets := Ets};
    {error, Reason} ->
      log_error("Failed to init producers for topic ~s, reason: ~p",
                [Topic, Reason]),
      erlang:send_after(?init_producers_delay, self(), ?init_producers),
      St
  end;
maybe_init_producers(St) ->
  St.

maybe_restart_producers(#{ets := ?not_initialized} = St) -> St;
maybe_restart_producers(#{ets := Ets,
                          client_id := ClientId,
                          topic := Topic,
                          config := Config
                         } = St) ->
  lists:foreach(
    fun({Partition, Pid}) ->
        case is_alive(Pid) of
          true -> ok;
          false -> start_producer_and_insert_pid(Ets, ClientId, Topic, Partition, Config)
        end
    end, ets:tab2list(Ets)),
  St.

get_name(Config) -> maps:get(name, Config, ?MODULE).

start_producer_and_insert_pid(Ets, ClientId, Topic, Partition, Config) ->
  {ok, Pid} = wolff_producer:start_link(ClientId, Topic, Partition,
                                        ?conn_down(to_be_discovered), Config),
  ets:insert(Ets, {Partition, Pid}),
  ok.

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
refresh_partition_count(#{ets := ?not_initialized} = St) ->
  %% to be initialized
  St;
refresh_partition_count(#{client_pid := Pid, topic := Topic} = St) ->
  case wolff_client:get_leader_connections(Pid, Topic) of
    {ok, Connections} ->
      start_new_producers(St, Connections);
    {error, Reason} ->
      log_warning("failed_to_refresh_partition_count topic:~s, reason; ~p", [Topic, Reason]),
      St
  end.

start_new_producers(#{client_id := ClientId,
                      topic := Topic,
                      config := Config,
                      ets := Ets
                     } = St, Connections0) ->
  NowCount = length(Connections0),
  %% process only the newly discovered connections
  F = fun({Partition, _MaybeConnPid}) -> [] =:= ets:lookup(Ets, Partition) end,
  Connections = lists:filter(F, Connections0),
  Workers = start_link_producers(ClientId, Topic, Connections, Config),
  true = ets:insert(Ets, maps:to_list(Workers)),
  OldCount = partition_cnt(ClientId, Topic),
  case OldCount < NowCount of
    true ->
      log_info("started_producers_for_newly_discovered_partitions ~p", [Workers]),
      ok = put_partition_cnt(ClientId, Topic, NowCount);
    false ->
      ok
  end,
  St.

partition_cnt(ClientId, Topic) ->
  persistent_term:get({?MODULE, ClientId, Topic}).

put_partition_cnt(ClientId, Topic, Count) ->
  persistent_term:put({?MODULE, ClientId, Topic}, Count).

ensure_timer_cancelled(Tref) when is_reference(Tref) ->
    _ = erlang:cancel_timer(Tref),
    ok;
ensure_timer_cancelled(_) ->
    ok.
