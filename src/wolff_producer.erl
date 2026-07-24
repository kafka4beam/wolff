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

-module(wolff_producer).

-include_lib("kafka_protocol/include/kpro.hrl").
-include("wolff.hrl").

-define(MIN_DISCARD_LOG_INTERVAL, 5000).

%% APIs
-export([start_link/5, stop/1, notify_stop/2, send/3, send/4, send_sync/3]).

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_continue/2, handle_info/2, init/1, terminate/2]).

%% replayq callbacks
-export([queue_item_sizer/1, queue_item_marshaller/1]).

%% for test
-export([batch_bytes/1, varint_bytes/1]).

-export_type([config_in/0, config_key/0]).

-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type offset_reply() :: wolff:offset_reply().
-type config_key() :: group |
                      replayq_dir |
                      replayq_max_total_bytes |
                      replayq_seg_bytes |
                      replayq_offload_mode |
                      required_acks |
                      ack_timeout |
                      max_batch_bytes |
                      max_linger_ms |
                      max_linger_bytes |
                      max_send_ahead |
                      compression |
                      drop_if_highmem |
                      max_batch_age |
                      max_retry |
                      reconnect_delay_ms |
                      telemetry_meta_data |
                      max_partitions.

-type config_in() :: #{group => wolff_client:producer_group(),
                       replayq_dir => string() | binary(),
                       replayq_max_total_bytes => pos_integer(),
                       replayq_seg_bytes => pos_integer(),
                       replayq_offload_mode => boolean(),
                       required_acks => kpro:required_acks(),
                       ack_timeout => timeout(),
                       max_batch_bytes => pos_integer(),
                       max_linger_ms => non_neg_integer(),
                       max_linger_bytes => non_neg_integer(),
                       max_send_ahead => non_neg_integer(),
                       compression => kpro:compress_option(),
                       drop_if_highmem => boolean(),
                       max_batch_age => timeout(),
                       max_retry => infinity | non_neg_integer(),
                       reconnect_delay_ms => non_neg_integer(),
                       telemetry_meta_data => map(),
                       max_partitions => pos_integer()
                      }.

%% Some keys are removed from `config_in()'.
-type config_state() :: #{group => wolff_client:producer_group(),
                          replayq_max_total_bytes => pos_integer(),
                          replayq_offload_mode => boolean(),
                          required_acks => kpro:required_acks(),
                          ack_timeout => timeout(),
                          max_batch_bytes => pos_integer(),
                          max_linger_ms => non_neg_integer(),
                          max_linger_bytes => non_neg_integer(),
                          max_send_ahead => non_neg_integer(),
                          compression => kpro:compress_option(),
                          drop_if_highmem => boolean(),
                          max_batch_age => timeout(),
                          max_retry => infinity | non_neg_integer(),
                          reconnect_delay_ms => non_neg_integer(),
                          telemetry_meta_data => map(),
                          max_partitions => pos_integer()
                         }.

-define(no_timer, no_timer).
-define(reconnect, reconnect).
-define(linger_expire, linger_expire).
-define(linger_expire_timer, linger_expire_timer).
-define(pop_linger_expire, pop_linger_expire).
-define(pop_linger_timer, pop_linger_timer).
-define(DEFAULT_REPLAYQ_SEG_BYTES, 10 * 1024 * 1024).
-define(DEFAULT_REPLAYQ_LIMIT, 2000000000).
-define(Q_ITEM(CallId, Ts, Batch), {CallId, Ts, Batch}).
-define(SEND_REQ(From, Batch, AckFun), {send, From, Batch, AckFun}).
-define(queued, queued).
-define(no_queued_reply, no_queued_reply).
-define(ACK_CB(AckCb, Partition), {AckCb, Partition}).
-define(no_queue_ack, no_queue_ack).
-define(MAX_LINGER_BYTES, (10 bsl 20)).
-define(EMPTY, empty).
-define(SYNC_REF(Caller, Ref), {Caller, Ref}).
-define(IS_SYNC_REF(Caller, Ref), ((is_reference(Caller) orelse is_pid(Caller)) andalso is_reference(Ref))).
-define(DISCARD_OVERFLOW, "buffer_size_limit").
-define(DISCARD_HIGH_MEM, "high_system_RAM_usage").
-define(DISCARD_EXPIRED, "message_expired").

-type ack_fun() :: wolff:ack_fun().
-type send_req() :: ?SEND_REQ({pid(), reference()}, [wolff:msg()], ack_fun()).
-type sent() :: #{req_ref := reference(),
                  q_items := [?Q_ITEM(_CallId, _Ts, _Batch)],
                  q_ack_ref := replayq:ack_ref(),
                  attempts := pos_integer()
                 }.
-type sync_refs() :: {pid() | reference(), reference()}.
-type calls() :: #{ts := pos_integer(),
                   bytes := pos_integer(),
                   batch_r := [send_req()]
                  }.
-type state() :: #{ client_id := wolff:client_id()
                  , config := config_state()
                  , conn := undefined | _
                  , ?linger_expire_timer := false | reference()
                  , partition := partition()
                  , pending_acks := wolff_pendack:acks()
                  , produce_api_vsn := undefined | _
                  , replayq := replayq:q()
                  , sent_reqs := queue:queue(sent())
                  , sent_reqs_count := non_neg_integer()
                  , inflight_calls := non_neg_integer()
                  , topic := topic()
                  , calls := ?EMPTY | calls()
                  , sender => pid()
                  , ?pop_linger_timer := false | reference()
                  }.

%% @doc Start a per-partition producer worker.
%% Configs:
%% * `replayq_dir': `replayq' config to queue messages on disk in order to survive restart.
%% * `replayq_seg_bytes': default 10MB, replayq segment size.
%% * `replayq_max_total_bytes': default 2GB, replayq max total size.
%% * `replayq_offload_mode': default false, replayq work in offload mode if set to true.
%% * `required_acks': `all_isr', `leader_only' or `none'.
%% * `ack_timeout': default 10_000, timeout leader wait for replicas before reply to producer
%% * `max_batch_bytes': Most number of bytes to collect into a produce request.
%%    NOTE: This is only a rough estimation of the batch size towards kafka, NOT the
%%    exact max allowed message size configured in kafka.
%% * `max_linger_ms': Number of milliseconds to wait for more calls to accumulate
%%    a larger batch. When the queue is writing to disk, the wait happens before
%%    enqueue (to also batch disk writes); otherwise the producer delays popping
%%    the queue for a new produce request until at least
%%    `min(max_linger_bytes, max_batch_bytes)' bytes are queued, or the linger
%%    time expires. Default: 0 (as in send immediately)
%% * `max_linger_bytes': Number of bytes to collect before sending it to Kafka.
%%    If set to 0, `max_batch_bytes' is taken for mem-only mode, otherwise it's 10 times
%%    `max_batch_bytes' (but never exceeds 10MB) to optimize disk write.
%% * `max_send_ahead': Number of batches to be sent ahead without receiving ack for
%%    the last request. Must be 0 if messages must be delivered in strict order.
%% * `compression': `no_compression', `snappy' or `gzip'.
%% * `drop_if_highmem': default false. When set to true, and the queue is in
%%    memory-only mode, the producer drops incoming calls instead of growing
%%    the queue once `load_ctl:is_high_mem/0' reports high memory pressure.
%%    To avoid starving the sender, the producer still keeps up to
%%    `(max_send_ahead + 1) * max_batch_bytes' bytes buffered before any drop
%%    kicks in.
%% * `max_batch_age': default `infinity'. Maximum age (in milliseconds) a batch
%%    may reach before it is dropped instead of being sent to Kafka. A batch is
%%    dropped only when ALL of its messages are older than this age (measured
%%    from when they were appended to the buffer). This applies both to batches
%%    still queued (dropped from the front before the next send) and to batches
%%    that were in-flight when a connection dropped (dropped instead of retried
%%    on reconnect). Each dropped message has its ack callback evaluated with
%%    reason `message_expired' and bumps the `dropped' and `dropped_expired'
%%    counters. Set to `infinity' to disable (never drop by age).
%% * `max_retry': default `infinity'. Maximum number of times a batch is retried
%%    after a Kafka error response (e.g. `not_leader_for_partition') before it is
%%    dropped. A batch is dropped once its attempt counter reaches
%%    `max_retry + 1' (i.e. the initial send plus `max_retry' retries have all
%%    failed). Each dropped message has its ack callback evaluated with reason
%%    `max_retry_exceeded' and bumps the `dropped' counter. `max_retry = 0' drops
%%    on the first error; `infinity' (the default) retries forever. Note this
%%    counts Kafka error responses only; resends triggered purely by connection
%%    loss are bounded by `max_batch_age', not by `max_retry'.
%% * `reconnect_delay_ms': default 2000. Base delay in milliseconds before a
%%    disconnected producer tries to reconnect to the partition leader. A random
%%    jitter of 1..1000 ms is added on top so that partition workers do not all
%%    reconnect at the same instant. The very first attempt right after a
%%    disconnect uses no delay (reconnect immediately); the delay applies to
%%    subsequent attempts.
-spec start_link(wolff:client_id(), topic(), partition(), pid() | ?conn_down(any()), config_in()) ->
  {ok, pid()} | {error, any()}.
start_link(ClientId, Topic, Partition, MaybeConnPid, Config) ->
  St = #{client_id => ClientId,
         topic => Topic,
         partition => Partition,
         conn => MaybeConnPid,
         config => use_defaults(Config),
         ?linger_expire_timer => false,
         ?pop_linger_timer => false
        },
  %% the garbage collection can be expensive if using the default 'on_heap' option.
  SpawnOpts = [{spawn_opt, [{message_queue_data, off_heap}]}],
  gen_server:start_link(?MODULE, St, SpawnOpts).

stop(Pid) ->
  gen_server:stop(Pid).

notify_stop(Pid, Reason) ->
  gen_server:cast(Pid, {stop, Reason}).

%% @equiv send(Pid, Batch, AckFun, wait_for_queued)
-spec send(pid(), [wolff:msg()], wolff:ack_fun()) -> ok.
send(Pid, Batch, AckFun) ->
  send(Pid, Batch, AckFun, wait_for_queued).

%% @doc Send a batch asynchronously.
%% The callback function is evaluated by producer process when ack is received from kafka.
%% In case `required_acks' is configured to `none', the callback is evaluated immediately after send.
%% NOTE: This API has no backpressure,
%%       high produce rate may cause excessive ram and disk usage.
%%       Even less backpressure when the 4th arg is `no_linger'.
%% NOTE: It's possible that two or more batches get included into one produce request.
%%       But a batch is never split into produce requests.
%%       Make sure it will not exceed the `max_batch_bytes' limit when sending a batch.
-spec send(pid(), [wolff:msg()], wolff:ack_fun() | sync_refs(), WaitForQueued::wait_for_queued | no_wait_for_queued) -> ok.
send(Pid, [_ | _] = Batch0, AckFun, wait_for_queued) ->
  Caller = self(),
  Mref = erlang:monitor(process, Pid),
  Batch = ensure_ts(Batch0),
  erlang:send(Pid, ?SEND_REQ({Caller, Mref}, Batch, AckFun)),
  receive
    {Mref, ?queued} ->
      erlang:demonitor(Mref, [flush]),
      ok;
    {'DOWN', Mref, _, _, Reason} ->
      erlang:error({producer_down, Reason})
  end;
send(Pid, [_ | _] = Batch0, AckFun, no_wait_for_queued) ->
  Batch = ensure_ts(Batch0),
  erlang:send(Pid, ?SEND_REQ(?no_queued_reply, Batch, AckFun)),
  ok.

%% @doc Send a batch synchronously.
%% Raise error exception in case produce pid is down or when timed out.
-spec send_sync(pid(), [wolff:msg()], timeout()) -> {partition(), offset_reply()}.
send_sync(Pid, Batch0, Timeout) ->
  Caller = caller(),
  Mref = erlang:monitor(process, Pid),
  Ack = ?SYNC_REF(Caller, Mref),
  ok = send(Pid, Batch0, Ack, no_wait_for_queued),
  receive
    {Mref, Partition, BaseOffset} ->
      %% sent from eval_ack_cb
      erlang:demonitor(Mref, [flush]),
      {Partition, BaseOffset};
    {'DOWN', Mref, _, _, Reason} ->
      erlang:error({producer_down, Reason})
  after
    Timeout ->
      deref_caller(Caller),
      erlang:demonitor(Mref, [flush]),
      receive
        {Mref, Partition, BaseOffset} ->
          {Partition, BaseOffset}
      after
        0 ->
          erlang:error(timeout)
      end
  end.

init(#{client_id := ClientId, topic := Topic, partition := Partition} = St) ->
  erlang:process_flag(trap_exit, true),
  ok = set_process_label(ClientId, Topic, Partition),
  {ok, Sender} = wolff_sender:start_link(self(), ClientId, Topic, Partition),
  {ok, St#{sender => Sender}, {continue, do_init}}.

do_init(#{client_id := ClientId,
          topic := Topic,
          partition := Partition,
          config := Config0
         } = St) ->
  PathSegment0 =
    case maps:find(group, Config0) of
      {ok, Group} when is_binary(Group) -> <<Group/binary, $_, Topic/binary>>;
      _ -> <<ClientId/binary, $_, Topic/binary>>
    end,
  PathSegment = escape(PathSegment0),
  QCfg = case maps:get(replayq_dir, Config0, false) of
           false ->
             #{mem_only => true};
           BaseDir ->
             Dir = filename:join([BaseDir, PathSegment, integer_to_list(Partition)]),
             SegBytes = maps:get(replayq_seg_bytes, Config0, ?DEFAULT_REPLAYQ_SEG_BYTES),
             Offload = maps:get(replayq_offload_mode, Config0, false),
             #{dir => Dir, seg_bytes => SegBytes, offload => Offload}
         end,
  MaxTotalBytes = maps:get(replayq_max_total_bytes, Config0, ?DEFAULT_REPLAYQ_LIMIT),
  Q = replayq:open(QCfg#{sizer => fun ?MODULE:queue_item_sizer/1,
                         marshaller => fun ?MODULE:queue_item_marshaller/1,
                         max_total_bytes => MaxTotalBytes
                        }),
  Config1 = maps:update_with(
              telemetry_meta_data,
              fun(Meta) -> Meta#{partition_id => Partition} end,
              #{partition_id => Partition},
              Config0),
  Config2 = resolve_max_linger_bytes(Config1, Q),
  %% replayq configs are kept in Q, there is no need to duplicate them
  Config = maps:without([replayq_dir, replayq_seg_bytes], Config2),
  wolff_metrics:queuing_set(Config, replayq:count(Q)),
  wolff_metrics:queuing_bytes_set(Config, replayq:bytes(Q)),
  wolff_metrics:inflight_set(Config, 0),
  %% The initial connect attempt is made by the caller (wolff_producers.erl)
  %% If succeeded, 'Conn' is a pid (but it may as well be dead by now),
  %% if failed, it's a `term()' to indicate the failure reason.
  %% When it's not a pid, retry timer will be started in mark_connection_down
  %% like when 'DOWN' message is received.
  handle_leader_connection(
    St#{replayq => Q,
        config => Config,
        pending_acks => wolff_pendack:new(),
        produce_api_vsn => undefined,
        sent_reqs => queue:new(), % {kpro:req(), replayq:ack_ref(), [{CallId, MsgCount}]}
        sent_reqs_count => 0,
        inflight_calls => 0,
        client_id => ClientId,
        calls => ?EMPTY
       }).

handle_call(stop, From, St) ->
  gen_server:reply(From, ok),
  {stop, normal, St};
handle_call(_Call, _From, St) ->
  {noreply, St}.

handle_continue(do_init, St0) ->
  {noreply, do_init(St0)}.

handle_info(?linger_expire, St0) ->
  St1 = enqueue_calls(St0#{?linger_expire_timer => false}, no_linger),
  St = maybe_send_to_kafka(St1),
  {noreply, St};
handle_info({timeout, Ref, ?pop_linger_expire}, St0) ->
  case maps:get(?pop_linger_timer, St0, false) of
    Ref ->
      %% Lingered long enough, flush the queue even if the batch is under-sized
      St = maybe_send_to_kafka(St0#{?pop_linger_timer => false}, no_linger),
      {noreply, St};
    _ ->
      %% stale timer message: the timer fired right before it was cancelled
      {noreply, St0}
  end;
handle_info(?SEND_REQ(_, Batch, _) = Call, #{calls := Calls0, config := #{max_linger_bytes := Max}} = St0) ->
  Bytes = batch_bytes(Batch),
  Calls = collect_send_calls(Call, Bytes, Calls0, Max),
  St1 = enqueue_calls(St0#{calls => Calls}, maybe_linger),
  St = maybe_send_to_kafka(St1),
  {noreply, St};
handle_info({msg, Conn, Rsp}, #{conn := Conn} = St0) ->
  try handle_kafka_ack(Rsp, St0) of
    St1 ->
      St = maybe_send_to_kafka(St1),
      {noreply, St}
  catch
    throw : {kafka_error, Reason, St1} ->
      %% connection is not really down, but we need to
      %% stay down for a while and maybe restart sending on a new connection
      %% if Reason is not_leader_for_partition,
      %% wolff_client should expire old metadata while we are down
      %% and connect to the new leader when we request for a new connection
      St = mark_connection_down(St1, Reason),
      {noreply, St}
  end;
handle_info(?leader_connection(Conn), St) ->
  {noreply, handle_leader_connection(St#{conn := Conn})};
handle_info(?reconnect, St0) ->
  St = St0#{reconnect_timer => ?no_timer},
  {noreply, ensure_delayed_reconnect(St, normal_delay)};
handle_info({'DOWN', _, _, Conn, Reason}, #{conn := Conn} = St0) ->
  %% assert, connection can never down when pending on a reconnect
  #{reconnect_timer := ?no_timer} = St0,
  St = mark_connection_down(St0, Reason),
  {noreply, St};
handle_info({'EXIT', _, Reason}, St) ->
  %% linked process died
  {stop, Reason, St};
handle_info(_Info, St) ->
  {noreply, St}.

handle_cast({stop, Reason}, St) ->
  {stop, {shutdown, Reason}, St};
handle_cast(_Cast, St) ->
  {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
  {ok, St}.

terminate(Reason, #{replayq := Q} = St) ->
  case Reason of
    {shutdown, ?partition_lost} ->
      ok = replayq:close_and_purge(Q),
      _ = reply_error_for_all_reqs(St, ?partition_lost);
    _ ->
      ok = replayq:close(Q)
  end,
  ok = clear_gauges(St, Q).

%% Share the same logic for initial connection and reconnect
handle_leader_connection(#{topic := Topic, partition := Partition, conn := Conn} = St0) when is_pid(Conn) ->
  Attempts = maps:get(reconnect_attempts, St0, 0),
  Attempts > 0 andalso
    log_info(Topic, Partition, "partition_leader_reconnected", #{conn_pid => Conn}),
  _ = erlang:monitor(process, Conn),
  St1 = St0#{reconnect_timer => ?no_timer,
             reconnect_attempts => 0, %% reset counter
             conn := Conn},
  St2 = get_produce_version(St1),
  St3 = resend_sent_reqs(St2, ?reconnect),
  %% no_linger: the queue had the whole disconnected period to accumulate,
  %% flush it immediately (also in case the pop-linger expired while disconnected)
  maybe_send_to_kafka(St3, no_linger);
handle_leader_connection(#{conn := Reason} = St) ->
  mark_connection_down(St#{reconnect_timer => ?no_timer}, Reason).

clear_gauges(#{config := Config}, Q) ->
  wolff_metrics:inflight_set(Config, 0),
  maybe_reset_queuing(Config, Q),
  ok.

maybe_reset_queuing(Config, Q) ->
  case {replayq:count(Q), is_replayq_durable(Config, Q)} of
    {0, _} ->
      wolff_metrics:queuing_set(Config, 0),
      wolff_metrics:queuing_bytes_set(Config, 0);
    {_, false} ->
      wolff_metrics:queuing_set(Config, 0),
      wolff_metrics:queuing_bytes_set(Config, 0);
    {_, _} ->
      ok
  end.

ensure_ts(Batch) ->
  lists:map(fun(#{ts := _} = Msg) -> Msg;
               (Msg) -> Msg#{ts => now_ts()}
            end, Batch).

resolve_max_linger_bytes(#{max_linger_bytes := 0,
                           max_batch_bytes := Mb
                          } = Config, Q) ->
  case replayq:is_mem_only(Q) of
    true ->
      Config#{max_linger_bytes => Mb};
    false ->
      %% when disk or offload mode, try to linger with more bytes
      %% to optimize disk write performance
      Config#{max_linger_bytes => min(?MAX_LINGER_BYTES, Mb * 10)}
  end;
resolve_max_linger_bytes(Config, _Q) ->
  Config.

use_defaults(Config) ->
  use_defaults(Config, [{required_acks, all_isr},
                        {ack_timeout, 10000},
                        {max_batch_bytes, ?WOLFF_KAFKA_DEFAULT_MAX_MESSAGE_BYTES},
                        {max_linger_ms, 0},
                        {max_linger_bytes, 0},
                        {max_send_ahead, 0},
                        {compression, no_compression},
                        {max_batch_age, infinity},
                        {max_retry, infinity},
                        {reconnect_delay_ms, 2000}
                       ]).

use_defaults(Config, []) -> Config;
use_defaults(Config, [{K, V} | Rest]) ->
  case maps:is_key(K, Config) of
    true -> use_defaults(Config, Rest);
    false -> use_defaults(Config#{K => V}, Rest)
  end.

resend_requests(#{q_items := Items, req_ref := {alias, OldRef}} = Sent, St, ?reconnect) ->
  erlang:unalias(OldRef),
  {ok, Ref} = send_request(Items, St),
  [Sent#{req_ref := Ref}];
resend_requests(#{q_items := Items, req_ref := {alias, OldRef}} = Sent, St, ?message_too_large) ->
  erlang:unalias(OldRef),
  Refs = lists:map(fun(Item) -> {ok, Ref} = send_request([Item], St), Ref end, Items),
  #{attempts := Attempts, q_ack_ref := Q_AckRef} = Sent,
  make_sent_items_list(Items, Refs, Attempts, Q_AckRef).

%% only ack replayq when the last message is accepted by Kafka
make_sent_items_list([LastItem], [Ref], Attempts, Q_AckRef) ->
  [#{req_ref => Ref,
     q_items => [LastItem],
     q_ack_ref => Q_AckRef,
     attempts => Attempts
    }];
make_sent_items_list([Item | Items], [Ref | Refs], Attempts, Q_AckRef) ->
  [#{req_ref => Ref,
     q_items => [Item],
     q_ack_ref => ?no_queue_ack,
     attempts => Attempts
    } | make_sent_items_list(Items, Refs, Attempts, Q_AckRef)].

send_request(QueueItems,
             #{config := #{required_acks := RequiredAcks,
                           ack_timeout := AckTimeout,
                           compression := Compression
                          },
               produce_api_vsn := Vsn,
               topic := Topic,
               partition := Partition,
               conn := Conn,
               sender := Sender
              }) ->
  Batch = get_batch_from_queue_items(QueueItems),
  Opts = #{ack_timeout => AckTimeout,
           required_acks => RequiredAcks,
           compression => Compression
           },
  Ref = {alias, erlang:alias([reply])},
  ok = wolff_sender:do(Sender, Conn, Ref, Vsn, Topic, Partition, Batch, Opts),
  {ok, Ref}.

resend_sent_reqs(#{sent_reqs := SentReqs,
                   config := Config
                  } = St0, Reason) ->
  MaxBatchAge = maps:get(max_batch_age, Config, infinity),
  Now = now_ts(),
  F = fun(Sent, {Acc, StIn}) ->
          case is_batch_expired(Sent, MaxBatchAge, Now) of
            true ->
              %% All messages in this batch are older than max_batch_age;
              %% drop it instead of retrying.
              StOut = drop_expired_sent_req(Sent, StIn),
              {Acc, StOut};
            false ->
              wolff_metrics:retried_inc(Config, 1),
              NewSentList = resend_requests(Sent, StIn, Reason),
              {lists:reverse(NewSentList, Acc), StIn}
          end
      end,
  {NewSentReqs, St1} = lists:foldl(F, {[], St0}, queue:to_list(SentReqs)),
  St1#{sent_reqs := queue:from_list(lists:reverse(NewSentReqs))}.

%% A batch is expired only when ALL of its messages are older than
%% `max_batch_age'. Queue items are appended in timestamp order, so the last
%% item carries the newest timestamp; if even that is older than the age
%% threshold, the whole batch is expired.
is_batch_expired(_Sent, infinity, _Now) ->
  false;
is_batch_expired(#{q_items := Items}, MaxBatchAge, Now) ->
  ?Q_ITEM(_CallId, Ts, _Batch) = lists:last(Items),
  Now - Ts >= MaxBatchAge.

%% Drop an expired sent request: evaluate its ack callbacks with the
%% `message_expired' reason, release the replayq segment, adjust bookkeeping
%% and bump the `dropped' / `dropped_expired' counters. Modeled on
%% `clear_sent_and_ack_callers/3' and `handle_overflow/3'.
drop_expired_sent_req(#{q_items := Items,
                        q_ack_ref := Q_AckRef,
                        req_ref := ReqRef},
                      #{sent_reqs_count := SentReqsCount,
                        inflight_calls := InflightCalls,
                        pending_acks := PendingAcks,
                        replayq := Q,
                        config := Config
                       } = St) ->
  %% The reply alias is useless now; the ack for this request will never arrive.
  _ = unalias_req_ref(ReqRef),
  Calls = get_calls_from_queue_items(Items),
  NrOfCalls = count_calls(Items),
  NewPendingAcks =
    lists:foldl(
      fun({CallId, _BatchSize}, Acc) ->
          case wolff_pendack:take_inflight(Acc, CallId) of
            {ok, AckCb, Acc1} ->
              ok = eval_ack_cb(AckCb, ?message_expired),
              Acc1;
            false ->
              Acc
          end
      end, PendingAcks, Calls),
  ok = replayq_ack(Q, Q_AckRef),
  NewInflightCalls = InflightCalls - NrOfCalls,
  wolff_metrics:dropped_inc(Config, NrOfCalls),
  wolff_metrics:dropped_expired_inc(Config, NrOfCalls),
  wolff_metrics:inflight_set(Config, NewInflightCalls),
  ok = maybe_log_discard(St, NrOfCalls, ?DISCARD_EXPIRED),
  St#{sent_reqs_count := SentReqsCount - 1,
      inflight_calls := NewInflightCalls,
      pending_acks := NewPendingAcks}.

unalias_req_ref({alias, Ref}) ->
  _ = erlang:unalias(Ref),
  ok;
unalias_req_ref(_) ->
  ok.

%% Drop expired batches from the front (oldest end) of the queue before trying
%% to send. Queue items are appended in timestamp order, so once the front is
%% not expired, nothing behind it is either. This runs regardless of connection
%% or inflight-window state, so stale data is shed even while disconnected.
drop_expired_head(#{config := Config} = St) ->
  case maps:get(max_batch_age, Config, infinity) of
    infinity -> St;
    MaxBatchAge -> drop_expired_head(St, MaxBatchAge, now_ts())
  end.

drop_expired_head(#{replayq := Q} = St, MaxBatchAge, Now) ->
  case replayq:peek(Q) of
    ?Q_ITEM(_CallId, Ts, _Batch) when Now - Ts >= MaxBatchAge ->
      drop_expired_head(discard_expired_front(St), MaxBatchAge, Now);
    _ ->
      %% empty queue, or the front item is not expired yet
      St
  end.

%% Pop and drop the single front (oldest) queue item, which never made it to
%% Kafka; its callers are still in the pending-ack backlog. Mirrors
%% `handle_overflow/3'.
discard_expired_front(#{replayq := Q,
                        pending_acks := PendingAcks,
                        config := Config
                       } = St) ->
  {NewQ, AckRef, Items} = replayq:pop(Q, #{count_limit => 1}),
  ok = replayq:ack(NewQ, AckRef),
  Calls = get_calls_from_queue_items(Items),
  CallIDs = lists:map(fun({CallId, _BatchSize}) -> CallId end, Calls),
  NrOfCalls = count_calls(Items),
  wolff_metrics:dropped_inc(Config, NrOfCalls),
  wolff_metrics:dropped_expired_inc(Config, NrOfCalls),
  wolff_metrics:queuing_set(Config, replayq:count(NewQ)),
  wolff_metrics:queuing_bytes_set(Config, replayq:bytes(NewQ)),
  {CbList, NewPendingAcks} = wolff_pendack:drop_backlog(PendingAcks, CallIDs),
  lists:foreach(fun(Cb) -> eval_ack_cb(Cb, ?message_expired) end, CbList),
  ok = maybe_log_discard(St, NrOfCalls, ?DISCARD_EXPIRED),
  St#{replayq := NewQ, pending_acks := NewPendingAcks}.

maybe_send_to_kafka(St) ->
  maybe_send_to_kafka(St, maybe_linger).

%% Linger =:= no_linger when the pop-linger has expired or after a reconnect:
%% flush the queue without waiting for more calls to accumulate.
maybe_send_to_kafka(St0, Linger) ->
  %% shed expired batches first, so the pop-linger gate below
  %% sees the queue size without them
  #{conn := Conn, replayq := Q} = St = drop_expired_head(St0),
  case replayq:count(Q) =:= 0 of
    true ->
      %% nothing to send
      St;
    false when is_pid(Conn) ->
      %% has connection, try send
      maybe_send_to_kafka_has_pending(St, Linger);
    false ->
      %% no connection
      %% maybe the connection was closed after ideling
      %% re-connect is triggered immediately if this
      %% is the first attempt
      ensure_delayed_reconnect(St, no_delay_for_first_attempt)
  end.

maybe_send_to_kafka_has_pending(St, Linger) ->
  case is_send_ahead_allowed(St) of
    true ->
      case should_linger_before_pop(St, Linger) of
        true -> ensure_pop_linger_timer(St);
        false -> send_to_kafka(ensure_pop_linger_cancel(St))
      end;
    false -> St %% reached max inflight limit
  end.

send_to_kafka(#{sent_reqs := SentReqs,
                sent_reqs_count := SentReqsCount,
                inflight_calls := InflightCalls,
                replayq := Q,
                config := #{required_acks := RequiredAcks,
                            max_batch_bytes := BytesLimit} = Config,
                pending_acks := PendingAcks
               } = St0) ->
  {NewQ, QAckRef, Items} =
    replayq:pop(Q, #{bytes_limit => BytesLimit, count_limit => 999999999}),
  IDs = lists:map(fun({ID, _}) -> ID end, get_calls_from_queue_items(Items)),
  NewPendingAcks = wolff_pendack:move_backlog_to_inflight(PendingAcks, IDs),
  wolff_metrics:queuing_set(Config, replayq:count(NewQ)),
  wolff_metrics:queuing_bytes_set(Config, replayq:bytes(NewQ)),
  NewSentReqsCount = SentReqsCount + 1,
  NrOfCalls = count_calls(Items),
  NewInflightCalls = InflightCalls + NrOfCalls,
  _ = wolff_metrics:inflight_set(Config, NewInflightCalls),
  NoAck = (RequiredAcks =:= none),
  {ok, Ref} = send_request(Items, St0),
  Sent = #{req_ref => Ref,
           q_items => Items,
           q_ack_ref => QAckRef,
           attempts => 1
          },
  St1 = St0#{replayq := NewQ,
             sent_reqs := queue:in(Sent, SentReqs),
             sent_reqs_count := NewSentReqsCount,
             inflight_calls := NewInflightCalls,
             pending_acks := NewPendingAcks
            },
  St2 = maybe_fake_kafka_ack(NoAck, St1),
  maybe_send_to_kafka(St2).

%% when require no acks do not add to sent_reqs and ack caller immediately
maybe_fake_kafka_ack(_NoAck = true, St) ->
  do_handle_kafka_ack(?no_error, ?UNKNOWN_OFFSET, St);
maybe_fake_kafka_ack(_NoAck, St) -> St.

is_send_ahead_allowed(#{config := #{max_send_ahead := Max},
                        sent_reqs_count := SentCount}) ->
  SentCount =< Max.

%% nothing sent and nothing pending to be sent
is_idle(#{replayq := Q, sent_reqs_count := SentReqsCount}) ->
  SentReqsCount =:= 0 andalso replayq:count(Q) =:= 0.

now_ts() ->
    erlang:system_time(millisecond).

make_queue_item(CallId, Batch) ->
  ?Q_ITEM(CallId, now_ts(), Batch).

queue_item_sizer(?Q_ITEM(_CallId, _Ts, Batch)) ->
  batch_bytes(Batch).

batch_bytes(Batch) ->
  lists:foldl(fun(M, Sum) -> oct(M) + Sum end, 0, Batch).

queue_item_marshaller(?Q_ITEM(_, _, _) = I) ->
  term_to_binary(I);
queue_item_marshaller(Bin) when is_binary(Bin) ->
  binary_to_term(Bin).

get_produce_version(#{conn := Conn} = St) when is_pid(Conn) ->
  Vsn = case kpro:get_api_vsn_range(Conn, produce) of
          {ok, {_Min, Max}} -> Max;
          {error, _} -> 3 %% minimum magic v2 produce API version
        end,
  St#{produce_api_vsn => Vsn}.

get_batch_from_queue_items(Items) ->
  get_batch_from_queue_items(Items, []).

get_batch_from_queue_items([], Acc) ->
  lists:reverse(Acc);
get_batch_from_queue_items([?Q_ITEM(_CallId, _Ts, Batch) | Items], Acc) ->
  get_batch_from_queue_items(Items, lists:reverse(Batch, Acc)).

count_calls(Items) -> length(Items).

count_msgs([]) ->
  0;
count_msgs([?Q_ITEM(_CallId, _Ts, Batch) | Rest]) ->
  length(Batch) + count_msgs(Rest).

get_calls_from_queue_items(Items) ->
  get_calls_from_queue_items(Items, []).

get_calls_from_queue_items([], Calls) ->
  lists:reverse(Calls);
get_calls_from_queue_items([QItem | Rest], Calls) ->
  ?Q_ITEM(CallId, _Ts, Batch) = QItem,
  get_calls_from_queue_items(Rest, [{CallId, length(Batch)} | Calls]).

-spec handle_kafka_ack(_, state()) -> state().
handle_kafka_ack(#kpro_rsp{api = produce,
                           ref = Ref,
                           msg = Rsp
                          },
                 #{sent_reqs := SentReqs} = St) ->
  [TopicRsp] = kpro:find(responses, Rsp),
  [PartitionRsp] = kpro:find(partition_responses, TopicRsp),
  ErrorCode = kpro:find(error_code, PartitionRsp),
  BaseOffset = kpro:find(base_offset, PartitionRsp),
  case queue:peek(SentReqs) of
    {value, #{req_ref := Ref1}} when Ref1 =:= Ref ->
      %% sent_reqs queue front matched the response reference
      do_handle_kafka_ack(ErrorCode, BaseOffset, St);
    _ ->
      %% stale response e.g. when inflight > 1, but we have to retry an older req
      St
  end.

note(Fmt, Args) ->
  lists:flatten(io_lib:format(Fmt, Args)).

-spec do_handle_kafka_ack(_, _, state()) -> state().
do_handle_kafka_ack(?no_error, BaseOffset, St) ->
  clear_sent_and_ack_callers(?no_error, BaseOffset, St);
do_handle_kafka_ack(EC, _BaseOffset, St) when EC =:= ?message_too_large orelse EC =:= ?record_list_too_large ->
  #{topic := Topic, partition := Partition, config := Config, sent_reqs := SentReqs} = St,
  {value, #{q_items := Items}} = queue:peek(SentReqs),
  Batch = get_batch_from_queue_items(Items),
  Bytes = batch_bytes(Batch),
  Hint = case EC of
    ?message_too_large ->
      "Consider increasing server side topic config 'max.message.bytes'!";
    ?record_list_too_large ->
      "Cnosider increasing server side topic config 'segment.bytes'!"
  end,
  %% Reason to drop request or split batch is message_too_large
  %% because record_list_too_large makes little sense for application.
  Reason = ?message_too_large,
  case length(Items) of
    1 ->
      %% This is a single message batch, but it's still too large
      Note = "A single-request batch is dropped because it's too large for this topic! " ++ Hint,
      log_error(Topic, Partition, EC, #{note => Note, estimated_batch_bytes => Bytes}),
      wolff_metrics:dropped_inc(Config, 1),
      clear_sent_and_ack_callers(EC, Reason, St);
    N ->
      %% This is a batch of more than one queue items (calls)
      %% Split the batch, and re-send
      #{max_batch_bytes := Max} = Config,
      NewMax = max(1, (Max + 1) div 2),
      St1 = St#{config := Config#{max_batch_bytes := NewMax}},
      Note = note("Config max_batch_bytes=~p is too large for this topic, "
                  "trying to split the current batch and retry. "
                  "Will use max_batch_bytes=~p to collect future batches. " ++ Hint,
                  [Max, NewMax]),
      log_warn(Topic, Partition, EC, #{note => Note, calls_count => N, encode_bytes => Bytes}),
      %% We do not incement the attempt counter for too large batch
      %% St2 = increment_attempt_for_first_sent_req(St),
      resend_sent_reqs(St1, Reason)
  end;
do_handle_kafka_ack(ErrorCode, _BaseOffset, St) ->
  %% Other errors, such as not_leader_for_partition
  #{sent_reqs := SentReqs,
    topic := Topic,
    partition := Partition,
    config := Config
   } = St,
  {value, #{attempts := Attempts, q_items := Items}} = queue:peek(SentReqs),
  NrOfCalls = count_calls(Items),
  MaxRetry = maps:get(max_retry, Config, infinity),
  case is_max_retry_reached(Attempts, MaxRetry) of
    true ->
      %% Give up: drop the front batch instead of retrying it again.
      log_error(Topic, Partition, "dropped_produce_request_reached_max_retry",
                #{error_code => ErrorCode,
                  batch_size => count_msgs(Items),
                  attempts => Attempts,
                  max_retry => MaxRetry}),
      wolff_metrics:dropped_inc(Config, NrOfCalls),
      %% clear_sent_and_ack_callers/3 pops the front request, bumps the
      %% failed/retried_failed counter, and acks the callers with the reason.
      St1 = clear_sent_and_ack_callers(ErrorCode, ?max_retry_exceeded, St),
      %% Still reconnect: the error (e.g. not_leader_for_partition) means the
      %% current connection is stale for the remaining in-flight requests.
      erlang:throw({kafka_error, ErrorCode, St1});
    false ->
      inc_sent_failed(Config, NrOfCalls, Attempts),
      log_warn(Topic, Partition, "error_in_produce_response",
               #{error_code => ErrorCode,
                 batch_size => count_msgs(Items),
                 attempts => Attempts}),
      St1 = increment_attempt(St),
      erlang:throw({kafka_error, ErrorCode, St1})
  end.

%% A batch is dropped once it has been attempted `max_retry + 1' times, i.e. the
%% initial send plus `max_retry' retries have all failed.
is_max_retry_reached(_Attempts, infinity) ->
  false;
is_max_retry_reached(Attempts, MaxRetry) ->
  Attempts >= MaxRetry + 1.

%% Since we only handle Kafka ack for the first sent request in the sent queue
%% we only bump the first itme with attempt counter
increment_attempt(#{sent_reqs := SentReqs} = St) ->
  {{value, SentReq}, SentReqs1} = queue:out(SentReqs),
  #{attempts := Attempts} = SentReq,
  St#{sent_reqs := queue:in_r(SentReq#{attempts := Attempts + 1}, SentReqs1)}.

clear_sent_and_ack_callers(ErrorCode, BaseOffset, St) ->
  #{sent_reqs := SentReqs,
    sent_reqs_count := SentReqsCount,
    inflight_calls := InflightCalls,
    pending_acks := PendingAcks,
    replayq := Q,
    config := Config
  } = St,
  {{value, #{q_ack_ref := Q_AckRef,
             q_items := Items,
             attempts := Attempts}}, NewSentReqs} = queue:out(SentReqs),
  Calls = get_calls_from_queue_items(Items),
  NrOfCalls = count_calls(Items),
  case ErrorCode of
    ?no_error ->
      inc_sent_success(Config, NrOfCalls, Attempts);
    _ ->
      inc_sent_failed(Config, NrOfCalls, Attempts)
  end,
  NewSentReqsCount = SentReqsCount - 1,
  NewInflightCalls = InflightCalls - NrOfCalls,
  wolff_metrics:inflight_set(Config, NewInflightCalls),
  ok = replayq_ack(Q, Q_AckRef),
  NewPendingAcks = evaluate_pending_ack_funs(PendingAcks, Calls, BaseOffset),
  St#{sent_reqs := NewSentReqs,
      sent_reqs_count := NewSentReqsCount,
      inflight_calls := NewInflightCalls,
      pending_acks := NewPendingAcks
     }.

replayq_ack(_Q, ?no_queue_ack) ->
  ok;
replayq_ack(Q, Q_AckRef) ->
  replayq:ack(Q, Q_AckRef).

%% @private This function is called in below scenarios
%% * Failed to connect any of the brokers
%% * Failed to connect to partition leader
%% * Connection 'DOWN' due to Kafka initiated socket close
%% * Produce request failure
%%   - Socket error
%%   - Error code received from Kafka
mark_connection_down(#{topic := Topic,
                       partition := Partition,
                       conn := Old
                      } = St0, Reason) ->
  false = is_pid(Reason),
  St = St0#{conn := Reason},
  ok = log_connection_down(Topic, Partition, Old, Reason),
  case is_idle(St) of
    true ->
      St;
    false ->
      %% ensure delayed reconnect timer is started
      ensure_delayed_reconnect(St, normal_delay)
  end.

log_connection_down(_Topic, _Partition, _, to_be_discovered) ->
  %% this is the initial state of the connection
  ok;
log_connection_down(Topic, Partition, Conn, Reason) ->
  log_info(Topic, Partition,
           "connection_to_partition_leader_error",
           #{conn => Conn, reason => Reason}).

is_timer_on(?no_timer) ->
  false;
is_timer_on({T, Ref}) when is_integer(T), is_reference(Ref) ->
  %% started by timer:apply_after (OTP 24)
  erlang:monotonic_time(microsecond) < T;
is_timer_on({_, Ref}) when is_reference(Ref) ->
  %% started by timer:apply_after (OTP 25 or later)
  is_timer_on(Ref);
is_timer_on(Ref) when is_reference(Ref) ->
  erlang:read_timer(Ref) =/= false.

ensure_delayed_reconnect(St, DelayStrategy) ->
  Tref = maps:get(reconnect_timer, St, ?no_timer),
  case is_timer_on(Tref) of
    true ->
      St;
    false ->
      do_ensure_delayed_reconnect(St, DelayStrategy)
  end.

do_ensure_delayed_reconnect(
  #{config := #{reconnect_delay_ms := Delay0} = Config,
    client_id := ClientId,
    topic := Topic,
    partition := Partition
   } = St, DelayStrategy) ->
  Attempts = maps:get(reconnect_attempts, St, 0),
  MaxPartitions = maps:get(max_partitions, Config, all_partitions),
  Attempts > 0 andalso Attempts rem 10 =:= 0 andalso
    log_error(Topic, Partition,
              "producer_is_still_disconnected_after_retry",
              #{attempts => Attempts}),
  Delay =
    case DelayStrategy of
      no_delay_for_first_attempt when Attempts =:= 0 ->
        %% this is the first attempt after disconnected, no delay
        0;
      _ ->
        %% add an extra random delay to avoid all partition workers
        %% try to reconnect around the same moment
        Delay0 + rand:uniform(1000)
    end,
  case wolff_client_sup:find_client(ClientId) of
    {ok, ClientPid} ->
      Group = maps:get(group, Config, ?NO_GROUP),
      Args = [ClientPid, Group, Topic, Partition, self(), MaxPartitions],
      {ok, Tref} = timer:apply_after(Delay, wolff_client, recv_leader_connection, Args),
      St#{reconnect_timer => Tref, reconnect_attempts => Attempts + 1};
    {error, Reason} ->
      log_warn(Topic, Partition,
               "failed_to_find_client_will_retry_after_delay",
               #{delay => Delay, reason => Reason}),
      %% call timer:apply_after for both cases, do not use send_after here
      {ok, Tref} = timer:apply_after(Delay, erlang, send, [self(), ?reconnect]),
      St#{reconnect_timer => Tref, reconnect_attempts => Attempts + 1}
  end.

evaluate_pending_ack_funs(PendingAcks, [], _BaseOffset) -> PendingAcks;
evaluate_pending_ack_funs(PendingAcks, [{CallId, BatchSize} | Rest], BaseOffset) ->
  NewPendingAcks =
    case wolff_pendack:take_inflight(PendingAcks, CallId) of
      {ok, AckCb, PendingAcks1} ->
        ok = eval_ack_cb(AckCb, BaseOffset),
        PendingAcks1;
      false ->
        PendingAcks
    end,
  evaluate_pending_ack_funs(NewPendingAcks, Rest, offset(BaseOffset, BatchSize)).

offset(BaseOffset, Delta) when is_integer(BaseOffset) andalso BaseOffset >= 0 -> BaseOffset + Delta;
offset(BaseOffset, _Delta) -> BaseOffset.

log_info(Topic, Partition, Msg, Args) ->
  log(info, Args#{topic => Topic, partition => Partition, msg => Msg}).

log_warn(Topic, Partition, Msg, Args) ->
  log(warning, Args#{topic => Topic, partition => Partition, msg => Msg}).

log_error(Topic, Partition, Msg, Args) ->
  log(error, Args#{topic => Topic, partition => Partition, msg => Msg}).

log(Level, Report) ->
    logger:log(Level, Report).

%% Estimation of size in bytes of one payload sent to Kafka.
%% According to Kafka protocol, a v2 record consists of below fields:
%%   Length => varint              # varint_bytes(SizeOfAllRestFields)
%%   Attributes => int8            # 1 -- always zero (the byte value)
%%   TimestampDelta => varint      # 4 -- a wild guess
%%   OffsetDelta => varint         # 2 -- never exceeds 3 bytes
%%   KeyLen => varint              # varint_bytes(size(Key))
%%   Key => data                   # size(Key)
%%   ValueLen => varint            # varint_bytes(size(Value))
%%   Value => data                 # size(Value)
%%   Headers => [Header]           # headers_bytes(Headers)
oct(#{key := K, value := V} = Msg) ->
  HeadersBytes = headers_bytes(maps:get(headers, Msg, [])),
  FieldsBytes = HeadersBytes + 7 + encoded_bin_bytes(K) + encoded_bin_bytes(V),
  FieldsBytes + varint_bytes(HeadersBytes + FieldsBytes).

headers_bytes(Headers) ->
  headers_bytes(Headers, 0, 0).

headers_bytes([], Bytes, Count) ->
  Bytes + varint_bytes(Count);
headers_bytes([{Name, Value} | T], Bytes, Count) ->
  NewBytes = Bytes + encoded_bin_bytes(Name) + encoded_bin_bytes(Value),
  headers_bytes(T, NewBytes, Count+1).

-compile({inline, encoded_bin_bytes/1}).
encoded_bin_bytes(B) ->
  varint_bytes(size(B)) + size(B).

%% varint encoded bytes of a unsigned integer
-compile({inline, vb/1}).
vb(N) when N < 128 -> 1;
vb(N) when N < 16384 -> 2;
vb(N) when N < 2097152 -> 3;
vb(N) when N < 268435456 -> 4;
vb(N) when N < 34359738368 -> 5;
vb(N) when N < 4398046511104 -> 6;
vb(N) when N < 562949953421312 -> 7;
vb(N) when N < 72057594037927936 -> 8;
vb(N) when N < 9223372036854775808 -> 9;
vb(_) -> 10.

%% varint encoded bytes of a signed integer
-compile({inline, varint_bytes/1}).
varint_bytes(N) -> vb(zz(N)).

%% zigzag encode a signed integer
-compile({inline, zz/1}).
zz(I) -> (I bsl 1) bxor (I bsr 63).

%% collect send calls which are already sent to mailbox,
%% the collection is size-limited by the max_linger_bytes config.
collect_send_calls(Call, Bytes, ?EMPTY, Max) ->
  Init = #{ts => now_ts(), bytes => 0, batch_r => []},
  collect_send_calls(Call, Bytes, Init, Max);
collect_send_calls(Call, Bytes, Calls, Max) ->
  #{bytes := Bytes0, batch_r := BatchR} = Calls,
  Sum = Bytes0 + Bytes,
  R = Calls#{bytes => Sum,
             batch_r => [Call | BatchR]
            },
  case Sum < Max of
    true ->
      collect_send_calls2(R, Max);
    false ->
      R
  end.

%% Collect all send requests which are already in process mailbox
collect_send_calls2(Calls, Max) ->
  receive
    ?SEND_REQ(_, Batch, _) = Call ->
      Bytes = batch_bytes(Batch),
      collect_send_calls(Call, Bytes, Calls, Max)
  after
    0 ->
      Calls
  end.

ensure_linger_expire_timer_start(#{?linger_expire_timer := false} = St, Timeout) ->
  %% delay enqueue, try to accumulate more into the batch
  Ref = erlang:send_after(Timeout, self(), ?linger_expire),
  St#{?linger_expire_timer := Ref};
ensure_linger_expire_timer_start(St, _Timeout) ->
  %% timer is already started
  St.

ensure_linger_expire_timer_cancel(#{?linger_expire_timer := LTimer} = St) ->
  _ = is_reference(LTimer) andalso erlang:cancel_timer(LTimer),
  St#{?linger_expire_timer => false}.

%% check if the call collection should continue to linger before enqueue
is_linger_continue(#{config := #{max_linger_ms := 0}}) ->
  false;
is_linger_continue(#{calls := Calls, config := Config, replayq := Q}) ->
  case replayq:is_writing_to_disk(Q) of
    true ->
      #{max_linger_ms := MaxLingerMs, max_linger_bytes := MaxLingerBytes} = Config,
      #{ts := Ts, bytes := Bytes} = Calls,
      case Bytes < MaxLingerBytes of
        true ->
          TimeLeft = MaxLingerMs - (now_ts() - Ts),
          (TimeLeft > 0) andalso {true, TimeLeft};
        false ->
          false
      end;
    false ->
      false
  end.

%% Check if the producer should delay popping the queue so an under-sized
%% batch gets a chance to grow from future send calls.
%% Only when not writing to disk: when writing to disk, the linger is applied
%% before enqueue (see is_linger_continue/1) to also batch disk writes.
%% NOTE: the ?pop_linger_timer key may be absent when the new beam is
%% hot-loaded into an already running producer, hence maps:get/3 with
%% default in ensure_pop_linger_* below.
should_linger_before_pop(_St, no_linger) ->
  false;
should_linger_before_pop(#{config := #{max_linger_ms := 0}}, _) ->
  false;
should_linger_before_pop(#{replayq := Q, config := Config}, _) ->
  #{max_linger_bytes := MaxLingerBytes, max_batch_bytes := MaxBatchBytes} = Config,
  (not replayq:is_writing_to_disk(Q)) andalso
    replayq:bytes(Q) < min(MaxLingerBytes, MaxBatchBytes).

ensure_pop_linger_timer(St) ->
  case maps:get(?pop_linger_timer, St, false) of
    false ->
      #{config := #{max_linger_ms := Ms}} = St,
      Ref = erlang:start_timer(Ms, self(), ?pop_linger_expire),
      St#{?pop_linger_timer => Ref};
    _ ->
      %% timer is already started
      St
  end.

ensure_pop_linger_cancel(St) ->
  case maps:get(?pop_linger_timer, St, false) of
    false ->
      St;
    Ref ->
      %% no need to flush a stale ?pop_linger_expire message:
      %% it's ignored by the timer reference check in handle_info
      _ = erlang:cancel_timer(Ref),
      St#{?pop_linger_timer => false}
  end.

enqueue_calls(#{calls := ?EMPTY} = St, _) ->
  %% no call to enqueue
  St;
enqueue_calls(St, maybe_linger) ->
  case is_linger_continue(St) of
    {true, Timeout} ->
      ensure_linger_expire_timer_start(St, Timeout);
    false ->
      enqueue_calls(St, no_linger)
  end;
enqueue_calls(#{calls := #{batch_r := CallsR}} = St0, no_linger) ->
  Calls = lists:reverse(CallsR),
  St = ensure_linger_expire_timer_cancel(St0),
  enqueue_calls2(Calls, St#{calls => ?EMPTY}).

enqueue_calls2(Calls,
               #{replayq := Q,
                 pending_acks := PendingAcks0,
                 partition := Partition,
                 config := Config0
                } = St0) ->
  {QueueItems, PendingAcks, CallByteSize} =
    lists:foldl(
      fun(?SEND_REQ(_From, Batch, AckFun), {Items, PendingAcksIn, Size}) ->
          %% keep callback funs in memory, do not seralize it into queue because
          %% saving anonymous function to disk may easily lead to badfun exception
          %% in case of restart on newer version beam.
          {CallId, PendingAcksOut} = wolff_pendack:insert_backlog(PendingAcksIn, ?ACK_CB(AckFun, Partition)),
          NewItems = [make_queue_item(CallId, Batch) | Items],
          {NewItems, PendingAcksOut, Size + batch_bytes(Batch)}
      end, {[], PendingAcks0, 0}, Calls),
   NewQ = replayq:append(Q, lists:reverse(QueueItems)),
   wolff_metrics:queuing_set(Config0, replayq:count(NewQ)),
   wolff_metrics:queuing_bytes_set(Config0, replayq:bytes(NewQ)),
   lists:foreach(fun maybe_reply_queued/1, Calls),
   IsHighMemOverflow =
        maps:get(drop_if_highmem, Config0, false)
        andalso replayq:is_mem_only(NewQ)
        andalso load_ctl:is_high_mem(),
   Overflow = case IsHighMemOverflow of
                  true ->
                      high_mem_overflow(NewQ, CallByteSize, Config0);
                  false ->
                      replayq:overflow(NewQ)
              end,
   handle_overflow(St0#{replayq := NewQ,
                        pending_acks := PendingAcks
                       },
                   IsHighMemOverflow,
                   Overflow).

%% Keep at least `(max_send_ahead + 1) * max_batch_bytes' worth of data
%% buffered even under high memory pressure, so the sender has data to fill all
%% of its in-flight slots and does not run dry when the queue is small. Drop
%% at most the just-arrived call's bytes; with the default `max_send_ahead = 0'
%% the reserve is one `max_batch_bytes' batch.
high_mem_overflow(Q, CallByteSize,
                  #{max_send_ahead := MaxSendAhead,
                    max_batch_bytes := MaxBatchBytes}) ->
    Reserved = (MaxSendAhead + 1) * MaxBatchBytes,
    Excess = max(0, replayq:bytes(Q) - Reserved),
    max(replayq:overflow(Q), min(CallByteSize, Excess)).

maybe_reply_queued(?SEND_REQ(?no_queued_reply, _, _)) ->
    ok;
maybe_reply_queued(?SEND_REQ({Caller, Ref}, _, _)) ->
    erlang:send(Caller, {Ref, ?queued}).

eval_ack_cb(?ACK_CB(AckFun, Partition), BaseOffset) when is_function(AckFun, 2) ->
  ok = AckFun(Partition, BaseOffset);
eval_ack_cb(?ACK_CB({F, A}, Partition), BaseOffset) when is_function(F) ->
  true = is_function(F, length(A) + 2),
  ok = erlang:apply(F, [Partition, BaseOffset | A]);
eval_ack_cb(?ACK_CB({Caller, Ref}, Partition), BaseOffset) when ?IS_SYNC_REF(Caller, Ref) ->
  _ = erlang:send(Caller, {Ref, Partition, BaseOffset}),
  ok.

handle_overflow(St, _IsHighMemOverflow, Overflow) when Overflow =< 0 ->
    ok = maybe_log_discard(St, 0, ?DISCARD_OVERFLOW),
    St;
handle_overflow(#{replayq := Q,
                  pending_acks := PendingAcks,
                  config := Config
                 } = St,
                IsHighMemOverflow,
                Overflow) ->
  BytesMode =
    case IsHighMemOverflow of
      true -> at_least;
      false -> at_most
    end,
  OverflowReason =
    case IsHighMemOverflow of
      true ->
        ?DISCARD_HIGH_MEM;
      false ->
        ?DISCARD_OVERFLOW
    end,
  {NewQ, QAckRef, Items} =
    replayq:pop(Q, #{bytes_limit => {BytesMode, Overflow}, count_limit => 999999999}),
  ok = replayq:ack(NewQ, QAckRef),
  Calls = get_calls_from_queue_items(Items),
  CallIDs = lists:map(fun({CallId, _BatchSize}) -> CallId end, Calls),
  NrOfCalls = count_calls(Items),
  wolff_metrics:dropped_queue_full_inc(Config, NrOfCalls),
  wolff_metrics:dropped_inc(Config, NrOfCalls),
  wolff_metrics:queuing_set(Config, replayq:count(NewQ)),
  wolff_metrics:queuing_bytes_set(Config, replayq:bytes(NewQ)),
  ok = maybe_log_discard(St, NrOfCalls, OverflowReason),
  {CbList, NewPendingAcks} = wolff_pendack:drop_backlog(PendingAcks, CallIDs),
  lists:foreach(fun(Cb) -> eval_ack_cb(Cb, ?buffer_overflow_discarded) end, CbList),
  St#{replayq := NewQ, pending_acks := NewPendingAcks}.

reply_error_for_all_reqs(St, Reason) ->
  #{pending_acks := PendingAcks, config := Config, sent_reqs := SentReqs} = St,
  F = fun(Cb, Acc) ->
    eval_ack_cb(Cb, Reason),
    Acc + 1
  end,
  Count = wolff_pendack:fold(PendingAcks, F, 0),
  AlreadyInc = lists:foldl(
    fun(#{attempts := Attempts}, Acc) when Attempts > 1 ->
      %% 'failed' counter is already incremented for this sent request
      Acc + 1;
    (_, Acc) ->
      Acc
  end, 0, queue:to_list(SentReqs)),
  inc_sent_failed(Config, Count - AlreadyInc, 1).

%% use process dictionary for upgrade without restart
maybe_log_discard(St, Increment, Reason) ->
  Last = get_overflow_log_state(),
  #{last_cnt := LastCnt, acc_cnt := AccCnt} = Last,
  case LastCnt =:= AccCnt andalso Increment =:= 0 of
    true -> %% no change
      ok;
    false ->
      maybe_log_discard(St, Increment, Last, Reason)
  end.

maybe_log_discard(#{topic := Topic, partition := Partition},
                  Increment,
                  #{last_ts := LastTs, last_cnt := LastCnt, acc_cnt := AccCnt},
                 Reason) ->
  NowTs = now_ts(),
  NewAccCnt = AccCnt + Increment,
  DiffCnt = NewAccCnt - LastCnt,
  case NowTs - LastTs > ?MIN_DISCARD_LOG_INTERVAL of
    true ->
      log_warn(Topic, Partition,
               "dropped_produce_requests",
               #{count => DiffCnt, cause => Reason}),
      put_overflow_log_state(NowTs, NewAccCnt, NewAccCnt);
    false ->
      put_overflow_log_state(LastTs, LastCnt, NewAccCnt)
  end.

get_overflow_log_state() ->
  case get(?buffer_overflow_discarded) of
    undefined -> #{last_ts => 0, last_cnt => 0, acc_cnt => 0};
    V when is_map(V) -> V
  end.

put_overflow_log_state(Ts, Cnt, Acc) ->
  put(?buffer_overflow_discarded, #{last_ts => Ts, last_cnt => Cnt, acc_cnt => Acc}),
  ok.

inc_sent_failed(Config, NrOfCalls, Attempts) when Attempts > 1 ->
    wolff_metrics:retried_failed_inc(Config, NrOfCalls);
inc_sent_failed(Config, NrOfCalls, _Attempts) ->
    wolff_metrics:failed_inc(Config, NrOfCalls).

inc_sent_success(Config, NrOfCalls, Attempts) when Attempts > 1 ->
    wolff_metrics:retried_success_inc(Config, NrOfCalls);
inc_sent_success(Config, NrOfCalls, _Attempts) ->
    wolff_metrics:success_inc(Config, NrOfCalls).

is_replayq_durable(#{replayq_offload_mode := true}, _Q) ->
    false;
is_replayq_durable(_, Q) ->
    not replayq:is_mem_only(Q).

-spec escape(string() | binary()) -> binary().
escape(Str) ->
    NormalizedStr = unicode:characters_to_nfd_list(Str),
    iolist_to_binary(escape_uri(NormalizedStr)).

%% copied from `edoc_lib' because dialyzer cannot see this private
%% function there.
escape_uri([C | Cs]) when C >= $a, C =< $z ->
    [C | escape_uri(Cs)];
escape_uri([C | Cs]) when C >= $A, C =< $Z ->
    [C | escape_uri(Cs)];
escape_uri([C | Cs]) when C >= $0, C =< $9 ->
    [C | escape_uri(Cs)];
escape_uri([C = $. | Cs]) ->
    [C | escape_uri(Cs)];
escape_uri([C = $- | Cs]) ->
    [C | escape_uri(Cs)];
escape_uri([C = $_ | Cs]) ->
    [C | escape_uri(Cs)];
escape_uri([C | Cs]) when C > 16#7f ->
    %% This assumes that characters are at most 16 bits wide.
    escape_byte(((C band 16#c0) bsr 6) + 16#c0)
	++ escape_byte(C band 16#3f + 16#80)
	++ escape_uri(Cs);
escape_uri([C | Cs]) ->
    escape_byte(C) ++ escape_uri(Cs);
escape_uri([]) ->
    [].

%% copied from `edoc_lib' because dialyzer cannot see this private
%% function there.
%% has a small modification: it uses `=' in place of `%' so that it
%% won't generate invalid paths in windows.
escape_byte(C) when C >= 0, C =< 255 ->
    [$=, hex_digit(C bsr 4), hex_digit(C band 15)].

%% copied from `edoc_lib' because dialyzer cannot see this private
%% function there.
hex_digit(N) when N >= 0, N =< 9 ->
    N + $0;
hex_digit(N) when N > 9, N =< 15 ->
    N + $a - 10.

%% Alias in OTP 24/25 has memory leak.
%% https://github.com/erlang/otp/issues/6947
-if(?OTP_RELEASE >= 26).
caller() -> erlang:alias([reply]).
deref_caller(Alias) ->
    erlang:unalias(Alias).
-else.
caller() -> self().
deref_caller(_Pid) -> ok.
-endif.

-if(?OTP_RELEASE >= 27).
set_process_label(ClientId, Topic, Partition) ->
    proc_lib:set_label({?MODULE, ClientId, Topic, Partition}).
-else.
set_process_label(_ClientId, _Topic, _Partition) ->
    ok.
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

is_batch_expired_test_() ->
    Now = now_ts(),
    Item = fun(AgeMs) -> ?Q_ITEM(0, Now - AgeMs, [#{key => <<>>, value => <<>>}]) end,
    Sent = fun(Items) -> #{q_items => Items} end,
    [ {"infinity never expires",
       ?_assertNot(is_batch_expired(Sent([Item(10000)]), infinity, Now))}
    , {"all items older than max_batch_age -> expired",
       ?_assert(is_batch_expired(Sent([Item(100), Item(200)]), 50, Now))}
    , {"one item younger than max_batch_age -> not expired",
       ?_assertNot(is_batch_expired(Sent([Item(100), Item(10)]), 50, Now))}
    , {"exactly at max_batch_age -> expired",
       ?_assert(is_batch_expired(Sent([Item(50)]), 50, Now))}
    ].

is_max_retry_reached_test_() ->
    [ {"infinity never reached", ?_assertNot(is_max_retry_reached(1000000, infinity))}
      %% max_retry = 0 -> drop on the first attempt's failure
    , {"0: first attempt reached", ?_assert(is_max_retry_reached(1, 0))}
      %% max_retry = 3 -> allow attempts 1..4, drop when attempts reaches 4
    , {"3: attempt 3 not reached", ?_assertNot(is_max_retry_reached(3, 3))}
    , {"3: attempt 4 reached", ?_assert(is_max_retry_reached(4, 3))}
    ].

maybe_log_discard_test_() ->
    [ {"no-increment", fun() -> maybe_log_discard(undefined, 0, ?DISCARD_OVERFLOW) end}
    , {"fake-last-old",
       fun() ->
         Ts0 = now_ts() - ?MIN_DISCARD_LOG_INTERVAL - 1,
         ok = put_overflow_log_state(Ts0, 2, 2),
         ok = maybe_log_discard(#{topic => <<"a">>, partition => 0}, 1, ?DISCARD_OVERFLOW),
         St = get_overflow_log_state(),
         ?assertMatch(#{last_cnt := 3, acc_cnt := 3}, St),
         ?assert(maps:get(last_ts, St) - Ts0 > ?MIN_DISCARD_LOG_INTERVAL)
       end}
    , {"fake-last-fresh",
       fun() ->
         Ts0 = now_ts(),
         ok = put_overflow_log_state(Ts0, 2, 2),
         ok = maybe_log_discard(#{topic => <<"a">>, partition => 0}, 2, ?DISCARD_OVERFLOW),
         St = get_overflow_log_state(),
         ?assertMatch(#{last_cnt := 2, acc_cnt := 4}, St),
         ?assert(maps:get(last_ts, St) - Ts0 < ?MIN_DISCARD_LOG_INTERVAL)
       end}
    ].

high_mem_overflow_test_() ->
    %% A replayq holding plain integers, where each integer is its own size in bytes.
    OpenQ = fun(MaxTotal) ->
                    replayq:open(#{mem_only => true,
                                   sizer => fun(N) -> N end,
                                   max_total_bytes => MaxTotal})
            end,
    Append = fun(Q, Items) -> replayq:append(Q, Items) end,
    Cfg = fun(MSA, MBB) -> #{max_send_ahead => MSA, max_batch_bytes => MBB} end,
    [ {"empty queue, new call fits within reserve -> no drop",
       fun() ->
         %% Reserved = (2+1)*100 = 300. Queue holds 50 bytes after append.
         Q = Append(OpenQ(10000), [50]),
         ?assert(high_mem_overflow(Q, 50, Cfg(2, 100)) =< 0)
       end}
    , {"queue exactly at reserve -> no drop",
       fun() ->
         %% Reserved = 300, queue at 300, new call's bytes = 100.
         Q = Append(OpenQ(10000), [100, 100, 100]),
         ?assert(high_mem_overflow(Q, 100, Cfg(2, 100)) =< 0)
       end}
    , {"queue above reserve -> drop only the new call's bytes",
       fun() ->
         %% Reserved = 300, queue at 400 after appending 100. Excess = 100.
         Q = Append(OpenQ(10000), [100, 100, 100, 100]),
         ?assertEqual(100, high_mem_overflow(Q, 100, Cfg(2, 100)))
       end}
    , {"queue well above reserve -> still capped at the new call's bytes",
       fun() ->
         %% Reserved = 300, queue at 800. Excess = 500 but cap is CallByteSize = 100.
         Q = Append(OpenQ(10000), [100, 100, 100, 100, 100, 100, 100, 100]),
         ?assertEqual(100, high_mem_overflow(Q, 100, Cfg(2, 100)))
       end}
    , {"max_send_ahead = 0 keeps one batch worth of reserve",
       fun() ->
         %% Reserved = (0+1)*100 = 100, queue at 100 -> no drop yet.
         Q1 = Append(OpenQ(10000), [100]),
         ?assert(high_mem_overflow(Q1, 100, Cfg(0, 100)) =< 0),
         %% Queue at 200, new call 100 -> drop 100.
         Q2 = Append(OpenQ(10000), [100, 100]),
         ?assertEqual(100, high_mem_overflow(Q2, 100, Cfg(0, 100)))
       end}
    , {"replayq max_total_bytes overflow always wins",
       fun() ->
         %% max_total_bytes = 50, queue at 100 -> replayq:overflow = 50.
         %% Reserved = 300 (huge), Excess = max(0, 100-300) = 0.
         %% Result must still be 50 because the queue itself is over its hard limit.
         Q = Append(OpenQ(50), [100]),
         ?assertEqual(50, high_mem_overflow(Q, 100, Cfg(2, 100)))
       end}
    ].
-endif.
