%% Copyright (c) 2018-2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([start_link/5, stop/1, send/3, send/4, send_sync/3, ack_cb/4]).

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

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
                          telemetry_meta_data => map(),
                          max_partitions => pos_integer()
                         }.

-define(no_timer, no_timer).
-define(reconnect, reconnect).
-define(linger_expire, linger_expire).
-define(linger_expire_timer, linger_expire_timer).
-define(DEFAULT_REPLAYQ_SEG_BYTES, 10 * 1024 * 1024).
-define(DEFAULT_REPLAYQ_LIMIT, 2000000000).
-define(Q_ITEM(CallId, Ts, Batch), {CallId, Ts, Batch}).
-define(SEND_REQ(From, Batch, AckFun), {send, From, Batch, AckFun}).
-define(queued, queued).
-define(no_queued_reply, no_queued_reply).
-define(ACK_CB(AckCb, Partition), {AckCb, Partition}).
-define(no_queue_ack, no_queue_ack).
-define(no_caller_ack, no_caller_ack).
-define(MAX_LINGER_BYTES, (10 bsl 20)).
-type ack_fun() :: wolff:ack_fun().
-type send_req() :: ?SEND_REQ({pid(), reference()}, [wolff:msg()], ack_fun()).
-type sent() :: #{req_ref := reference(),
                  q_items := [?Q_ITEM(_CallId, _Ts, _Batch)],
                  q_ack_ref := replayq:ack_ref(),
                  attempts := pos_integer()
                 }.

-type state() :: #{ call_id_base := pos_integer()
                  , client_id := wolff:client_id()
                  , config := config_state()
                  , conn := undefined | _
                  , ?linger_expire_timer := false | reference()
                  , partition := partition()
                  , pending_acks := #{} % CallId => AckCb
                  , produce_api_vsn := undefined | _
                  , replayq := replayq:q()
                  , sent_reqs := queue:queue(sent())
                  , sent_reqs_count := non_neg_integer()
                  , inflight_calls := non_neg_integer()
                  , topic := topic()
                  , calls := empty | #{ts := pos_integer(), bytes := pos_integer(), batch_r := [send_req()]}
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
%% * `max_linger_ms': Age in milliseconds a baatch can stay in queue when the connection
%%    is idle (as in no pending acks). Default: 0 (as in send immediately)
%% * `max_linger_bytes': Number of bytes to collect before sending it to Kafka.
%%    If set to 0, `max_batch_bytes' is taken for mem-only mode, otherwise it's 10 times
%%    `max_batch_bytes' (but never exceeds 10MB) to optimize disk write.
%% * `max_send_ahead': Number of batches to be sent ahead without receiving ack for
%%    the last request. Must be 0 if messages must be delivered in strict order.
%% * `compression': `no_compression', `snappy' or `gzip'.
-spec start_link(wolff:client_id(), topic(), partition(), pid() | ?conn_down(any()), config_in()) ->
  {ok, pid()} | {error, any()}.
start_link(ClientId, Topic, Partition, MaybeConnPid, Config) ->
  St = #{client_id => ClientId,
         topic => Topic,
         partition => Partition,
         conn => MaybeConnPid,
         config => use_defaults(Config),
         ?linger_expire_timer => false
        },
  %% the garbage collection can be expensive if using the default 'on_heap' option.
  SpawnOpts = [{spawn_opt, [{message_queue_data, off_heap}]}],
  gen_server:start_link(?MODULE, St, SpawnOpts).

stop(Pid) ->
  gen_server:call(Pid, stop, infinity).

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
-spec send(pid(), [wolff:msg()], wolff:ack_fun(), WaitForQueued::wait_for_queued | no_wait_for_queued) -> ok.
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
  %% synced local usage, safe to use anonymous fun
  AckFun = {fun ?MODULE:ack_cb/4, [Caller, Mref]},
  ok = send(Pid, Batch0, AckFun, no_wait_for_queued),
  receive
    {Mref, Partition, BaseOffset} ->
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

%% @hidden Callbak exported for send_sync/3.
ack_cb(Partition, BaseOffset, Caller, Mref) ->
  _ = erlang:send(Caller, {Mref, Partition, BaseOffset}),
  ok.

init(St) ->
  erlang:process_flag(trap_exit, true),
  %% ensure init/1 can never fail
  %% so the caller can unify error handling on EXIT signal
  self() ! {do_init, St},
  {ok, #{}}.

-spec do_init(map()) -> state().
do_init(#{client_id := ClientId,
          conn := Conn,
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
  %% The initial connect attempt is made by the caller (wolff_producers.erl)
  %% If succeeded, `Conn' is a pid (but it may as well be dead by now),
  %% if failed, it's a `term()' to indicate the failure reason.
  %% Send it to self regardless of failure, retry timer should be started
  %% when the message is received (same handling as in when `DOWN' message is
  %% received after a normal start)
  _ = erlang:send(self(), ?leader_connection(Conn)),
  %% replayq configs are kept in Q, there is no need to duplicate them
  Config1 = maps:update_with(
              telemetry_meta_data,
              fun(Meta) -> Meta#{partition_id => Partition} end,
              #{partition_id => Partition},
              Config0),
  Config2 = resolve_max_linger_bytes(Config1, Q),
  Config = maps:without([replayq_dir, replayq_seg_bytes], Config2),
  wolff_metrics:queuing_set(Config, replayq:count(Q)),
  wolff_metrics:inflight_set(Config, 0),
  St#{replayq => Q,
      config := Config,
      call_id_base => erlang:system_time(microsecond),
      pending_acks => #{}, % CallId => AckCb
      produce_api_vsn => undefined,
      sent_reqs => queue:new(), % {kpro:req(), replayq:ack_ref(), [{CallId, MsgCount}]}
      sent_reqs_count => 0,
      inflight_calls => 0,
      conn := undefined,
      client_id => ClientId,
      calls => empty
  }.

handle_call(stop, From, St) ->
  gen_server:reply(From, ok),
  {stop, normal, St};
handle_call(_Call, _From, St) ->
  {noreply, St}.

handle_info({do_init, St0}, _) ->
  St = do_init(St0),
  {noreply, St};
handle_info(?linger_expire, St0) ->
  St1 = enqueue_calls(St0#{?linger_expire_timer => false}, no_linger),
  St = maybe_send_to_kafka(St1),
  {noreply, St};
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
    throw : Reason ->
      %% connection is not really down, but we need to
      %% stay down for a while and maybe restart sending on a new connection
      %% if Reason is not_leader_for_partition,
      %% wolff_client should expire old metadata while we are down
      %% and connect to the new leader when we request for a new connection
      St = mark_connection_down(St0, Reason),
      {noreply, St}
  end;
handle_info(?leader_connection(Conn), #{topic := Topic,
                                        partition := Partition
                                       } = St0) when is_pid(Conn) ->
  Attempts = maps:get(reconnect_attempts, St0, 0),
  Attempts > 0 andalso
    log_info(Topic, Partition, "partition_leader_reconnected", #{conn_pid => Conn}),
  _ = erlang:monitor(process, Conn),
  St1 = St0#{reconnect_timer => ?no_timer,
             reconnect_attempts => 0, %% reset counter
             conn := Conn},
  St2 = get_produce_version(St1),
  St3 = resend_sent_reqs(St2, ?reconnect),
  St  = maybe_send_to_kafka(St3),
  {noreply, St};
handle_info(?leader_connection(?conn_down(Reason)), St0) ->
  St = mark_connection_down(St0#{reconnect_timer => ?no_timer}, Reason),
  {noreply, St};
handle_info(?leader_connection(?conn_error(Reason)), St0) ->
  St = mark_connection_down(St0#{reconnect_timer => ?no_timer}, Reason),
  {noreply, St};
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

handle_cast(_Cast, St) ->
  {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
  {ok, St}.

terminate(_Reason, #{replayq := Q} = State) ->
  ok = replayq:close(Q),
  ok = clear_gauges(State, Q);
terminate(_Reason, _State) ->
  ok.

clear_gauges(#{config := Config}, Q) ->
  wolff_metrics:inflight_set(Config, 0),
  maybe_reset_queuing(Config, Q),
  ok.

maybe_reset_queuing(Config, Q) ->
  case {replayq:count(Q), is_replayq_durable(Config, Q)} of
    {0, _} ->
      wolff_metrics:queuing_set(Config, 0);
    {_, false} ->
      wolff_metrics:queuing_set(Config, 0);
    {_, _} ->
      ok
  end.

ensure_ts(Batch) ->
  lists:map(fun(#{ts := _} = Msg) -> Msg;
               (Msg) -> Msg#{ts => now_ts()}
            end, Batch).

make_call_id(Base) ->
  Base + erlang:unique_integer([positive]).

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
                        {reconnect_delay_ms, 2000}
                       ]).

use_defaults(Config, []) -> Config;
use_defaults(Config, [{K, V} | Rest]) ->
  case maps:is_key(K, Config) of
    true -> use_defaults(Config, Rest);
    false -> use_defaults(Config#{K => V}, Rest)
  end.

remake_requests(#{q_items := Items, attempts := Attempts} = Sent, St, ?reconnect) ->
  #kpro_req{ref = Ref} = Req = make_request(Items, St),
  {[Req], [Sent#{req_ref := Ref, attempts := Attempts + 1}]};
remake_requests(#{q_items := Items} = Sent, St, ?message_too_large) ->
  Reqs = lists:map(fun(Item) -> make_request([Item], St) end, Items),
  #{attempts := Attempts, q_ack_ref := Q_AckRef} = Sent,
  {Reqs, make_sent_items_list(Items, Reqs, Attempts + 1, Q_AckRef)}.

%% only ack replayq when the last message is accepted by Kafka
make_sent_items_list([LastItem], [#kpro_req{ref = Ref}], Attempts, Q_AckRef) ->
  [#{req_ref => Ref,
     q_items => [LastItem],
     q_ack_ref => Q_AckRef,
     attempts => Attempts
    }];
make_sent_items_list([Item | Items], [#kpro_req{ref = Ref} | Reqs], Attempts, Q_AckRef) ->
  [#{req_ref => Ref,
     q_items => [Item],
     q_ack_ref => ?no_queue_ack,
     attempts => Attempts
    } | make_sent_items_list(Items, Reqs, Attempts, Q_AckRef)].

make_request(QueueItems,
             #{config := #{required_acks := RequiredAcks,
                           ack_timeout := AckTimeout,
                           compression := Compression
                          },
               produce_api_vsn := Vsn,
               topic := Topic,
               partition := Partition}) ->
  Batch = get_batch_from_queue_items(QueueItems),
  kpro_req_lib:produce(Vsn, Topic, Partition, Batch,
                       #{ack_timeout => AckTimeout,
                         required_acks => RequiredAcks,
                         compression => Compression
                        }).

resend_sent_reqs(#{sent_reqs := SentReqs,
                   conn := Conn,
                   config := Config
                  } = St, Reason) ->
  F = fun(Sent, Acc) ->
          wolff_metrics:retried_inc(Config, 1),
          {Reqs, NewSentList} = remake_requests(Sent, St, Reason),
          lists:foreach(fun(Req) -> ok = request_async(Conn, Req) end, Reqs),
          lists:reverse(NewSentList, Acc)
      end,
  NewSentReqs = lists:foldl(F, [], queue:to_list(SentReqs)),
  St#{sent_reqs := queue:from_list(lists:reverse(NewSentReqs))}.

maybe_send_to_kafka(#{conn := Conn, replayq := Q} = St) ->
  case replayq:count(Q) =:= 0 of
    true ->
      %% nothing to send
      St;
    false when is_pid(Conn) ->
      %% has connection, try send
      maybe_send_to_kafka_has_pending(St);
    false ->
      %% no connection
      %% maybe the connection was closed after ideling
      %% re-connect is triggered immediately if this
      %% is the first attempt
      ensure_delayed_reconnect(St, no_delay_for_first_attempt)
  end.

maybe_send_to_kafka_has_pending(St) ->
  case is_send_ahead_allowed(St) of
    true -> send_to_kafka(St);
    false -> St %% reached max inflight limit
  end.

send_to_kafka(#{sent_reqs := SentReqs,
                sent_reqs_count := SentReqsCount,
                inflight_calls := InflightCalls,
                replayq := Q,
                config := #{max_batch_bytes := BytesLimit} = Config,
                conn := Conn
               } = St0) ->
  {NewQ, QAckRef, Items} =
    replayq:pop(Q, #{bytes_limit => BytesLimit, count_limit => 999999999}),
  wolff_metrics:queuing_set(Config, replayq:count(NewQ)),
  NewSentReqsCount = SentReqsCount + 1,
  NrOfCalls = count_calls(Items),
  NewInflightCalls = InflightCalls + NrOfCalls,
  _ = wolff_metrics:inflight_set(Config, NewInflightCalls),
  #kpro_req{ref = Ref, no_ack = NoAck} = Req = make_request(Items, St0),
  St1 = St0#{replayq := NewQ},
  Sent = #{req_ref => Ref,
           q_items => Items,
           q_ack_ref => QAckRef,
           attempts => 1
          },
  St2 = St1#{sent_reqs := queue:in(Sent, SentReqs),
             sent_reqs_count := NewSentReqsCount,
             inflight_calls := NewInflightCalls
            },
  ok = request_async(Conn, Req),
  St3 = maybe_fake_kafka_ack(NoAck, Sent, St2),
  maybe_send_to_kafka(St3).

%% when require no acks do not add to sent_reqs and ack caller immediately
maybe_fake_kafka_ack(_NoAck = true, Sent, St) ->
  do_handle_kafka_ack(?no_error, ?UNKNOWN_OFFSET, Sent, St);
maybe_fake_kafka_ack(_NoAck, _Sent, St) -> St.

is_send_ahead_allowed(#{config := #{max_send_ahead := Max},
                        sent_reqs_count := SentCount}) ->
  SentCount =< Max.

%% nothing sent and nothing pending to be sent
is_idle(#{replayq := Q, sent_reqs_count := SentReqsCount}) ->
  SentReqsCount =:= 0 andalso replayq:count(Q) =:= 0.

now_ts() ->erlang:system_time(millisecond).

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

count_calls([]) ->
  0;
count_calls([?Q_ITEM(?no_caller_ack, _Ts, _Batch) | Rest]) ->
  count_calls(Rest);
count_calls([?Q_ITEM(_CallId, _Ts, _Batch) | Rest]) ->
  1 + count_calls(Rest).

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
    {value, #{req_ref := Ref} = Sent} ->
      %% sent_reqs queue front matched the response reference
      do_handle_kafka_ack(ErrorCode, BaseOffset, Sent, St);
    _ ->
      %% stale response e.g. when inflight > 1, but we have to retry an older req
      St
  end.

note(Fmt, Args) ->
  lists:flatten(io_lib:format(Fmt, Args)).

-spec do_handle_kafka_ack(_, _, sent(), state()) -> state().
do_handle_kafka_ack(?no_error, BaseOffset, _Sent, St) ->
  clear_sent_and_ack_callers(?no_error, BaseOffset, St);
do_handle_kafka_ack(?message_too_large = EC, _BaseOffset, Sent, St) ->
  #{topic := Topic, partition := Partition, config := Config} = St,
  #{q_items := Items} = Sent,
  #kpro_req{msg = IoData} = make_request(Items, St),
  Bytes = iolist_size(IoData),
  case length(Items) of
    1 ->
      %% This is a single message batch, but it's still too large
      Note = "A single-request batch is dropped because it's too large for this topic! "
             "Consider increasing 'max.message.bytes' config on the server side!",
      log_error(Topic, Partition, EC, #{note => Note, encode_bytes => Bytes}),
      wolff_metrics:dropped_inc(Config, 1),
      clear_sent_and_ack_callers(EC, EC, St);
    N ->
      %% This is a batch of more than one queue items (calls)
      %% Split the batch, and re-send
      #{max_batch_bytes := Max} = Config,
      NewMax = max(1, (Max + 1) div 2),
      St1 = St#{config := Config#{max_batch_bytes := NewMax}},
      Note = note("Config max_batch_bytes=~p is too large for this topic, "
                  "trying to split the current batch and retry. "
                  "Will use max_batch_bytes=~p to collect future batches.",
                  [Max, NewMax]),
      log_warn(Topic, Partition, EC, #{note => Note, calls_count => N, encode_bytes => Bytes}),
      resend_sent_reqs(St1, EC)
  end;
do_handle_kafka_ack(ErrorCode, _BaseOffset, Sent, St) ->
  %% Other errors, such as not_leader_for_partition
  #{attempts := Attempts, q_items := Items} = Sent,
  #{topic := Topic,
    partition := Partition,
    config := Config
   } = St,
  NrOfCalls = count_calls(Items),
  inc_sent_failed(Config, NrOfCalls, Attempts),
  log_warn(Topic, Partition, "error_in_produce_response",
           #{error_code => ErrorCode,
             batch_size => count_msgs(Items),
             attempts => Attempts}),
  erlang:throw(ErrorCode).

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

ensure_delayed_reconnect(#{config := #{reconnect_delay_ms := Delay0} = Config,
                           client_id := ClientId,
                           topic := Topic,
                           partition := Partition,
                           reconnect_timer := ?no_timer
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
  end;
ensure_delayed_reconnect(St, _Delay) ->
  %% timer already started
  St.

evaluate_pending_ack_funs(PendingAcks, [], _BaseOffset) -> PendingAcks;
evaluate_pending_ack_funs(PendingAcks, [{?no_caller_ack, BatchSize} | Rest], BaseOffset) ->
  evaluate_pending_ack_funs(PendingAcks, Rest, offset(BaseOffset, BatchSize));
evaluate_pending_ack_funs(PendingAcks, [{CallId, BatchSize} | Rest], BaseOffset) ->
  NewPendingAcks =
    case maps:get(CallId, PendingAcks, false) of
      false ->
        PendingAcks;
      AckCb ->
        ok = eval_ack_cb(AckCb, BaseOffset),
        maps:without([CallId], PendingAcks)
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

request_async(Conn, Req) when is_pid(Conn) ->
  ok = kpro:send(Conn, Req).

%% collect send calls which are already sent to mailbox,
%% the collection is size-limited by the max_linger_bytes config.
collect_send_calls(Call, Bytes, empty, Max) ->
  Init = #{ts => now_ts(), bytes => 0, batch_r => []},
  collect_send_calls(Call, Bytes, Init, Max);
collect_send_calls(Call, Bytes, #{ts := Ts, bytes := Bytes0, batch_r := BatchR} = Calls, Max) ->
  Sum = Bytes0 + Bytes,
  R = Calls#{ts => Ts, bytes => Sum, batch_r => [Call | BatchR]},
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
is_linger_continue(#{calls := Calls, config := Config}) ->
  #{max_linger_ms := MaxLingerMs, max_linger_bytes := MaxLingerBytes} = Config,
  #{ts := Ts, bytes := Bytes} = Calls,
  case Bytes < MaxLingerBytes of
    true ->
      TimeLeft = MaxLingerMs - (now_ts() - Ts),
      (TimeLeft > 0) andalso {true, TimeLeft};
    false ->
      false
  end.

enqueue_calls(#{calls := empty} = St, _) ->
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
  enqueue_calls2(Calls, St#{calls => empty}).

enqueue_calls2(Calls,
               #{replayq := Q,
                 pending_acks := PendingAcks0,
                 call_id_base := CallIdBase,
                 partition := Partition,
                 config := Config0
                } = St0) ->
  {QueueItems, PendingAcks, CallByteSize} =
    lists:foldl(
      fun(?SEND_REQ(_From, Batch, AckFun), {Items, PendingAcksIn, Size}) ->
          CallId = make_call_id(CallIdBase),
          %% keep callback funs in memory, do not seralize it into queue because
          %% saving anonymous function to disk may easily lead to badfun exception
          %% in case of restart on newer version beam.
          PendingAcksOut = PendingAcksIn#{CallId => ?ACK_CB(AckFun, Partition)},
          NewItems = [make_queue_item(CallId, Batch) | Items],
          {NewItems, PendingAcksOut, Size + batch_bytes(Batch)}
      end, {[], PendingAcks0, 0}, Calls),
   NewQ = replayq:append(Q, lists:reverse(QueueItems)),
   wolff_metrics:queuing_set(Config0, replayq:count(NewQ)),
   lists:foreach(fun maybe_reply_queued/1, Calls),
   Overflow = case maps:get(drop_if_highmem, Config0, false)
                  andalso replayq:is_mem_only(NewQ)
                  andalso load_ctl:is_high_mem() of
                  true ->
                      max(replayq:overflow(NewQ), CallByteSize);
                  false ->
                      replayq:overflow(NewQ)
              end,
   handle_overflow(St0#{replayq := NewQ,
                        pending_acks := PendingAcks
                       },
                   Overflow).

maybe_reply_queued(?SEND_REQ(?no_queued_reply, _, _)) ->
    ok;
maybe_reply_queued(?SEND_REQ({Caller, Ref}, _, _)) ->
    erlang:send(Caller, {Ref, ?queued}).

eval_ack_cb(?ACK_CB(AckFun, Partition), BaseOffset) when is_function(AckFun, 2) ->
  ok = AckFun(Partition, BaseOffset); %% backward compatible
eval_ack_cb(?ACK_CB({F, A}, Partition), BaseOffset) ->
  true = is_function(F, length(A) + 2),
  ok = erlang:apply(F, [Partition, BaseOffset | A]).

handle_overflow(St, Overflow) when Overflow =< 0 ->
    ok = maybe_log_discard(St, 0),
    St;
handle_overflow(#{replayq := Q,
                  pending_acks := PendingAcks,
                  config := Config
                 } = St,
                Overflow) ->
  {NewQ, QAckRef, Items} =
    replayq:pop(Q, #{bytes_limit => Overflow, count_limit => 999999999}),
  ok = replayq:ack(NewQ, QAckRef),
  Calls = get_calls_from_queue_items(Items),
  NrOfCalls = count_calls(Items),
  wolff_metrics:dropped_queue_full_inc(Config, NrOfCalls),
  wolff_metrics:dropped_inc(Config, NrOfCalls),
  wolff_metrics:queuing_set(Config, replayq:count(NewQ)),
  ok = maybe_log_discard(St, NrOfCalls),
  NewPendingAcks = evaluate_pending_ack_funs(PendingAcks, Calls, ?buffer_overflow_discarded),
  St#{replayq := NewQ, pending_acks := NewPendingAcks}.

%% use process dictionary for upgrade without restart
maybe_log_discard(St, Increment) ->
  Last = get_overflow_log_state(),
  #{last_cnt := LastCnt, acc_cnt := AccCnt} = Last,
  case LastCnt =:= AccCnt andalso Increment =:= 0 of
    true -> %% no change
      ok;
    false ->
      maybe_log_discard(St, Increment, Last)
  end.

maybe_log_discard(#{topic := Topic, partition := Partition},
                  Increment,
                  #{last_ts := LastTs, last_cnt := LastCnt, acc_cnt := AccCnt}) ->
  NowTs = erlang:system_time(millisecond),
  NewAccCnt = AccCnt + Increment,
  DiffCnt = NewAccCnt - LastCnt,
  case NowTs - LastTs > ?MIN_DISCARD_LOG_INTERVAL of
    true ->
      log_warn(Topic, Partition,
               "replayq_overflow_dropped_produce_calls",
               #{count => DiffCnt}),
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

maybe_log_discard_test_() ->
    [ {"no-increment", fun() -> maybe_log_discard(undefined, 0) end}
    , {"fake-last-old",
       fun() ->
         Ts0 = erlang:system_time(millisecond) - ?MIN_DISCARD_LOG_INTERVAL - 1,
         ok = put_overflow_log_state(Ts0, 2, 2),
         ok = maybe_log_discard(#{topic => <<"a">>, partition => 0}, 1),
         St = get_overflow_log_state(),
         ?assertMatch(#{last_cnt := 3, acc_cnt := 3}, St),
         ?assert(maps:get(last_ts, St) - Ts0 > ?MIN_DISCARD_LOG_INTERVAL)
       end}
    , {"fake-last-fresh",
       fun() ->
         Ts0 = erlang:system_time(millisecond),
         ok = put_overflow_log_state(Ts0, 2, 2),
         ok = maybe_log_discard(#{topic => <<"a">>, partition => 0}, 2),
         St = get_overflow_log_state(),
         ?assertMatch(#{last_cnt := 2, acc_cnt := 4}, St),
         ?assert(maps:get(last_ts, St) - Ts0 < ?MIN_DISCARD_LOG_INTERVAL)
       end}
    ].
-endif.
