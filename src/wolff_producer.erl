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
-export([start_link/5, stop/1, send/3, send_sync/3]).

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

%% replayq callbacks
-export([queue_item_sizer/1, queue_item_marshaller/1]).

%% for test
-export([batch_bytes/1, varint_bytes/1]).

-export_type([config/0]).

-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type offset_reply() :: wolff:offset_reply().
-type config() :: #{replayq_dir := string(),
                    replayq_max_total_bytes => pos_integer(),
                    replayq_seg_bytes => pos_integer(),
                    replayq_offload_mode => boolean(),
                    required_acks => kpro:required_acks(),
                    ack_timeout => timeout(),
                    max_batch_bytes => pos_integer(),
                    max_linger_ms => non_neg_integer(),
                    max_send_ahead => non_neg_integer(),
                    compression => kpro:compress_option()
                   }.

-define(no_timer, no_timer).
-define(reconnect, reconnect).
-define(sent_req(Req, Q_AckRef, Calls), {Req, Q_AckRef, Calls}).
-define(linger_expire, linger_expire).
-define(linger_expire_timer, linger_expire_timer).
-define(DEFAULT_REPLAYQ_SEG_BYTES, 10 * 1024 * 1024).
-define(DEFAULT_REPLAYQ_LIMIT, 2000000000).
-define(Q_ITEM(CallId, Ts, Batch), {CallId, Ts, Batch}).
-define(SEND_REQ(From, Batch, AckFun), {send, From, Batch, AckFun}).
-define(queued, queued).
-define(no_queued_reply, no_queued_reply).
-define(ACK_CB(AckCb, Partition), {AckCb, Partition}).

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
%% * `max_send_ahead': Number of batches to be sent ahead without receiving ack for
%%    the last request. Must be 0 if messages must be delivered in strict order.
%% * `compression': `no_compression', `snappy' or `gzip'.
-spec start_link(wolff:client_id(), topic(), partition(), pid() | ?conn_down(any()), config()) ->
  {ok, pid()} | {error, any()}.
start_link(ClientId, Topic, Partition, MaybeConnPid, Config) ->
  St = #{client_id => ClientId,
         topic => Topic,
         partition => Partition,
         conn => MaybeConnPid,
         config => use_defaults(Config),
         ?linger_expire_timer => false
        },
  gen_server:start_link(?MODULE, St, []).

stop(Pid) ->
  gen_server:call(Pid, stop, infinity).

%% @doc Send a batch asynchronously.
%% The callback function is evaluated by producer process when ack is received from kafka.
%% In case `required_acks' is configured to `none', the callback is evaluated immediately after send.
%% NOTE: This API has no backpressure,
%%       high produce rate may cause execussive ram and disk usage.
%% NOTE: It's possible that two or more batches get included into one produce request.
%%       But a batch is never split into produce requests.
%%       Make sure it will not exceed the `max_batch_bytes' limit when sending a batch.
-spec send(pid(), [wolff:msg()], wolff:ack_fun()) -> ok.
send(Pid, [_ | _] = Batch0, AckFun) ->
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
  end.

%% @doc Send a batch synchronously.
%% Raise error exception in case produce pid is down or when timed out.
-spec send_sync(pid(), [wolff:msg()], timeout()) -> {partition(), offset_reply()}.
send_sync(Pid, Batch0, Timeout) ->
  Caller = self(),
  Mref = erlang:monitor(process, Pid),
  %% synced local usage, safe to use anonymous fun
  AckFun = fun(Partition, BaseOffset) ->
               _ = erlang:send(Caller, {Mref, Partition, BaseOffset}),
               ok
           end,
  Batch = ensure_ts(Batch0),
  erlang:send(Pid, ?SEND_REQ(?no_queued_reply, Batch, AckFun)),
  receive
    {Mref, Partition, BaseOffset} ->
      erlang:demonitor(Mref, [flush]),
      {Partition, BaseOffset};
    {'DOWN', Mref, _, _, Reason} ->
      erlang:error({producer_down, Reason})
  after
    Timeout ->
      erlang:demonitor(Mref, [flush]),
      erlang:error(timeout)
  end.

init(St) ->
  erlang:process_flag(trap_exit, true),
  %% ensure init/1 can never fail
  %% so the caller can unify error handeling on EXIT signal
  self() ! {do_init, St},
  {ok, #{}}.

do_init(#{client_id := ClientId,
          conn := Conn,
          topic := Topic,
          partition := Partition,
          config := Config0
         } = St) ->
  QCfg = case maps:get(replayq_dir, Config0, false) of
           false ->
             #{mem_only => true};
           BaseDir ->
             Dir = filename:join([BaseDir, Topic, integer_to_list(Partition)]),
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
  %% If succeeded, `Conn' is a pid (but it may as well dead by now),
  %% if failed, it's a `term()' to indicate the failure reason.
  %% Send it to self regardless of failure, retry timer should be started
  %% when the message is received (same handling as in when `DOWN' message is
  %% received after a normal start)
  _ = erlang:send(self(), ?leader_connection(Conn)),
  %% replayq configs are kept in Q, there is no need to duplicate them
  Config = maps:without([replayq_dir, replayq_seg_bytes], Config0),
  St#{replayq => Q,
      config := Config,
      call_id_base => erlang:system_time(microsecond),
      pending_acks => #{}, % CallId => AckCb
      sent_reqs => queue:new(), % {kpro:req(), replayq:ack_ref(), [{CallId, MsgCount}]}
      sent_reqs_count => 0,
      conn := undefined,
      client_id => ClientId
      }.

handle_call(stop, From, St) ->
  gen_server:reply(From, ok),
  {stop, normal, St};
handle_call(_Call, _From, St) ->
  {noreply, St}.

handle_info({do_init, St0}, _) ->
  St = do_init(St0),
  {noreply, St};
handle_info(?linger_expire, St) ->
  {noreply, maybe_send_to_kafka(St#{?linger_expire_timer := false})};
handle_info(?SEND_REQ(_, Batch, _) = Call, #{client_id := ClientId,
                                             topic := Topic,
                                             partition := Partition,
                                             config := #{max_batch_bytes := Limit}
                                            } = St0) ->
  {Calls, Cnt, Oct} = collect_send_calls([Call], 1, batch_bytes(Batch), Limit),
  ok = wolff_stats:recv(ClientId, Topic, Partition, #{cnt => Cnt, oct => Oct}),
  St1 = enqueue_calls(Calls, St0),
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
    log_info(Topic, Partition, "partition leader reconnected: ~0p", [Conn]),
  _ = erlang:monitor(process, Conn),
  St1 = St0#{reconnect_timer => ?no_timer,
             reconnect_attempts => 0, %% reset counter
             conn := Conn},
  St2 = get_produce_version(St1),
  St3 = resend_sent_reqs(St2),
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

terminate(_, #{replayq := Q}) ->
  ok = replayq:close(Q);
terminate(_, _) ->
  ok.

ensure_ts(Batch) ->
  lists:map(fun(#{ts := _} = Msg) -> Msg;
               (Msg) -> Msg#{ts => now_ts()}
            end, Batch).

make_call_id(Base) ->
  Base + erlang:unique_integer([positive]).

use_defaults(Config) ->
  use_defaults(Config, [{required_acks, all_isr},
                        {ack_timeout, 10000},
                        {max_batch_bytes, ?WOLFF_KAFKA_DEFAULT_MAX_MESSAGE_BYTES},
                        {max_linger_ms, 0},
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

resend_sent_reqs(#{sent_reqs := SentReqs,
                   conn := Conn
                  } = St) ->
  F = fun(?sent_req(Req0, Q_AckRef, Calls), Acc) ->
          Req = Req0#kpro_req{ref = make_ref()}, %% make a new reference
          ok = request_async(Conn, Req),
          NewSent = ?sent_req(Req, Q_AckRef, Calls),
          [NewSent | Acc]
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
    true -> maybe_send_to_kafka_now(St);
    false -> St %% reached max inflight limit
  end.

maybe_send_to_kafka_now(#{?linger_expire_timer := LTimer,
                          replayq := Q,
                          config := #{max_linger_ms := Max}} = St) ->
  First = replayq:peek(Q),
  LingerTimeout = Max - (now_ts() - get_item_ts(First)),
  case LingerTimeout =< 0 of
    true ->
      %% the oldest item is too old, send now
      send_to_kafka(St); %% send now
    false when is_reference(LTimer) ->
      %% timer already started
      St;
    false ->
      %% delay send, try to accumulate more into the batch
      Ref = erlang:send_after(LingerTimeout, self(), ?linger_expire),
      St#{?linger_expire_timer := Ref}
  end.

send_to_kafka(#{sent_reqs := Sent,
                sent_reqs_count := SentCount,
                replayq := Q,
                config := #{max_batch_bytes := BytesLimit,
                            required_acks := RequiredAcks,
                            ack_timeout := AckTimeout,
                            compression := Compression
                           },
                conn := Conn,
                produce_api_vsn := Vsn,
                topic := Topic,
                partition := Partition,
                ?linger_expire_timer := LTimer
               } = St0) ->
  %% timer might have alreay expired, but should do no harm
  is_reference(LTimer) andalso erlang:cancel_timer(LTimer),
  {NewQ, QAckRef, Items} =
    replayq:pop(Q, #{bytes_limit => BytesLimit, count_limit => 999999999}),
  {FlatBatch, Calls} = get_flat_batch(Items, [], []),
  [_ | _] = FlatBatch, %% assert
  Req = kpro_req_lib:produce(Vsn, Topic, Partition, FlatBatch,
                             #{ack_timeout => AckTimeout,
                               required_acks => RequiredAcks,
                               compression => Compression
                              }),
  St1 = St0#{replayq := NewQ, ?linger_expire_timer := false},
  NewSent = ?sent_req(Req, QAckRef, Calls),
  St2 = St1#{sent_reqs := queue:in(NewSent, Sent),
             sent_reqs_count := SentCount + 1
            },
  ok = request_async(Conn, Req),
  ok = send_stats(St2, FlatBatch),
  St3 = maybe_fake_kafka_ack(Req, St2),
  maybe_send_to_kafka(St3).

%% when require no acks do not add to sent_reqs and ack caller immediately
maybe_fake_kafka_ack(#kpro_req{no_ack = true, ref = Ref}, St) ->
  do_handle_kafka_ack(Ref, ?UNKNOWN_OFFSET, St);
maybe_fake_kafka_ack(_Req, St) -> St.

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

get_item_ts(?Q_ITEM(_, Ts, _)) -> Ts.

get_produce_version(#{conn := Conn} = St) when is_pid(Conn) ->
  Vsn = case kpro:get_api_vsn_range(Conn, produce) of
          {ok, {_Min, Max}} -> Max;
          {error, _} -> 3 %% minimum magic v2 produce API version
        end,
  St#{produce_api_vsn => Vsn}.

get_flat_batch([], Msgs, Calls) ->
  {lists:reverse(Msgs), lists:reverse(Calls)};
get_flat_batch([QItem | Rest], Msgs, Calls) ->
  ?Q_ITEM(CallId, _Ts, Batch) = QItem,
  get_flat_batch(Rest, lists:reverse(Batch, Msgs),
                 [{CallId, length(Batch)} | Calls]).

handle_kafka_ack(#kpro_rsp{api = produce,
                           ref = Ref,
                           msg = Rsp
                          }, St) ->
  [TopicRsp] = kpro:find(responses, Rsp),
  [PartitionRsp] = kpro:find(partition_responses, TopicRsp),
  ErrorCode = kpro:find(error_code, PartitionRsp),
  BaseOffset = kpro:find(base_offset, PartitionRsp),
  case ErrorCode =:= ?no_error of
    true ->
      do_handle_kafka_ack(Ref, BaseOffset, St);
    false ->
      #{topic := Topic, partition := Partition} = St,
      log_warn(Topic, Partition, "~s-~p: Produce response error-code = ~0p", [ErrorCode]),
      erlang:throw(ErrorCode)
  end.

do_handle_kafka_ack(Ref, BaseOffset,
                    #{sent_reqs := SentReqs,
                      sent_reqs_count := SentReqsCount,
                      pending_acks := PendingAcks,
                      replayq := Q
                     } = St) ->
  case queue:peek(SentReqs) of
    {value, ?sent_req(#kpro_req{ref = Ref}, Q_AckRef, Calls)} ->
      ok = replayq:ack(Q, Q_AckRef),
      {{value, _}, NewSentReqs} = queue:out(SentReqs),
      NewPendingAcks = evaluate_pending_ack_funs(PendingAcks, Calls, BaseOffset),
      St#{sent_reqs := NewSentReqs,
          sent_reqs_count := SentReqsCount - 1,
          pending_acks := NewPendingAcks
         };
    _ ->
      %% stale ack
      St
  end.

%% @private This function is called in below scenarios
%% * Failed to connect any of the brokers
%% * Failed to connect to partition leader
%% * Connection 'DOWN' due to Kafka initiated soket close
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
log_connection_down(Topic, Partition, Conn, Reason) when is_pid(Conn) ->
  log_info(Topic, Partition, "connection to partition leader is down, pid=~p, reason=~0p", [Conn, Reason]);
log_connection_down(Topic, Partition, _, noproc) ->
  log_info(Topic, Partition, "connection to partition leader is down, pending on reconnect", []);
log_connection_down(Topic, Partition, _, Reason) ->
  log_info(Topic, Partition, "connection to partition leader is down, reason=~p", [Reason]).

ensure_delayed_reconnect(#{config := #{reconnect_delay_ms := Delay0},
                           client_id := ClientId,
                           topic := Topic,
                           partition := Partition,
                           reconnect_timer := ?no_timer
                          } = St, DelayStrategy) ->
  Attempts = maps:get(reconnect_attempts, St, 0),
  Attempts > 0 andalso Attempts rem 10 =:= 0 andalso
    log_error(Topic, Partition, "still disconnected after ~p reconnect attempts", [Attempts]),
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
      Args = [ClientPid, Topic, Partition, self()],
      {ok, Tref} = timer:apply_after(Delay, wolff_client, recv_leader_connection, Args),
      St#{reconnect_timer => Tref, reconnect_attempts => Attempts + 1};
    {error, _Restarting} ->
      log_warn(Topic, Partition, "client down, will try to rediscover client pid after ~p ms delay", [Delay]),
      %% call timer:apply_after for both cases, do not use send_after here
      {ok, Tref} = timer:apply_after(Delay, erlang, send, [self(), ?reconnect]),
      St#{reconnect_timer => Tref, reconnect_attempts => Attempts + 1}
  end;
ensure_delayed_reconnect(St, _Delay) ->
  %% timer already started
  St.

evaluate_pending_ack_funs(PendingAcks, [], _BaseOffset) -> PendingAcks;
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

log_info(Topic, Partition, Fmt, Args) ->
  error_logger:info_msg("~s-~p: " ++ Fmt, [Topic, Partition | Args]).

log_warn(Topic, Partition, Fmt, Args) ->
  error_logger:warning_msg("~s-~p: " ++ Fmt, [Topic, Partition | Args]).

log_error(Topic, Partition, Fmt, Args) ->
    error_logger:error_msg("~s-~p: " ++ Fmt, [Topic, Partition | Args]).

send_stats(#{client_id := ClientId, topic := Topic, partition := Partition}, Batch) ->
  {Cnt, Oct} =
    lists:foldl(fun(Msg, {C, O}) -> {C + 1, O + oct(Msg)} end, {0, 0}, Batch),
  ok = wolff_stats:sent(ClientId, Topic, Partition, #{cnt => Cnt, oct => Oct}).

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

%% Collect all send requests which are already in process mailbox
collect_send_calls(Calls, Count, Size, Limit) when Size >= Limit ->
  {lists:reverse(Calls), Count, Size};
collect_send_calls(Calls, Count, Size, Limit) ->
  receive
    ?SEND_REQ(_, Batch, _) = Call ->
      collect_send_calls([Call | Calls], Count + 1, Size + batch_bytes(Batch), Limit)
  after
    0 ->
      {lists:reverse(Calls), Count, Size}
  end.

enqueue_calls(Calls, #{replayq := Q,
                       pending_acks := PendingAcks0,
                       call_id_base := CallIdBase,
                       partition := Partition
                      } = St0) ->
  {QueueItems, PendingAcks} =
    lists:foldl(
      fun(?SEND_REQ(_From, Batch, AckFun), {Items, PendingAcksIn}) ->
          CallId = make_call_id(CallIdBase),
          %% keep callback funs in memory, do not seralize it into queue because
          %% saving anonymous function to disk may easily lead to badfun exception
          %% in case of restart on newer version beam.
          PendingAcksOut = PendingAcksIn#{CallId => ?ACK_CB(AckFun, Partition)},
          NewItems = [make_queue_item(CallId, Batch) | Items],
          {NewItems, PendingAcksOut}
      end, {[], PendingAcks0}, Calls),
   NewQ = replayq:append(Q, lists:reverse(QueueItems)),
   lists:foreach(fun maybe_reply_queued/1, Calls),
   handle_overflow(St0#{replayq := NewQ,
                        pending_acks := PendingAcks
                       }, replayq:overflow(NewQ)).

maybe_reply_queued(?SEND_REQ(?no_queued_reply, _, _)) -> ok;
maybe_reply_queued(?SEND_REQ({Pid, Ref}, _, _)) -> erlang:send(Pid, {Ref, ?queued}).

eval_ack_cb(?ACK_CB(AckFun, Partition), BaseOffset) when is_function(AckFun, 2) ->
  ok = AckFun(Partition, BaseOffset); %% backward compatible
eval_ack_cb(?ACK_CB({F, A}, Partition), BaseOffset) ->
  true = is_function(F, length(A) + 2),
  ok = erlang:apply(F, [Partition, BaseOffset | A]).

handle_overflow(St, Overflow) when Overflow =< 0 ->
    ok = maybe_log_discard(St, 0),
    St;
handle_overflow(#{replayq := Q,
                  pending_acks := PendingAcks
                 } = St, Overflow) ->
  {NewQ, QAckRef, Items} =
    replayq:pop(Q, #{bytes_limit => Overflow, count_limit => 999999999}),
  ok = replayq:ack(NewQ, QAckRef),
  {FlatBatch, Calls} = get_flat_batch(Items, [], []),
  [_ | _] = FlatBatch, %% assert
  ok = maybe_log_discard(St, length(Calls)),
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

maybe_log_discard(#{topic := Topic, partition := Partition}, Increment,
                  #{last_ts := LastTs, last_cnt := LastCnt, acc_cnt := AccCnt}) ->
  NowTs = erlang:system_time(millisecond),
  NewAccCnt = AccCnt + Increment,
  DiffCnt = NewAccCnt - LastCnt,
  case NowTs - LastTs > ?MIN_DISCARD_LOG_INTERVAL of
    true ->
      log_warn(Topic, Partition, "replayq_overflow_dropped_number_of_requests ~p", [DiffCnt]),
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
