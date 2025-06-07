-module(wolff_tests).

-export([ack_cb/4]).

%% Helper functions so they can be used in other test files
-export([get_telemetry_seq/2,
         handle_telemetry_event/4,
         telemetry_id/0,
         telemetry_events/0,
         install_event_logging/1,
         install_event_logging/3,
         deinstall_event_logging/1,
         print_telemetry_check/2]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("lc/include/lc.hrl").
-include("wolff.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").

-define(KEY, key(?FUNCTION_NAME)).
-define(HOSTS, [{"localhost", 9092}]).
-define(assert_eq_optional_tail(EXPR,EXPECTED), assert_eq_optional_tail(fun() -> EXPR end, EXPECTED)).

%% Kafka v2 batch consists of below fields:
%%   FIELD          TYPE              BYTES   BYTES_ACC
%%   FirstOffset => int64           # 8  8
%%   Length => int32                # 4  12
%%   PartitionLeaderEpoch => int32  # 4  16
%%   Magic => int8                  # 1  17
%%   CRC => int32                   # 4  21
%%   Attributes => int16            # 2  23
%%   LastOffsetDelta => int32       # 4  27
%%   FirstTimestamp => int64        # 8  35
%%   MaxTimestamp => int64          # 8  43
%%   ProducerId => int64            # 8  51
%%   ProducerEpoch => int16         # 2  53
%%   FirstSequence => int32         # 4  57
%%   Records => [Record]            # 4  61
%% i.e. Kafka batching overhead is 61 bytes.
-define(BATCHING_OVERHEAD, 61).
-define(MAX_BATCH_BYTES,
        (?WOLFF_KAFKA_DEFAULT_MAX_MESSAGE_BYTES - ?BATCHING_OVERHEAD)).

-define(WAIT(TIMEOUT, RECEIVE, THEN),
        receive
          RECEIVE ->
            THEN;
          UNEXPECTEDMSG ->
            error({unexpected, UNEXPECTEDMSG})
        after
          TIMEOUT ->
            error(timeout)
        end).

ack_cb(Partition, Offset, Self, Ref) ->
  Self ! {ack, Ref, Partition, Offset},
  ok.

metadata_connection_restart_test_() ->
  {timeout, 10, fun metadata_connection_restart/0}.

metadata_connection_restart() ->
  ClientCfg = client_config(),
  ClientId = <<"client-1">>,
  {ok, Client} = start_client(ClientId, ?HOSTS, ClientCfg),
  GetMetadataConn = fun() ->
    ok = wolff:check_connectivity(ClientId),
    ok = wolff_client:check_connectivity(Client),
    State = sys:get_state(Client),
    Pid = maps:get(metadata_conn, State),
    ?assert(is_process_alive(Pid)),
    Pid
  end,
  Pid1 = GetMetadataConn(),
  exit(Pid1, kill),
  Pid2 = GetMetadataConn(),
  ok = stop_client(Client),
  ?assertNot(is_process_alive(Pid2)).

metadata_connection_restart2_test_() ->
  {timeout, 10, fun metadata_connection_restart2/0}.

metadata_connection_restart2() ->
  ClientCfg0 = client_config(),
  ClientCfg = ClientCfg0#{min_metadata_refresh_interval => 0},
  ClientId = <<"client-1">>,
  {ok, Client} = start_client(ClientId, ?HOSTS, ClientCfg),
  GetMetadataConn = fun() ->
    ?assertMatch({ok, _}, get_leader_connections(Client, <<"test-topic">>)),
    State = sys:get_state(Client),
    Pid = maps:get(metadata_conn, State),
    ?assert(is_process_alive(Pid)),
    Pid
  end,
  Pid1 = GetMetadataConn(),
  exit(Pid1, kill),
  Pid2 = GetMetadataConn(),
  ok = stop_client(Client),
  ?assertNot(is_process_alive(Pid2)).

send_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientCfg = client_config(),
  ClientId = <<"client-1">>,
  Topic = <<"test-topic">>,
  {ok, Client} = start_client(ClientId, ?HOSTS, ClientCfg),
  ?assertEqual(ok, wolff:check_if_topic_exists(ClientId, Topic)),
  ProducerCfg = #{partitioner => fun(_, _) -> 0 end},
  {ok, Producers} = wolff:start_producers(Client, Topic, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  Ref = make_ref(),
  Self = self(),
  AckFun = {fun ?MODULE:ack_cb/4, [Self, Ref]},
  _ = wolff:send(Producers, [Msg], AckFun),
  ?WAIT(5000, {ack, Ref, Partition, BaseOffset},
        io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
                  [Partition, BaseOffset])),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client),
  erlang:display(get_telemetry_seq(CntrEventsTable, [wolff, queuing])),
  erlang:display(ets:tab2list(CntrEventsTable)),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing])),
     [0,1,0]),
  ?assertMatch(
     [0,N,0] when N > 0,
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing_bytes]))
    ),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, inflight])),
     [0,1,0]),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff, success]),
  ets:delete(CntrEventsTable),
  deinstall_event_logging(?FUNCTION_NAME).

%% Test with single message which has the maximum payload below limit
%% it should be accepted by Kafka, otherwise message_too_large
send_one_msg_max_batch_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg = #{partitioner => fun(_, _) -> 0 end},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  EmptyBatchBytes = batch_bytes([#{key => <<>>, value => <<>>}]),
  VarintDelta = wolff_producer:varint_bytes(?MAX_BATCH_BYTES) -
                wolff_producer:varint_bytes(0),
  MaxValueBytes = ?MAX_BATCH_BYTES - EmptyBatchBytes -
                  VarintDelta*2, % *2 because the length is repeated
  Msg = #{key => <<>>,
          value => iolist_to_binary(lists:duplicate(MaxValueBytes, 0))
         },
  EstimatedBytes = batch_bytes([Msg]),
  ?assertEqual(EstimatedBytes, ?MAX_BATCH_BYTES),
  Ref = make_ref(),
  Self = self(),
  AckFun = fun(Partition, Offset) -> Self ! {ack, Ref, Partition, Offset}, ok end,
  _ = wolff:send(Producers, [Msg], AckFun),
  ?WAIT(5000, {ack, Ref, Partition, BaseOffset},
        io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
                  [Partition, BaseOffset])),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing])),
     [0,1,0]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing_bytes])),
     [0,EstimatedBytes,0]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, inflight])),
     [0,1,0]),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff, success]),
  ets:delete(CntrEventsTable),
  deinstall_event_logging(?FUNCTION_NAME).

%% Test with a batch with only one byte values, so the batch size could reach
%% maximum. Kafka should accept this batch otherwise the produce request will
%% fail with message_too_large error.
send_smallest_msg_max_batch_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg = #{partitioner => fun(_, _) -> 0 end},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => <<>>, value => <<"0">>},
  OneMsgBytes = batch_bytes([Msg]),
  Count = ?WOLFF_KAFKA_DEFAULT_MAX_MESSAGE_BYTES div OneMsgBytes,
  Batch = lists:duplicate(Count, Msg),
  io:format(user, "~nestimated_count=~p~n", [Count]),
  io:format(user, "estimated_bytes=~p~n", [batch_bytes(Batch)]),
  io:format(user, "  encoded_bytes=~p~n", [encoded_bytes(Batch)]),
  Ref = make_ref(),
  Self = self(),
  AckFun = fun(Partition, Offset) -> Self ! {ack, Ref, Partition, Offset}, ok end,
  _ = wolff:send(Producers, Batch, AckFun),
  ?WAIT(5000, {ack, Ref, Partition, BaseOffset},
       io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
                 [Partition, BaseOffset])),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client),
  %% note: we count requests/batches, not individual messages
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing])),
     [0,1,0]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing_bytes])),
     [0,batch_bytes(Batch),0]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, inflight])),
     [0,1,0]),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff, success]),
  ets:delete(CntrEventsTable),
  deinstall_event_logging(?FUNCTION_NAME).

send_sync_timeout_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg = #{partitioner => first_key_dispatch},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  ok = meck:new(kpro, [non_strict, no_history, no_link, passthrough]),
  ok = meck:expect(kpro, send, 2,
    fun(Conn, Req) ->
      timer:sleep(1000),
      meck:passthrough([Conn, Req])
    end),
  try
    ?assertError(timeout, wolff:send_sync(Producers, [Msg], 1)),
    {Partition, Offset} = wolff:send_sync(Producers, [Msg], 4000),
    ok = expect_stale_reply(Partition, Offset - 1),
    ok = wolff:stop_producers(Producers),
    ok = stop_client(Client),
    ets:delete(CntrEventsTable),
    deinstall_event_logging(?FUNCTION_NAME)
  after
    meck:unload(kpro)
  end.

expect_stale_reply(Partition, Offset) ->
    OTP = list_to_integer(erlang:system_info(otp_release)),
    receive
        {_, Partition, Offset} when OTP >= 26 ->
            error({unexpected_receive, Partition, Offset});
        {_, Partition, Offset} ->
            ok
    after
        10 ->
            case OTP >= 26 of
                true ->
                    ok;
                false ->
                    error({unexpected_timeout, Partition, Offset})
            end
    end.

send_sync_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg = #{partitioner => first_key_dispatch},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000),
  io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
            [Partition, BaseOffset]),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client),
  ets:delete(CntrEventsTable),
  deinstall_event_logging(?FUNCTION_NAME).

connection_restart_test_() ->
  {timeout, 10, fun() -> test_connection_restart() end}.

test_connection_restart() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientCfg0 = client_config(),
  ClientCfg = ClientCfg0#{min_metadata_refresh_interval => 0},
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg0 = producer_config(),
  %% allow message linger, so we have time to kill the connection
  ProducerCfg = ProducerCfg0#{max_linger_ms => 200,
                              reconnect_delay_ms => 0
                             },
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Ref = make_ref(),
  Self = self(),
  AckFun = fun(Partition, Offset) -> Self ! {ack, Ref, Partition, Offset}, ok end,
  Msg = #{key => ?KEY, value => <<"value">>},
  _ = wolff:send(Producers, [Msg], AckFun),
  Producer = wolff:get_producer(Producers, 0),
  #{conn := Conn} = sys:get_state(Producer),
  erlang:exit(Conn, kill),
  ?WAIT(5000, {ack, Ref, Partition, BaseOffset},
      io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
                [Partition, BaseOffset])),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client),
  %% telemetry events may depend on timing so we do not check them in this test
  %% case
  ets:delete(CntrEventsTable),
  deinstall_event_logging(?FUNCTION_NAME).

zero_ack_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg0 = producer_config(),
  ProducerCfg = ProducerCfg0#{required_acks => none},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000),
  io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
            [Partition, BaseOffset]),
  ?assertEqual(-1, BaseOffset),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing])),
     [0,1,0]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, inflight])),
     [0,1,0]),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff, success]),
  ets:delete(CntrEventsTable),
  deinstall_event_logging(?FUNCTION_NAME).

%% replayq overflow while inflight is not empty
replayq_overflow_while_inflight_test() ->
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  Msg = fun(Value) -> #{key => <<>>, value => Value} end,
  Batch = fun(Value) -> [Msg(Value), Msg(Value)] end,
  BatchSize = wolff_producer:batch_bytes(Batch(<<"testdata1">>)),
  ProducerCfg = #{max_batch_bytes => 1, %% ensure send one call at a time
                  replayq_max_total_bytes => BatchSize,
                  required_acks => all_isr,
                  max_send_ahead => 0 %% ensure one sent to kafka at a time
                 },
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Pid = wolff_producers:lookup_producer(Producers, 0),
  ?assert(is_process_alive(Pid)),
  TesterPid = self(),
  ok = meck:new(kpro, [no_history, no_link, passthrough]),
  meck:expect(kpro, send,
              fun(_Conn, Req) ->
                      Payload = iolist_to_binary(lists:last(tuple_to_list(Req))),
                      [_, <<N, _/binary>>] = binary:split(Payload, <<"testdata">>),
                      TesterPid ! {sent_to_kafka, <<"testdata", N>>},
                      ok
              end),
  AckFun1 = fun(_Partition, BaseOffset) -> TesterPid ! {ack_1, BaseOffset}, ok end,
  AckFun2 = fun(_Partition, BaseOffset) -> TesterPid ! {ack_2, BaseOffset}, ok end,
  AckFun3 = fun(_Partition, BaseOffset) -> TesterPid ! {ack_3, BaseOffset}, ok end,
  SendF = fun(AckFun, Value) -> wolff:send(Producers, Batch(Value), AckFun) end,
  %% send 3 batches first will be inflight, 2nd is overflow (kicked out by the 3rd)
  proc_lib:spawn_link(fun() -> SendF(AckFun1, <<"testdata1">>) end),
  proc_lib:spawn_link(fun() -> timer:sleep(5), SendF(AckFun2, <<"testdata2">>) end),
  proc_lib:spawn_link(fun() -> timer:sleep(10), SendF(AckFun3, <<"testdata3">>) end),
  try
    %% the 1st batch is sent to Kafka, but no reply, so no ack
    receive
      {sent_to_kafka, <<"testdata1">>} -> ok;
      {ack_1, _} -> error(unexpected)
    after
        2000 ->
            error(timeout)
    end,
    %% the 2nd batch should be dropped (pushed out by the 3rd)
    receive
      {ack_2, buffer_overflow_discarded} -> ok;
      {sent_to_kafka, <<"testdata2">>} -> error(unexpected);
      {sent_to_kafka, Offset2} -> error({unexpected, Offset2})
    after
        2000 ->
            io:format(user, "~p~n", [sys:get_state(Pid)]),
            error(timeout)
    end,
    %% the 3rd batch should stay in the queue because max_send_ahead is 0
    receive
      {ack_3, Any} -> error({unexpected, Any})
    after
        100 ->
            ok
    end
  after
    meck:unload(kpro),
    ok = wolff:stop_producers(Producers),
    ok = stop_client(Client)
  end.

%% replayq overflow while inflight is not empty and resend after Kafka is connected again
replayq_overflow_while_disconnected_test() ->
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  Msg = fun(Value) -> #{key => <<>>, value => Value} end,
  Batch = fun(Value) -> [Msg(Value), Msg(Value)] end,
  BatchSize = wolff_producer:batch_bytes(Batch(<<"testdata1">>)),
  ProducerCfg = #{max_batch_bytes => 1, %% ensure send one call at a time
                  replayq_max_total_bytes => BatchSize,
                  required_acks => all_isr,
                  max_send_ahead => 1, %% allow 2 inflight requests
                  reconnect_delay_ms => 0
                 },
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Pid = wolff_producers:lookup_producer(Producers, 0),
  #{conn := Conn} = St = sys:get_state(Pid),
  ?assert(is_pid(Conn), {Pid, St}),
  ?assert(is_process_alive(Pid)),
  TesterPid = self(),
  ok = meck:new(kpro, [no_history, no_link, passthrough]),
  meck:expect(kpro, send,
              fun(_Conn, Req) ->
                      Payload = iolist_to_binary(lists:last(tuple_to_list(Req))),
                      [_, <<N, _/binary>>] = binary:split(Payload, <<"testdata">>),
                      TesterPid ! {sent_to_kafka, <<"testdata", N>>},
                      ok
              end),
  AckFun = fun(_Partition, BaseOffset) -> TesterPid ! {ack, BaseOffset}, ok end,
  SendF = fun(AckFun0, Value) -> wolff:send(Producers, Batch(Value), AckFun0) end,
  %% send 4 batches first 2 will be inflight, 3nd is overflow (kicked out by the 4th)
  proc_lib:spawn_link(fun() -> SendF(AckFun, <<"testdata1">>) end),
  proc_lib:spawn_link(fun() -> timer:sleep(5), SendF(AckFun, <<"testdata2">>) end),
  proc_lib:spawn_link(fun() -> timer:sleep(10), SendF(AckFun, <<"testdata3">>) end),
  proc_lib:spawn_link(fun() -> timer:sleep(15), SendF(AckFun, <<"testdata4">>) end),
  ExpectSent =
    fun(Payload) ->
      receive
        {sent_to_kafka, Payload} -> ok
      after
        2000 ->
            error(timeout)
      end
    end,
  ExpectAck =
    fun(Offset) ->
      receive
        {ack, Offset} -> ok
      after
        2000 -> error(timeout)
      end
    end,
  try
    %% the 1st batch is sent to Kafka, but no reply, so no ack
    ok = ExpectSent(<<"testdata1">>),
    ok = ExpectSent(<<"testdata2">>),
    %% kill the connection, producer should trigger a reconnect
    exit(Conn, kill),
    %% the 3rd batch should be dropped (pushed out by the 4th)
    ok = ExpectAck(buffer_overflow_discarded),
    %% after reconnected, expect a resend of first 2 messages to Kafka
    ok = ExpectSent(<<"testdata1">>),
    ok = ExpectSent(<<"testdata2">>),
    %% the 4th batch should stay in the queue because max_send_ahead is 1 (max-inflight=2)
    receive
      {sent_to_kafka, <<"testdata4">>} -> error(unexpected);
      {ack, _} = Ack -> error({unexpected, Ack})
    after
        100 ->
            ok
    end,
    %% fake a Kafka produce response for the 1st message
    ok = mock_kafka_ack(Pid, 1),
    %% expect the ack for the 1st message
    ok = ExpectAck(1),
    %% now expect the 4th message to be sent
    ok = ExpectSent(<<"testdata4">>)
  after
    meck:unload(kpro),
    ok = wolff:stop_producers(Producers),
    ok = stop_client(Client)
  end.

mock_kafka_ack(Pid, Offset) ->
    #{conn := NewConn, sent_reqs := SentReqs} = sys:get_state(Pid),
    {value, #{req_ref := ReqRef}} = queue:peek(SentReqs),
    RspBody =
      #{responses =>
        [#{topic => <<"test-topic">>,
           partition_responses =>
           [#{partition => 0,
              error_code => no_error,
              base_offset => Offset,
              error_message => <<>>,
              log_start_offset => 0,
              log_append_time => -1,
              record_errors => []}
           ]}]},
    Rsp = #kpro_rsp{api = produce,
                    ref = ReqRef,
                    msg = RspBody},
    Pid ! {msg, NewConn, Rsp},
    ok.

replayq_overflow_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  Msg = #{key => <<>>, value => <<"12345">>},
  Batch = [Msg, Msg],
  BatchSize = wolff_producer:batch_bytes(Batch),
  LingerMs = 100,
  ProducerCfg = #{max_batch_bytes => 1, %% ensure send one call at a time
                  replayq_max_total_bytes => BatchSize,
                  required_acks => all_isr,
                  max_linger_ms => LingerMs, %% delay enqueue
                  max_linger_bytes => BatchSize + 1 %% delay enqueue
                 },
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Pid = wolff_producers:lookup_producer(Producers, 0),
  ?assert(is_process_alive(Pid)),
  TesterPid = self(),
  AckFun1 = fun(_Partition, BaseOffset) -> TesterPid ! {ack_1, BaseOffset}, ok end,
  AckFun2 = fun(_Partition, BaseOffset) -> TesterPid ! {ack_2, BaseOffset}, ok end,
  SendF = fun(AckFun) -> wolff:send(Producers, Batch, AckFun) end,
  %% send two batches to overflow one
  proc_lib:spawn_link(fun() -> SendF(AckFun1) end),
  timer:sleep(1), %% ensure order
  proc_lib:spawn_link(fun() -> SendF(AckFun2) end),
  timer:sleep(LingerMs * 2),
  try
    %% pushed out of replayq due to overflow
    receive
      {ack_1, buffer_overflow_discarded} -> ok;
      {ack_1, Other} -> error({"unexpected_ack", Other})
    after
        2000 ->
            error(timeout)
    end,
    %% the second batch should eventually get the ack
    receive
      {ack_2, buffer_overflow_discarded} -> error("unexpected_discard");
      {ack_2, BaseOffset} -> ?assert(BaseOffset >= 0)
    after
        2000 ->
            error(timeout)
    end
  after
    ok = wolff:stop_producers(Producers),
    ok = stop_client(Client)
  end,
  [1] = get_telemetry_seq(CntrEventsTable, [wolff, dropped]),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff, dropped_queue_full]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing])),
     [0,2,1,0]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing_bytes])),
     [0,64,32,0]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, inflight])),
     [0,1,0]),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff, success]),
  ets:delete(CntrEventsTable),
  deinstall_event_logging(?FUNCTION_NAME).

replayq_highmem_overflow_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  load_ctl:put_config(#{?MEM_MON_F1 => 0.0}),
  application:ensure_all_started(lc),
  timer:sleep(2000),
  true = load_ctl:is_high_mem(),
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  Msg = #{key => <<>>, value => <<"12345">>},
  Batch = [Msg, Msg],
  BatchSize = wolff_producer:batch_bytes(Batch),
  ProducerCfg = #{max_batch_bytes => 1, %% make sure not collecting calls into one batch
                  replayq_max_total_bytes => BatchSize*1000,
                  required_acks => all_isr,
                  max_linger_ms => 1000, %% do not send to kafka immediately
                  drop_if_highmem => true
                 },
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  TesterPid = self(),
  AckFun1 = fun(_Partition, BaseOffset) -> TesterPid ! {ack_1, BaseOffset}, ok end,
  AckFun2 = fun(_Partition, BaseOffset) -> TesterPid ! {ack_2, BaseOffset}, ok end,
  AckFun3 = fun(_Partition, BaseOffset) -> TesterPid ! {ack_3, BaseOffset}, ok end,
  SendF = fun(AckFun) -> wolff:send(Producers, Batch, AckFun) end,
  %% send two batches to overflow one
  spawn(fun() -> SendF(AckFun1) end),
  timer:sleep(1), %% ensure order
  spawn(fun() -> SendF(AckFun2) end),
  try
    %% pushed out of replayq due to overflow
    receive
      {ack_1, buffer_overflow_discarded} -> ok;
      {ack_1, Other0} -> error({"unexpected_ack", Other0})
    after
        2000 ->
            error(timeout)
    end,
    %% the second batch is discarded as well
    receive
      {ack_2, buffer_overflow_discarded} -> ok;
      {ack_2, Other1} -> error({"unexpected_ack", Other1})
    after
        2000 ->
            error(timeout)
    end,
    application:stop(lc),
    false = load_ctl:is_high_mem(),
    timer:sleep(1),
    spawn(fun() -> SendF(AckFun3) end),
    %% the second batch should eventually get the ack
    receive
      {ack_3, buffer_overflow_discarded} -> error("unexpected_discard");
      {ack_3, BaseOffset} -> ?assert(BaseOffset >= 0)
    after
        2000 ->
            error(timeout)
    end
  after
    ok = wolff:stop_producers(Producers),
    ok = stop_client(Client)
  end,
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing])),
     [0, 1, 0, 1, 0, 1, 0]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing_bytes])),
     [0,32,0,32,0,32,0]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, inflight])),
     [0,1,0]),
  [1, 1] = show( get_telemetry_seq(CntrEventsTable, [wolff, dropped]) ),
  [1, 1] = show( get_telemetry_seq(CntrEventsTable, [wolff, dropped_queue_full]) ),
  [1] = show( get_telemetry_seq(CntrEventsTable, [wolff, success]) ),
  ets:delete(CntrEventsTable),
  deinstall_event_logging(?FUNCTION_NAME).

mem_only_replayq_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg = #{},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000),
  io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
            [Partition, BaseOffset]),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing])),
     [0, 1, 0]),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, inflight])),
     [0, 1, 0]),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff, success]),
  ets:delete(CntrEventsTable),
  deinstall_event_logging(?FUNCTION_NAME).

recover_from_replayq_test() ->
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-2">>, ?HOSTS, ClientCfg),
  ProducerCfg = #{replayq_dir => "test-data-2"},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {0, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000),
  ProducerPid = wolff_producers:lookup_producer(Producers, 0),
  #{conn := Conn} = sys:get_state(ProducerPid),
  %% suspend the connection process so to make sure the messages will remain in the replayq
  sys:suspend(Conn),
  %% do not expect any ack.
  AckFun = fun(_Partition, _BaseOffset) -> error(unexpected) end,
  N = 10,
  L = lists:seq(1, 10),
  lists:foreach(fun(_) -> wolff:cast(Producers, [Msg], AckFun) end, L),
  %% ensure the messages are in replayq
  ProducerPid ! linger_expire,
  #{pending_acks := Acks} = sys:get_state(ProducerPid),
  ?assertEqual(N, wolff_pendack:count(Acks)),
  %% kill the processes
  unlink(ProducerPid),
  ok = wolff_producer:stop(ProducerPid),
  exit(Conn, kill),
  %% restart the producers
  {ok, Producers1} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  ProducerPid1 = wolff_producers:lookup_producer(Producers1, 0),
  retry(fun() ->
    #{pending_acks := Acks1, replayq := Q} = sys:get_state(ProducerPid1),
    ?assertEqual(0, wolff_pendack:count(Acks1)),
    ?assertEqual(0, replayq:count(Q))
  end, 3000),
  {0, BaseOffset1} = wolff:send_sync(Producers1, [Msg], 2000),
  ?assertEqual(BaseOffset + N + 1, BaseOffset1),
  ok = wolff:stop_producers(Producers1),
  ok = stop_client(Client).

retry(_F, T) when T < 0 ->
   error(retry_failed);
retry(F, T) ->
  try
    F()
  catch _:_ ->
    timer:sleep(100),
    retry(F, T - 100)
  end.

replayq_offload_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg0 = producer_config(),
  ProducerCfg = ProducerCfg0#{replayq_offload_mode => true},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000),
  io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
            [Partition, BaseOffset]),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing])),
     [0, 1, 0]),
  ?assertMatch(
     [0,N,0] when N > 0,
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, queuing_bytes]))
    ),
  ?assert_eq_optional_tail(
     wolff_test_utils:dedup_list(get_telemetry_seq(CntrEventsTable, [wolff, inflight])),
     [0, 1, 0]),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff, success]),
  ets:delete(CntrEventsTable),
  deinstall_event_logging(?FUNCTION_NAME).

check_connectivity_test() ->
  ClientId = <<"client-connectivity-test">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = client_config(),
  {ok, Client} = start_client(ClientId, ?HOSTS, ClientCfg),
  ?assertEqual(ok, wolff:check_connectivity(ClientId)),
  ?assertEqual(ok, wolff:check_if_topic_exists(?HOSTS, ClientCfg, <<"test-topic">>)),
  ?assertError(function_clause, wolff:check_connectivity([], ClientCfg)),
  ?assertEqual(
    {error, unknown_topic_or_partition},
    wolff:check_if_topic_exists(?HOSTS, ClientCfg, <<"unknown-topic">>)
  ),
  ?assertError(function_clause, wolff:check_if_topic_exists([], ClientCfg, <<"unknown-topic">>)),
  ok = stop_client(Client),
  ?assertMatch({error, #{reason := no_such_client}}, wolff:check_connectivity(ClientId)),
  ok = application:stop(wolff).

client_state_upgrade_test() ->
  ClientId = <<"client-state-upgrade-test">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = client_config(),
  {ok, Client} = start_client(ClientId, ?HOSTS, ClientCfg),
  ?assertEqual(ok, wolff:check_connectivity(ClientId)),
  timer:sleep(500),
  ok = verify_state_upgrade(Client, fun() -> _ = wolff_client:get_id(Client) end),
  ok = verify_state_upgrade(Client, fun() -> Client ! ignore end),
  ok = verify_state_upgrade(Client, fun() -> gen_server:cast(Client, ignore) end),
  ok = stop_client(Client),
  ?assertMatch({error, #{reason := no_such_client}}, wolff:check_connectivity(ClientId)),
  ok = application:stop(wolff).

fail_to_connect_all_test_() ->
  {timeout, 30,
   fun() -> test_fail_to_connect_all() end}.

test_fail_to_connect_all() ->
  ClientId = <<"fail-to-connect-test">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = #{connect_timeout => 500},
  Hosts = [{localhost, 9999},
           {<<"localhost">>, 9999},
           {{1, 2, 3, 4}, 9999}, %% timeout
           {"127.0.0", 9999} %% invalid ip
          ],
  {ok, Client} = start_client(ClientId, Hosts, ClientCfg),
  ?assertEqual({error, failed_to_fetch_metadata},
               get_leader_connections(Client, <<"test-topic">>)),
  Refuse = #{host => <<"localhost:9999">>, reason => connection_refused},
  {error, Errors} = wolff:check_connectivity(ClientId),
  ?assertMatch([#{host := <<"1.2.3.4:9999">>, reason := _},
                #{host := <<"127.0.0:9999">>, reason := _},
                Refuse, Refuse
                ],
               lists:sort(Errors)),
  ok = application:stop(wolff).

show(X) ->
    erlang:display(X),
    X.

leader_restart_test_() ->
  {timeout, 90,
   fun() -> test_leader_restart() end}.

test_leader_restart() ->
  Topic = "testtopic" ++ integer_to_list(abs(erlang:monotonic_time())),
  %% number of partitions should be greater than number of Kafka brokers
  %% to ensure there is at least one leader in kafka-2
  Partitions = 5,
  %% replication factor has to be 1 to trigger leader_not_available error code
  ReplicationFactor = 1,
  with_topic(Topic, Partitions, ReplicationFactor, default_max_message_bytes, fun() ->
    ClientCfg = client_config(),
    ClientIdA = iolist_to_binary("ca-" ++ Topic),
    ClientIdB = iolist_to_binary("cb-" ++ Topic),
    {ok, ClientA} = start_client(ClientIdA, ?HOSTS, ClientCfg#{connection_strategy => per_partition}),
    {ok, ClientB} = start_client(ClientIdB, ?HOSTS, ClientCfg#{connection_strategy => per_broker}),
    TopicBin = iolist_to_binary(Topic),
    {ok, LeadersA0} = get_leader_connections(ClientA, TopicBin),
    {ok, LeadersB0} = get_leader_connections(ClientB, TopicBin),
    %% Restart kafka-2, but not kakfa-1, because kafka-1 is the cluster controller
    %% when Kafka version >= 3
    ok = stop_kafka_2(),
    {ok, LeadersA1} = get_leader_connections(ClientA, TopicBin),
    {ok, LeadersB1} = get_leader_connections(ClientB, TopicBin),
    ok = start_kafka_2(),
    %% expect the number of leaders are the same even though one broker is down
    ?assert(length(LeadersA0) =:= length(LeadersA1)),
    ?assert(length(LeadersB0) =:= length(LeadersB1)),
    ok = wolff_client:stop(ClientA),
    ok = wolff_client:stop(ClientB)
  end).

with_topic(Topic, Partitions, ReplicationFactor, MaxMessageBytes, TestFunc) ->
  ok = create_topic(Topic, Partitions, ReplicationFactor, MaxMessageBytes),
  try
    _ = application:stop(wolff), %% ensure stopped
    {ok, _} = application:ensure_all_started(wolff),
    TestFunc()
  after
    ok = delete_topic(Topic)
  end.


message_too_large_test_() ->
  {timeout, 60,
   fun() -> test_message_too_large() end}.

test_message_too_large() ->
  Topic = "message-too-large-" ++ integer_to_list(abs(erlang:monotonic_time())),
  Partitions = 1,
  ReplicationFactor = 1,
  MaxMessageBytes = 100,
  with_topic(Topic, Partitions, ReplicationFactor, MaxMessageBytes, fun() ->
    ClientCfg = client_config(),
    ClientId = iolist_to_binary("client-" ++ Topic),
    {ok, Client} = start_client(ClientId, ?HOSTS, ClientCfg#{connection_strategy => per_partition}),
    TopicBin = iolist_to_binary(Topic),
    %% try to batch more messages than Kafka's limit,
    %% the producer will get message_too_large error back
    %% then it should retry sending one message at a time
    ProducerCfg = #{partitioner => fun(_, _) -> 0 end,
                    max_batch_bytes => MaxMessageBytes * 3,
                    %% ensure batching by delay enqueue by 100 seconds
                    max_linger_ms => 100,
                    %% ensure linger is not expired by reaching size
                    max_linger_bytes => MaxMessageBytes * 100
                   },
    {ok, Producers} = wolff:start_producers(Client, TopicBin, ProducerCfg),
    MaxBytesCompensateOverhead = MaxMessageBytes - ?BATCHING_OVERHEAD - 7,
    Msg = fun(C) -> #{key => <<>>, value => iolist_to_binary(lists:duplicate(MaxBytesCompensateOverhead, C))} end,
    SendFunc = fun(Batch) ->
      Ref = make_ref(),
      Self = self(),
      AckFun = {fun ?MODULE:ack_cb/4, [Self, Ref]},
      {_, _} = wolff:cast(Producers, Batch, AckFun),
      fun() -> ?WAIT(5000, {ack, Ref, _Partition, BaseOffset}, BaseOffset) end
    end,
    %% Must be ok to send one message
    ?assertEqual(0, (SendFunc([Msg($0)]))()),
    %% The following 3 calls will be collected into one batch
    %% then split into 3 batches for retry.
    Wait1 = SendFunc([Msg($a)]),
    Wait2 = SendFunc([Msg($b)]),
    Wait3 = SendFunc([Msg($c)]),
    ?assertEqual(1, Wait1()),
    ?assertEqual(2, Wait2()),
    ?assertEqual(3, Wait3()),
    ProducerPid = wolff_producers:lookup_producer(Producers, 0),
    %% assert producer lowered max_batch_bytes to half
    ?assertMatch(#{config := #{max_batch_bytes := 150}}, sys:get_state(ProducerPid)),
    %% send a too-large single message, producer is forced to drop it
    ?assertEqual(message_too_large, (SendFunc([Msg(<<"0123456789">>)]))()),
    ok = wolff:stop_producers(Producers),
    ok = stop_client(Client),
    ok = application:stop(wolff)
  end).

one_byte_limit_test_() ->
  {timeout, 10, fun one_byte_limit/0}.

one_byte_limit() ->
  Topic = "one-byte-limit-" ++ integer_to_list(abs(erlang:monotonic_time())),
  Partitions = 1,
  ReplicationFactor = 1,
  with_topic(Topic, Partitions, ReplicationFactor, _MessageSizeLimit = 1, fun() ->
    ClientCfg = client_config(),
    ClientId = iolist_to_binary("client-" ++ Topic),
    {ok, Client} = start_client(ClientId, ?HOSTS, ClientCfg#{connection_strategy => per_partition}),
    TopicBin = iolist_to_binary(Topic),
    %% try to batch more messages than Kafka's limit,
    %% the producer will get message_too_large error back
    %% then it should retry sending one message at a time
    ProducerCfg = #{partitioner => fun(_, _) -> 0 end, max_batch_bytes => 1},
    {ok, Producers} = wolff:start_producers(Client, TopicBin, ProducerCfg),
    SendFunc = fun(Batch) ->
      Ref = make_ref(),
      Self = self(),
      AckFun = {fun ?MODULE:ack_cb/4, [Self, Ref]},
      _ = wolff:send(Producers, Batch, AckFun),
      ?WAIT(5000, {ack, Ref, _Partition, BaseOffset}, BaseOffset)
    end,
    %% send batch of two one-byte messages, producer should not enter
    %% a dead-loop of repeating NewMax = (1+1) div 2
    Msg = #{key => <<>>, value => <<0>>},
    Batch = [Msg, Msg],
    ?assertEqual(message_too_large, SendFunc(Batch)),
    ok = wolff:stop_producers(Producers),
    ok = stop_client(Client),
    ok = application:stop(wolff)
  end).

%% wolff_client died by the time when wolff_producers tries to initialize producers
%% it should not cause the wolff_producers_sup to restart
producers_init_failure_test_() ->
  {timeout, 10, fun() -> test_producers_init_failure() end}.

test_producers_init_failure() ->
   _ = application:stop(wolff),
  {ok, _} = application:ensure_all_started(wolff),
  ClientId = <<"proucers-init-failure-test">>,
  Topic = <<"test-topic">>,
  ClientCfg = #{connection_strategy => per_broker},
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ?assertEqual({error, {unknown_call, foo}}, gen_server:call(ClientPid, foo)),
  %% suspend the client so it will not respond to calls
  sys:suspend(ClientPid),
  ProducerCfg = #{required_acks => all_isr,
                  partition_count_refresh_interval_seconds => 0
                 },
  %% this call will hang until the client pid is killed later
  {TmpPid, _} = spawn_monitor(fun() -> {error, {killed, _}} = wolff_producers:start_linked_producers(ClientId, Topic, ProducerCfg), exit(normal) end),
  %% wait a bit to ensure the spanwed process gets the chance to run
  timer:sleep(100),
  %% kill the client pid, so the gen_server:call towards the client will fail
  exit(ClientPid, kill),
  receive
    {'DOWN', _, process, TmpPid, Reason} ->
      ?assertEqual(normal, Reason)
  after 2000 ->
    error(timeout)
  end,
  %% cleanup
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = application:stop(wolff),
  ok.

%% helpers

%% verify wolff_client state upgrade.
%% 1. replace the state with old version
%% 2. assert the replace worked
%% 3. trigger some activity to the wolff_client pid
%% 4. check if the state is upgraded to new version
verify_state_upgrade(Client, F) ->
  sys:replace_state(Client, fun(St) -> to_old_client_state(St) end),
  ?assertMatch(#{connect := ConnFun} when is_function(ConnFun), sys:get_state(Client)),
  _ = F(), %% trigger a handle_call, handle_info or handle_cast which in turn triggers state upgrade
  ?assertMatch(#{conn_config := _}, sys:get_state(Client)),
  ok.

to_old_client_state(St0) ->
  St = maps:without([conn_config], St0),
  St#{connect => fun() -> error(unexpected) end}.

client_config() -> #{}.

producer_config() ->
  #{replayq_dir => "test-data"}.

key(Name) ->
  iolist_to_binary(io_lib:format("~p/~p", [Name, calendar:local_time()])).

start_client(ClientId, Hosts, Config) ->
  {ok, _} = application:ensure_all_started(wolff),
  wolff:ensure_supervised_client(ClientId, Hosts, Config).

stop_client(ClientId) when is_binary(ClientId) ->
  wolff:stop_and_delete_supervised_client(ClientId);
stop_client(Pid) when is_pid(Pid) ->
  Id = wolff_client:get_id(Pid),
  stop_client(Id).

batch_bytes(Batch) ->
  wolff_producer:batch_bytes(Batch).

encoded_bytes(Batch) ->
  Encoded = kpro_batch:encode(2, Batch, no_compression),
  iolist_size(Encoded).

create_topic(Topic, Partitions, ReplicationFactor, MaxMessageBytes) ->
  Cmd = create_topic_cmd(Topic, Partitions, ReplicationFactor, MaxMessageBytes),
  Result = os:cmd(Cmd),
  Pattern = "Created topic ",
  ?assert(string:str(Result, Pattern) > 0, Result),
  ok.

delete_topic(Topic) ->
  wolff_test_utils:delete_topic(Topic).

create_topic_cmd(Topic, Partitions, ReplicationFactor, MaxMessageBytes) ->
  wolff_test_utils:topics_cmd_base(Topic) ++
  " --create" ++
  " --partitions " ++ integer_to_list(Partitions) ++
  " --replication-factor " ++ integer_to_list(ReplicationFactor) ++
  case is_integer(MaxMessageBytes) of
    true ->
      " --config max.message.bytes=" ++ integer_to_list(MaxMessageBytes);
    false ->
      ""
  end.

stop_kafka_2() ->
  Cmd = "docker stop kafka-2",
  os:cmd(Cmd),
  ok.

start_kafka_2() ->
  Cmd = "docker start kafka-2",
  os:cmd(Cmd),
  ok = wait_for_kafka(60),
  ok.

wait_for_kafka(0) ->
  error(timeout);
wait_for_kafka(Limit) ->
  case wolff:check_connectivity([{"localhost", 9192}], client_config()) of
    ok ->
      ok;
    {error, _Reason} ->
      timer:sleep(1000),
      wait_for_kafka(Limit - 1)
  end.

%% Helper function that is useful when one wants to get code to check that the
%% telemetry events are triggered correctly in a test case
print_telemetry_check(Tab, Name) ->
    erlang:display({'[__test_code_gen__]',Name}),
    DispFlat = fun(X) -> erlang:display(erlang:binary_to_list(erlang:iolist_to_binary(X))) end,
    lists:foreach(fun({Id, _List}) ->
                          DispFlat(io_lib:format("~p = wolff_tests:get_telemetry_seq(CntrEventsTable, ~p),", [wolff_tests:get_telemetry_seq(Tab,Id), Id]))
                  end, ets:tab2list(Tab)).

get_telemetry_seq(Table, EventId) ->
    case ets:lookup(Table, EventId) of
       [] -> [];
       [{_, Events}] ->
            lists:reverse(
              [case Data of
                   #{counter_inc := Val} ->
                       Val;
                   #{gauge_set := Val} ->
                       Val;
                   #{gauge_shift := Val} ->
                       Val
               end
               || #{metrics_data := Data} <- Events])
    end.

handle_telemetry_event(
    EventId,
    MetricsData,
    MetaData,
    #{record_table := EventRecordTable,
      log_events := LogEvents}
) ->
    case EventRecordTable =/= none of
        true ->
            do_handle_telemetry_event(EventRecordTable, EventId, MetricsData, MetaData);
        false ->
            ok
    end,
    case LogEvents of
        true ->
            ct:pal("<<< telemetry event >>>\n[event id]: ~p\n[metrics data]: ~p\n[meta data]: ~p\n",
                   [EventId, MetricsData, MetaData]);
        false ->
            ok
    end.

do_handle_telemetry_event(EventRecordTable, EventId, MetricsData, MetaData) ->
    try
        PastEvents = case ets:lookup(EventRecordTable, EventId) of
                         [] -> [];
                         [{_EventId, PE}] -> PE
                     end,
        NewEventList = [ #{metrics_data => MetricsData,
                           meta_data => MetaData} | PastEvents],
        ets:insert(EventRecordTable, {EventId, NewEventList})
    catch
        error:badarg:Stacktrace ->
            ct:pal("<<< error handling telemetry event >>>\n[event id]: ~p\n[metrics data]: ~p\n[meta data]: ~p\n\nStacktrace:\n  ~p\n",
                   [EventId, MetricsData, MetaData, Stacktrace])
    end.

telemetry_id() ->
    <<"emqx-bridge-kafka-producer-telemetry-handler">>.

%% Just log events but do not record
install_event_logging(TestCaseName) ->
    install_event_logging(TestCaseName, none, true).

install_event_logging(TestCaseName, EventRecordTable, LogEvents) ->
    case LogEvents of
        true ->
            ct:pal("=== Starting event logging for test case ~p ===\n", [TestCaseName]);
        false ->
            ok
    end,
    ok = application:ensure_started(telemetry),
    %% Attach event handlers for Kafka telemetry events. If a handler with the
    %% handler id already exists, the attach_many function does nothing
    telemetry:attach_many(
        %% unique handler id
        telemetry_id(),
        telemetry_events(),
        fun wolff_tests:handle_telemetry_event/4,
        #{record_table => EventRecordTable,
          log_events => LogEvents}
    ),
    timer:sleep(100),
    ok.

telemetry_events() ->
    [
      [wolff, dropped],
      [wolff, dropped_queue_full],
      [wolff, matched],
      [wolff, queuing],
      [wolff, queuing_bytes],
      [wolff, retried],
      [wolff, failed],
      [wolff, inflight],
      [wolff, retried_failed],
      [wolff, retried_success],
      [wolff, success]
    ].

deinstall_event_logging(TestCaseName) ->
    telemetry:detach(telemetry_id()),
    ct:pal("=== Stopping event logging for test case ~p ===\n", [TestCaseName]).

assert_eq_optional_tail(Fun, ExpectedList) ->
    ExpectedListMinusLast = lists:sublist(ExpectedList, length(ExpectedList) - 1),
    case Fun() of
        ExpectedList ->
            ok;
        ExpectedListMinusLast ->
            %% sometimes the terminate callback seems to not have time
            %% to insert into the ets, making this flaky.
            ok;
        Xs0 ->
            ct:fail("unexpected result: ~p", [Xs0])
    end.

get_leader_connections(Client, Topic) ->
    wolff_client:get_leader_connections(Client, ?NO_GROUP, Topic).
