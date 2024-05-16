-module(wolff_tests).

-export([ack_cb/4]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("lc/include/lc.hrl").
-include("wolff.hrl").

-define(KEY, key(?FUNCTION_NAME)).
-define(HOSTS, [{"localhost", 9092}]).

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

metadata_connection_restart_test() ->
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

metadata_connection_restart2_test() ->
  ClientCfg0 = client_config(),
  ClientCfg = ClientCfg0#{min_metadata_refresh_interval => 0},
  ClientId = <<"client-1">>,
  {ok, Client} = start_client(ClientId, ?HOSTS, ClientCfg),
  GetMetadataConn = fun() ->
    ?assertMatch({ok, _}, wolff_client:get_leader_connections(Client, <<"test-topic">>)),
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
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg = #{partitioner => fun(_, _) -> 0 end},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  Ref = make_ref(),
  Self = self(),
  AckFun = {fun ?MODULE:ack_cb/4, [Self, Ref]},
  _ = wolff:send(Producers, [Msg], AckFun),
  ?WAIT(5000, {ack, Ref, Partition, BaseOffset},
        io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
                  [Partition, BaseOffset])),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client).

%% Test with single message which has the maximum payload below limit
%% it should be accepted by Kafka, otherwise message_too_large
send_one_msg_max_batch_test() ->
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
  ok = stop_client(Client).

%% Test with a batch with only one byte values, so the batch size could reach
%% maximum. Kafka should accept this batch otherwise the produce request will
%% fail with message_too_large error.
send_smallest_msg_max_batch_test() ->
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg = #{partitioner => fun(_, _) -> 0 end},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => <<>>, value => <<"0">>},
  OneMsgBytes = batch_bytes([Msg]),
  Count = ?WOLFF_KAFKA_DEFAULT_MAX_MESSAGE_BYTES div OneMsgBytes,
  io:format(user, "~nestimated_count=~p~n", [Count]),
  Batch = lists:duplicate(Count, Msg),
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
  ok = stop_client(Client).

send_sync_test() ->
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg = #{partitioner => first_key_dispatch},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000),
  io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
            [Partition, BaseOffset]),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client).

connection_restart_test_() ->
  {timeout, 10, fun() -> test_connection_restart() end}.

test_connection_restart() ->
  ClientCfg0 = client_config(),
  ClientCfg = ClientCfg0#{min_metadata_refresh_interval => 0},
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg0 = producer_config(),
  %% allow message linger, so we have time to kill the connection
  ProducerCfg = ProducerCfg0#{max_linger_ms => 200,
                              partitioner => roundrobin,
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
  ok = stop_client(Client).

zero_ack_test() ->
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
  ok = stop_client(Client).

replayq_overflow_test() ->
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  Msg = #{key => <<>>, value => <<"12345">>},
  Batch = [Msg, Msg],
  BatchSize = wolff_producer:batch_bytes(Batch),
  ProducerCfg = #{max_batch_bytes => 1, %% make sure not collecting calls into one batch
                  replayq_max_total_bytes => BatchSize,
                  required_acks => all_isr,
                  max_linger_ms => 1000 %% do not send to kafka immediately
                 },
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  TesterPid = self(),
  AckFun1 = fun(_Partition, BaseOffset) -> TesterPid ! {ack_1, BaseOffset}, ok end,
  AckFun2 = fun(_Partition, BaseOffset) -> TesterPid ! {ack_2, BaseOffset}, ok end,
  SendF = fun(AckFun) -> wolff:send(Producers, Batch, AckFun) end,
  %% send two batches to overflow one
  spawn(fun() -> SendF(AckFun1) end),
  timer:sleep(1), %% ensure order
  spawn(fun() -> SendF(AckFun2) end),
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
  end.

replayq_highmem_overflow_test() ->
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
  end.

mem_only_replayq_test() ->
  ClientCfg = client_config(),
  {ok, Client} = start_client(<<"client-1">>, ?HOSTS, ClientCfg),
  ProducerCfg = #{},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000),
  io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
            [Partition, BaseOffset]),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client).

replayq_offload_test() ->
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
  ok = stop_client(Client).

stats_test() ->
  ClientId = <<"client-stats-test">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ?assertMatch(#{send_oct := 0, send_cnt := 0, recv_cnt := 0, recv_oct := 0},
               wolff_stats:getstat()),
  ?assertMatch(#{send_oct := 0, send_cnt := 0, recv_cnt := 0, recv_oct := 0},
               wolff_stats:getstat(ClientId, <<"nonexisting-topic">>, 0)),
  ClientCfg = client_config(),
  {ok, Client} = start_client(ClientId, ?HOSTS, ClientCfg),
  ProducerCfg0 = producer_config(),
  ProducerCfg = ProducerCfg0#{required_acks => leader_only},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000),
  io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
            [Partition, BaseOffset]),
  ?assertMatch(#{send_oct := O, send_cnt := C,
                 recv_oct := O, recv_cnt := C} when O > 0 andalso C > 0,
               wolff_stats:getstat()),
  ?assertMatch(#{send_oct := O, send_cnt := C,
                 recv_oct := O, recv_cnt := C} when O > 0 andalso C > 0,
               wolff_stats:getstat(ClientId, <<"test-topic">>, Partition)),
  ok = wolff:stop_producers(Producers),
  ok = stop_client(Client),
  ok = application:stop(wolff),
  ?assertEqual(undefined, whereis(wolff_sup)),
  ?assertEqual(undefined, whereis(wolff_stats)),
  ok.

check_connectivity_test() ->
  ClientId = <<"client-stats-test">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = client_config(),
  {ok, Client} = start_client(ClientId, ?HOSTS, ClientCfg),
  ?assertEqual(ok, wolff:check_connectivity(ClientId)),
  ok = stop_client(Client),
  ?assertEqual({error, no_such_client}, wolff:check_connectivity(ClientId)),
  ok = application:stop(wolff).


client_state_upgrade_test() ->
  ClientId = <<"client-stats-test">>,
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
  ?assertEqual({error, no_such_client}, wolff:check_connectivity(ClientId)),
  ok = application:stop(wolff).

fail_to_connect_all_test() ->
  ClientId = <<"fail-to-connect-test">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = #{connect_timeout => 500},
  Hosts = [{localhost, 9999},
           {<<"localhost">>, 9999},
           {{1, 2, 3, 4}, 9999}, %% timeout
           {{127, 0, 0}, 9999}, %% invalid type, cause crash but caught
           {"127.0.0", 9999} %% invalid ip
          ],
  {ok, Client} = start_client(ClientId, Hosts, ClientCfg),
  ?assertEqual({error, failed_to_fetch_metadata},
               wolff_client:get_leader_connections(Client, <<"test-topic">>)),
  Refuse = {<<"localhost:9999">>, connection_refused},
  {error, Errors} = wolff:check_connectivity(ClientId),
  ?assertMatch([{<<"1.2.3.4:9999">>, connection_timed_out},
                {<<"127.0.0:9999">>, _},
                Refuse, Refuse,
                {<<"{127,0,0}:9999">>, _}
                ],
               lists:sort(Errors)),
  ok = application:stop(wolff).

leader_restart_test_() ->
  {timeout, 60,
   fun() -> test_leader_restart() end}.

test_leader_restart() ->
  Topic = "testtopic" ++ integer_to_list(abs(erlang:monotonic_time())),
  %% number of partitions should be greater than number of Kafka brokers
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
    {ok, LeadersA0} = wolff_client:get_leader_connections(ClientA, TopicBin),
    {ok, LeadersB0} = wolff_client:get_leader_connections(ClientB, TopicBin),
    ok = stop_kafka_2(),
    {ok, LeadersA1} = wolff_client:get_leader_connections(ClientA, TopicBin),
    {ok, LeadersB1} = wolff_client:get_leader_connections(ClientB, TopicBin),
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
    ProducerCfg = #{partitioner => fun(_, _) -> 0 end, max_batch_bytes => MaxMessageBytes * 3},
    {ok, Producers} = wolff:start_producers(Client, TopicBin, ProducerCfg),
    MaxBytesCompensateOverhead = MaxMessageBytes - ?BATCHING_OVERHEAD - 7,
    Msg = fun(C) -> #{key => <<>>, value => iolist_to_binary(lists:duplicate(MaxBytesCompensateOverhead, C))} end,
    SendFunc = fun(Batch) ->
      Ref = make_ref(),
      Self = self(),
      AckFun = {fun ?MODULE:ack_cb/4, [Self, Ref]},
      _ = wolff:send(Producers, Batch, AckFun),
      ?WAIT(5000, {ack, Ref, _Partition, BaseOffset}, BaseOffset)
    end,
    %% Must be ok to send one message
    ?assertEqual(0, SendFunc([Msg($0)])),
    %% Three messages in one batch is over the limit, but the producer
    %% should split the batch.
    %% assert base offset is the offset for the first message in the batch (1)
    %% but not the last (3)
    ?assertEqual(1, SendFunc([Msg($a), Msg($b), Msg($c)])),
    %% send a too-large single message, producer is forced to drop it
    ?assertEqual(message_too_large, SendFunc([Msg(<<"0123456789">>)])),
    ok = wolff:stop_producers(Producers),
    ok = stop_client(Client),
    ok = application:stop(wolff)
  end).

one_byte_limit_test() ->
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
  Expected = "Created topic " ++ Topic ++ ".\n",
  ?assertEqual(Expected, Result),
  ok.

delete_topic(Topic) ->
  Cmd = delete_topic_cmd(Topic),
  Result = os:cmd(Cmd),
  case string:str(Result, "Topic " ++ Topic ++ " is marked for deletion.") of
    1 -> ok;
    _ -> throw(Result)
  end.

create_topic_cmd(Topic, Partitions, ReplicationFactor, MaxMessageBytes) ->
  "docker exec wolff-kafka-1 /opt/kafka/bin/kafka-topics.sh" ++
  " --zookeeper zookeeper:2181" ++
  " --create" ++
  " --topic '" ++ Topic ++ "'" ++
  " --partitions " ++ integer_to_list(Partitions) ++
  " --replication-factor " ++ integer_to_list(ReplicationFactor) ++
  case is_integer(MaxMessageBytes) of
    true ->
      " --config max.message.bytes=" ++ integer_to_list(MaxMessageBytes);
    false ->
      ""
  end.

delete_topic_cmd(Topic) ->
  "docker exec wolff-kafka-1 /opt/kafka/bin/kafka-topics.sh" ++
  " --zookeeper zookeeper:2181" ++
  " --delete" ++
  " --topic '" ++ Topic ++ "'".

stop_kafka_2() ->
  Cmd = "docker stop wolff-kafka-2",
  os:cmd(Cmd),
  ok.

start_kafka_2() ->
  Cmd = "docker start wolff-kafka-2",
  os:cmd(Cmd),
  ok.
