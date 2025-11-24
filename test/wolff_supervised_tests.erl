-module(wolff_supervised_tests).

-include("wolff.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").

-define(KEY, key(?FUNCTION_NAME)).
-define(HOSTS, [{"localhost", 9092}]).

supervised_client_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientId = <<"supervised-wolff-client">>,
  ok = start_app(),
  ClientCfg = client_config(),
  {ok, Client} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  %% start it again should result in the same client pid
  ?assertEqual({ok, Client},
               wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg)),
  ProducerCfg0 = producer_config(?FUNCTION_NAME),
  ProducerCfg = ProducerCfg0#{required_acks => leader_only},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000),
  io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
            [Partition, BaseOffset]),
  ok = wolff:stop_producers(Producers),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = stop_app(),
  ?assertEqual(undefined, whereis(wolff_sup)),
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(queuing_bytes, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
  stop_app(),
  ok.

supervised_producers_test_() ->
  [{"atom-name", fun() -> test_supervised_producers(test_producers) end},
   {"binary-name", fun() -> test_supervised_producers(<<"test-producers">>) end}
  ].

test_supervised_producers(Name) ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientId = <<"supervised-producers-1">>,
  ok = start_app(),
  ClientCfg = client_config(),
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ProducerCfg0 = producer_config(Name),
  ProducerCfg = ProducerCfg0#{required_acks => all_isr},
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, <<"test-topic">>, ProducerCfg),
  ?assertEqual({ok, Producers}, wolff:ensure_supervised_producers(ClientId, <<"test-topic">>, ProducerCfg)),
  Msg = #{key => ?KEY, value => <<"value">>},
  Self = self(),
  AckFun = fun(_Partition, _BaseOffset) -> Self ! acked, ok end,
  {_Partition, _ProducerPid} = wolff:send(Producers, [Msg], AckFun),
  receive acked -> ok end,
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = stop_app(),
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(queuing_bytes, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
  ok.

%% Checks that having different producers to the same kafka topic works.
different_producers_same_topic_test_() ->
  {timeout, 30, fun test_different_producers_same_topic/0}.

test_different_producers_same_topic() ->
  ok = start_app(),
  ClientId = <<"same-topic">>,
  ClientCfg = client_config(),
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  Topic = <<"test-topic">>,
  ProducerName0 = <<"p0">>,
  Group0 = <<"a0">>,
  ProducerCfg0 = (producer_config(ProducerName0))#{required_acks => all_isr, group => Group0},
  {ok, Producers0} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg0),
  ?assertEqual({ok, Producers0},
               wolff:ensure_supervised_producers(ClientId, <<"test-topic">>, ProducerCfg0)),
  ProducerName1 = <<"p1">>,
  Group1 = <<"a1">>,
  ProducerCfg1 = (producer_config(ProducerName1))#{required_acks => all_isr, group => Group1},
  {ok, Producers1} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg1),
  ?assertEqual({ok, Producers0},
               wolff:ensure_supervised_producers(ClientId, <<"test-topic">>, ProducerCfg0)),
  %% We can send from each producer.
  Msg = #{key => ?KEY, value => <<"value">>},
  Self = self(),
  AckFun = fun(_Partition, _BaseOffset) -> Self ! acked, ok end,
  {_Partition0, _ProducerPid0} = wolff:send(Producers0, [Msg], AckFun),
  receive acked -> ok end,
  {_Partition1A, _ProducerPid1A} = wolff:send(Producers1, [Msg], AckFun),
  receive acked -> ok end,
  %% Each replayq dir should be namespaced by the group
  #{replayq_dir := BaseDir} = ProducerCfg0,
  {ok, Files} = file:list_dir(BaseDir),
  Dir0 = <<Group0/binary, $_, Topic/binary>>,
  Dir1 = <<Group1/binary, $_, Topic/binary>>,
  ReplayQDirs = [File || File <- Files,
                         filelib:is_dir(filename:join([BaseDir, File])),
                         lists:member(list_to_binary(File), [Dir0, Dir1])],
  ?assertMatch([_, _], ReplayQDirs, #{base_dir => Files}),
  ?assertMatch(#{metadata_ts := #{Topic := _},
                 known_topics := #{Topic := #{Group0 := true, Group1 := true}},
                 conns := #{{Topic, 0} := _}
                }, sys:get_state(ClientPid)),
  %% We now stop one of the producers.  The other should keep working.
  ok = wolff:stop_and_delete_supervised_producers(Producers0),
  ?assertMatch(#{metadata_ts := #{Topic := _},
                 known_topics := #{Topic := #{Group1 := true}},
                 conns := #{{Topic, 0} := _}
                }, sys:get_state(ClientPid)),
  ?assertMatch(#{}, sys:get_state(ClientPid)),
  {_Partition1B, _ProducerPid1B} = wolff:send(Producers1, [Msg], AckFun),
  receive acked -> ok end,
  ok = wolff:stop_and_delete_supervised_producers(Producers1),
  EmptyMap = #{},
  ?assertMatch(#{metadata_ts := EmptyMap,
                 known_topics := EmptyMap,
                 conns := EmptyMap
                },
               sys:get_state(ClientPid)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok = stop_app(),
  ok.

client_restart_test() ->
  ClientId = <<"client-restart-test">>,
  Topic = <<"test-topic">>,
  Partition = 0,
  test_client_restart(ClientId, Topic, Partition).

%% test-topic-2 should have 2 partitions
client_restart_2_test() ->
  ClientId = <<"client-restart-test-2">>,
  Topic = <<"test-topic-2">>,
  Partition = 0,
  test_client_restart(ClientId, Topic, Partition).

test_client_restart(ClientId, Topic, Partition) ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ok = start_app(),
  ClientCfg = #{connection_strategy => per_broker},
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ProducerCfg = #{replayq_dir => "test-data/client-restart-test",
                  required_acks => all_isr,
                  partitioner => Partition, %% always send to the same partition
                  partition_count_refresh_interval_seconds => 0,
                  name => ?FUNCTION_NAME
                 },
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg),
  Msg1 = #{key => ?KEY, value => <<"1">>},
  {_, Offset1} = wolff:send_sync(Producers, [Msg1], 5000),
  erlang:exit(ClientPid, kill),
  timer:sleep(5),
  Msg2 = #{key => ?KEY, value => <<"2">>},
  {_, _Offset2} = wolff:send_sync(Producers, [Msg2], 5000),
  {ok, NewClientPid} = wolff_client_sup:find_client(ClientId),
  ok = fetch_and_match(NewClientPid, Topic, Partition, Offset1, [Msg1, Msg2]),
  %% cleanup
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = stop_app(),
  [1,1] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(queuing_bytes, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
  ok.

max_partitions_test() ->
  ClientId = <<"max-partitions-test">>,
  Topic = <<"test-topic-2">>,
  Partition = 0,
  MaxPartitions = 1,
  ok = start_app(),
  ClientCfg = #{connection_strategy => per_partition},
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ProducerCfg = #{required_acks => all_isr,
                  partitioner => Partition,
                  max_partitions => MaxPartitions
                 },
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg),
  %% the topic has two partitions, but limited only to started one producer
  ?assertMatch([_], wolff_producers:find_producers_by_client_topic(ClientId, ?NO_GROUP, Topic)),
  %% cleanup
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = stop_app(),
  ok.

%% Test against a bad host.
%% No connection will be established at all.
%% Producer workers should not crash, async APIs should work.
bad_host_test() ->
  ClientId = <<"bad-host-test">>,
  ok = start_app(),
  {ok, _} = wolff:ensure_supervised_client(ClientId, [{"badhost", 9092}], #{}),
  ?assertMatch({error, _}, wolff:ensure_supervised_producers(ClientId, <<"t">>, #{name => ?FUNCTION_NAME})),
  ok = wolff:stop_and_delete_supervised_client(ClientId).

producer_restart_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientId = <<"producer-restart">>,
  Topic = <<"test-topic">>,
  Partition = 0,
  ok = start_app(),
  ClientCfg = #{},
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ProducerCfg = #{replayq_dir => "test-data/producer-restart-test",
                  required_acks => all_isr,
                  partitioner => Partition,
                  reconnect_delay_ms => 0,
                  name => ?FUNCTION_NAME
                 },
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg),
  GetPid = fun() -> {ok, Pid} = wolff_producers:find_producer_by_partition(ClientId, ?NO_GROUP, Topic, Partition), Pid end,
  Producer0 = GetPid(),
  Msg0 = #{key => ?KEY, value => <<"0">>},
  {0, Offset0} = wolff_producer:send_sync(Producer0, [Msg0], 2000),
  %% the commit towards replayq is async, wait for it
  timer:sleep(5),
  DummyAckFun = fun(_, _) -> ok end,
  Msg1 = #{key => ?KEY, value => <<"1">>},
  {Partition, Producer0} = wolff:send(Producers, [Msg1], DummyAckFun),
  erlang:exit(Producer0, kill),
  %% let wolff_producers to pick up the EXIT signal and mark it down in ets
  timer:sleep(10),
  _Producer1 = wait_for_pid(GetPid),
  Msg2 = #{key => ?KEY, value => <<"2">>},
  _ = wolff:send_sync(Producers, [Msg2], 4000),
  ok = fetch_and_match(ClientPid, Topic, Partition, Offset0, [Msg0, Msg1, Msg2]),
  %% cleanup
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = stop_app(),
  [1,2] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(queuing_bytes, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
  ok.

stop_with_name_test() ->
  ClientId = <<"stop-with-name">>,
  Topic = <<"test-topic">>,
  Partition = 0,
  ok = start_app(),
  ClientCfg = #{},
  {ok, _} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  Name = ?FUNCTION_NAME,
  ProducerCfg = #{required_acks => all_isr,
                  partitioner => Partition,
                  reconnect_delay_ms => 0,
                  name => Name
                 },
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg),
  %% cleanup
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = stop_app(),
  ok.

partition_count_increase_test_() ->
  {timeout, 30, %% it takes time to alter topic via cli in docker container
   fun test_partition_count_increase/0}.

test_partition_count_increase() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientId = <<"test-add-more-partitions">>,
  Topic = <<"test-topic-3">>,
  ok = start_app(),
  ClientCfg = #{},
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  {ok, Connections0} = get_leader_connections(ClientPid, Topic),
  Partitions0 = length(Connections0),
  ?assertEqual(Partitions0, count_partitions(Topic)),
  %% always send to the last partition
  Partitioner = fun(Count, _) -> Count - 1 end,
  Name = ?FUNCTION_NAME,
  IntervalSeconds = 1,
  ProducerCfg = #{required_acks => all_isr,
                  partitioner => Partitioner,
                  reconnect_delay_ms => 0,
                  name => Name,
                  partition_count_refresh_interval_seconds => IntervalSeconds
                 },
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition0, Offset0} = wolff:send_sync(Producers, [Msg], 3000),
  ?assert(is_integer(Offset0), Offset0),
  ?assertEqual(Partitions0 - 1, Partition0),
  %% create a new partition
  ensure_partitions(Topic, Partitions0 + 1),
  %% wait for the new partition to be discovered
  timer:sleep(timer:seconds(IntervalSeconds * 2)),
  {Partition1, Offset1} = wolff:send_sync(Producers, [Msg], 3000),
  ?assert(is_integer(Offset1), Offset1),
  ?assertEqual(Partitions0, Partition1),
  [1,1] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(queuing_bytes, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
  ok.

partition_count_decrease_test_() ->
  {timeout, 30, %% it takes time to alter topic via cli in docker container
   fun test_partition_count_decrease/0}.

test_partition_count_decrease() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientId = <<"recreate-topic-with-fewer-partitions">>,
  Topic = <<"test-topic-fewer-partition">>,
  Partitions0 = 3,
  delete_topic(Topic),
  create_topic(Topic, Partitions0),
  ok = start_app(),
  ClientCfg = #{},
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  {ok, Connections0} = get_leader_connections(ClientPid, Topic),
  ?assertEqual(Partitions0, length(Connections0)),
  ?assertEqual(Partitions0, count_partitions(Topic)),
  %% always send to the last partition
  Partitioner = fun(Count, _) -> Count - 1 end,
  Name = ?FUNCTION_NAME,
  IntervalSeconds = 1,
  ProducerCfg = #{required_acks => all_isr,
                  partitioner => Partitioner,
                  reconnect_delay_ms => 0,
                  name => Name,
                  partition_count_refresh_interval_seconds => IntervalSeconds
                 },
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition0, _} = wolff:send_sync(Producers, [Msg], 3000),
  ?assertEqual(Partitions0 - 1, Partition0),
  Self = self(),
  AsyncSends = lists:seq(1, 10),
  %% delete the topic
  delete_topic(Topic),
  AckFn = fun(I) -> fun(Partition, OffsetReply) -> Self ! {async_ack, I, Partition, OffsetReply}, ok end end,
  lists:foreach(fun(I) -> wolff:send(Producers, [Msg], AckFn(I)) end, AsyncSends),
  %% wait for the topic to be deleted
  create_topic(Topic, Partitions0 - 1),
  %% wait for partition_lost replies
  lists:foreach(
    fun(I) ->
      receive
        {async_ack, AckI, P, O} ->
          ?assertEqual(Partitions0 - 1, P),
          ?assertEqual(O, partition_lost),
          ?assertEqual(I, AckI)
      after
        10_000 ->
          error(timeout)
      end
    end, AsyncSends),
  {ok, Connections1} = get_leader_connections(ClientPid, Topic),
  ?assertEqual(Partitions0 - 1, length(Connections1)),
  ?assertEqual(Partitions0 - 1, count_partitions(Topic)),
  ?assertEqual(Partitions0 - 1, wolff_producers:get_partition_cnt(ClientId, ?NO_GROUP, Topic)),
  {Partition1, _} = wolff:send_sync(Producers, [Msg], 3000),
  ?assertEqual(Partition0 - 1, Partition1),
  [1,1] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  %% 10 messages, 1 failed and retried, 9 in the backlog got discarded
  [1,9] = get_telemetry_seq(CntrEventsTable, [wolff,failed]),
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(queuing_bytes, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = stop_app(),
  ok.

%% The last partition is temporarily missing from metadata response.
%% The producer should retry to recover
last_partition_missing_in_metadata_response_test_() ->
  {timeout, 30, %% it takes time to alter topic via cli in docker container
   fun() -> test_partition_missing_in_metadata_response(2) end}.

%% The partition in the middle is temporarily missing from metadata response.
%% The producer should retry to recover
mid_partition_missing_in_metadata_response_test_() ->
  {timeout, 30, %% it takes time to alter topic via cli in docker container
   fun() -> test_partition_missing_in_metadata_response(1) end}.

test_partition_missing_in_metadata_response(ThePartition) ->
  ClientId = <<"test-temporarily-malformed-metadata-response">>,
  %% ensure the topic does not exist
  Topic = <<"test-topic-5">>,
  delete_topic(Topic),
  Partitions = 3,
  create_topic(Topic, Partitions),
  io:format(user, "created topic ~s with ~p partitions\n", [Topic, Partitions]),
  ok = start_app(),
  %% always refresh metadata
  ClientCfg = #{min_metadata_refresh_interval => 0},
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  {ok, Connections} = get_leader_connections(ClientPid, Topic),
  ?assertEqual(Partitions, length(Connections)),
  %% always send it to the partition which is going to be missing
  Partitioner = fun(_, _) -> ThePartition end,
  ProducerCfg = #{required_acks => all_isr,
                  partitioner => Partitioner,
                  reconnect_delay_ms => 0,
                  %% 0 means no aut-refresh
                  partition_count_refresh_interval_seconds => 0
                 },
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg),
  ?assertEqual(Partitions, wolff_producers:get_partition_cnt(ClientId, ?NO_GROUP, Topic)),
  Msg = #{key => ?KEY, value => <<"value">>},
  ?assertMatch({ThePartition, _}, wolff:send_sync(Producers, [Msg], 3000)),
  %% mock a bad
  %% Kill partition leader connection
  Pid = get_partition_leader_connection(ClientPid, Topic, ThePartition),
  exit(Pid, kill),
  %% mock kafka_protcol to return metadata with ThePartition missing
  meck:new(kpro, [passthrough, no_history]),
  meck:expect(kpro, request_sync,
    fun(Connection, Req, Timeout) ->
      {ok, Rsp} = meck:passthrough([Connection, Req, Timeout]),
      case Rsp of
        #kpro_rsp{msg = #{topics := [#{partitions := PM0} = TopicMeta]} = Meta} ->
          PM = lists:filter(fun(#{partition_index := P}) -> P =/= ThePartition end, PM0),
          NewRsp = Rsp#kpro_rsp{msg =Meta#{topics := [TopicMeta#{partitions := PM}]}},
          {ok, NewRsp};
        _ ->
          {ok, Rsp}
      end
  end),
  ?assertNot(is_pid(get_partition_leader_connection(ClientPid, Topic, ThePartition))),
  %% the request will be buffered, but the call times out
  Msg2 = #{key => ?KEY, value => <<"v">>},
  ?assertError(timeout, wolff:send_sync(Producers, [Msg2], 100)),
  %% Wait for auto recover
  Tester = self(),
  meck:expect(kpro, send,
    fun(Conn, Req) ->
      Tester ! {sent, Conn},
      meck:passthrough([Conn, Req])
    end),
  %% Now remove the injected error (malformed metadata response)
  %% wolff_producer should now be able to recover
  meck:expect(kpro, request_sync,
    fun(Connection, Req, Timeout) ->
      meck:passthrough([Connection, Req, Timeout])
    end),
  receive
    {sent, NewConn} ->
      ?assertEqual(NewConn, get_partition_leader_connection(ClientPid, Topic, ThePartition))
  after
    2000 ->
      %% wolff_producer retry delay is 0, but it randomize with extra 0-1000ms
      error(timeout)
  end,
  meck:unload(kpro),
  ok = fetch_and_match(ClientPid, Topic, ThePartition, 0, [Msg, Msg2]),
  %% cleanup
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = stop_app(),
  ok.

get_partition_leader_connection(Client, Topic, Partition) ->
  ok = wolff_client:recv_leader_connection(Client, ?NO_GROUP, Topic, Partition, self(), all_partitions),
  receive
    {leader_connection, Pid} ->
      Pid
  after
    20000 ->
      error(timeout)
  end.

topic_recreate_test_() ->
  {timeout, 30, %% it takes time to alter topic via cli in docker container
   fun test_topic_recreate/0}.

test_topic_recreate() ->
  ClientId = <<"test-topic-recreate">>,
  Topic = <<"test-topic-recreate">>,
  %% ensure the topic does not exist
  delete_topic(Topic),
  Partitions0 = 3,
  create_topic(Topic, Partitions0),
  ok = start_app(),
  ClientCfg = #{min_metadata_refresh_interval => 0},
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  {ok, Connections0} = get_leader_connections(ClientPid, Topic),
  ?assertEqual(Partitions0, length(Connections0)),
  Partitioner = fun(_Count, _Key) -> 0 end,
  IntervalSeconds = 1,
  Name = ?FUNCTION_NAME,
  ProducerCfg = #{required_acks => all_isr,
                  partitioner => Partitioner,
                  reconnect_delay_ms => 0,
                  name => Name,
                  partition_count_refresh_interval_seconds => IntervalSeconds
                 },
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg),
  ?assertEqual(Partitions0, wolff_producers:get_partition_cnt(ClientId, ?NO_GROUP, Topic)),
  Msg = #{key => ?KEY, value => <<"value">>},
  ?assertEqual({0, 0}, wolff:send_sync(Producers, [Msg], 3000)),
  %% delete the topic
  delete_topic(Topic),
  Self = self(),
  %% Send a message while the topic is deleted
  AckFn = fun(Partition, OffsetReply) -> Self ! {async_ack, Partition, OffsetReply}, ok end,
  {0, _} = wolff:send(Producers, [Msg], AckFn),
  %% wait for the topic to be deleted
  create_topic(Topic, Partitions0 - 1),
  %% wait for the topic to be recreated
  timer:sleep(timer:seconds(IntervalSeconds * 4)),
  {ok, Connections1} = get_leader_connections(ClientPid, Topic),
  ?assertEqual(Partitions0 - 1, length(Connections1)),
  %% ensure the producers are stopped
  ?assertEqual(Partitions0 - 1, wolff_producers:get_partition_cnt(ClientId, ?NO_GROUP, Topic)),
  ?assertEqual({0, 1}, wolff:send_sync(Producers, [Msg], 3000)),
  receive
    {async_ack, P, O} ->
      ?assertEqual(0, P),
      ?assertEqual(0, O)
  after
    1000 ->
      error(timeout)
  end,
  %% cleanup
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = stop_app(),
  ok.

non_existing_topic_test() ->
  ClientId = atom_to_binary(?FUNCTION_NAME),
  Topic = <<"non-existing-topic">>,
  ok = start_app(),
  ClientCfg = #{},
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ?assertMatch({error, _}, wolff:ensure_supervised_producers(ClientId, Topic, #{name => ?FUNCTION_NAME})),
  ?assertMatch([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok.

start_producers_with_dead_client_test() ->
  ClientId = atom_to_binary(?FUNCTION_NAME),
  Topic = <<"non-existing-topic">>,
  ok = start_app(),
  ?assertMatch({error, _}, wolff:ensure_supervised_producers(<<"never-started">>, Topic, #{name => ?FUNCTION_NAME})),
  ?assertMatch([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok.

fail_retry_success_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientId = <<"supervised-producers">>,
  ok = start_app(),
  ClientCfg = client_config(),
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ProducerCfg0 = producer_config(?FUNCTION_NAME),
  ProducerCfg = ProducerCfg0#{required_acks => all_isr, reconnect_delay_ms => 10},
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  Self = self(),
  AckFun = fun(_Partition, _BaseOffset) -> Self ! acked, ok end,
  try
    telemetry:attach_many(tap, wolff_tests:telemetry_events(),
      fun(EventId, MetricsData, Metadata, state) ->
        Self ! {telemetry, EventId, MetricsData, Metadata},
        ok
      end,
      state),
    with_meck(kpro, find,
      fun(error_code, _Resp) ->
           ?unknown_server_error;
         (Code, Resp) ->
           meck:passthrough([Code, Resp])
      end,
      fun() ->
        {_Partition, _ProducerPid} = wolff:send(Producers, [Msg], AckFun),
        {ok, _} = wait_telemetry_event([wolff, failed]),
        ok
      end),
    {ok, _} = wait_telemetry_event([wolff, retried_success]),
    [1] = get_telemetry_seq(CntrEventsTable, [wolff, failed]),
    [1] = get_telemetry_seq(CntrEventsTable, [wolff, retried]),
    [1] = get_telemetry_seq(CntrEventsTable, [wolff, retried_success]),
    ok
  after
      ok = wolff:stop_and_delete_supervised_producers(Producers),
      ok = wolff:stop_and_delete_supervised_client(ClientId),
      ok = stop_app(),
      telemetry:detach(tap),
      wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
      ets:delete(CntrEventsTable),
      ok
  end,
  ok.

fail_retry_failed_test() ->
  {timeout, 60000,
   begin
     CntrEventsTable = ets:new(cntr_events, [public]),
     wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
     ClientId = <<"supervised-producers">>,
     ok = start_app(),
     ClientCfg = client_config(),
     {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
     ProducerCfg0 = producer_config(?FUNCTION_NAME),
     ProducerCfg = ProducerCfg0#{required_acks => all_isr},
     {ok, Producers} = wolff:ensure_supervised_producers(ClientId, <<"test-topic">>, ProducerCfg),
     Msg = #{key => ?KEY, value => <<"value">>},
     Self = self(),
     AckFun = fun(_Partition, _BaseOffset) -> Self ! acked, ok end,
     try
       telemetry:attach_many(tap, wolff_tests:telemetry_events(),
         fun(EventId, MetricsData, Metadata, state) ->
           Self ! {telemetry, EventId, MetricsData, Metadata},
           ok
         end,
         state),
       with_meck(kpro, find,
         fun(error_code, #{base_offset := _} = _Resp) ->
              ?unknown_server_error;
            (Code, Resp) ->
              meck:passthrough([Code, Resp])
         end,
         fun() ->
           {_Partition, ProducerPid} = wolff:send(Producers, [Msg], AckFun),
           {ok, _} = wait_telemetry_event([wolff, failed], #{timeout => 5_000}),
           %% simulate a disconnection
           ProducerPid ! {leader_connection, {down, test}},
           {ok, _} = wait_telemetry_event([wolff, retried_failed], #{timeout => 30_000}),
           ok
         end),
       {ok, _} = wait_telemetry_event([wolff, retried_success]),
       [1 | _] = get_telemetry_seq(CntrEventsTable, [wolff, failed]),
       [1 | _] = wolff_tests:get_telemetry_seq(CntrEventsTable, [wolff, retried]),
       [1 | _] = wolff_tests:get_telemetry_seq(CntrEventsTable, [wolff, retried_failed]),
       [1] = wolff_tests:get_telemetry_seq(CntrEventsTable, [wolff, retried_success]),
       ok
     after
         ok = wolff:stop_and_delete_supervised_producers(Producers),
         ok = wolff:stop_and_delete_supervised_client(ClientId),
         ok = stop_app(),
         telemetry:detach(tap),
         wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
         ets:delete(CntrEventsTable),
         ok
     end,
     ok
   end}.

%% helpers
with_meck(Mod, FnName, MockedFn, Action) ->
    ok = meck:new(Mod, [non_strict, no_history, no_link, passthrough]),
    ok = meck:expect(Mod, FnName, MockedFn),
    try
        Action()
    after
        meck:unload(Mod)
    end.

wait_telemetry_event(EventName) ->
    wait_telemetry_event(EventName, #{timeout => 10_000}).

wait_telemetry_event(EventName, Opts) ->
    Timeout = maps:get(timeout, Opts, 10_000),
    receive
      {telemetry, EventName, MetricsData, Metadata} ->
        {ok, {EventName, MetricsData, Metadata}}
    after
      Timeout ->
        {timeout, EventName}
    end.

wait_for_pid(F) ->
  Pid = F(),
  case is_pid(Pid) of
    true -> Pid;
    false -> timer:sleep(100), wait_for_pid(F)
  end.

fetch_and_match(ClientPid, Topic, Partition, Offset, ExpectedMsgs) ->
  {ok, Conns} = get_leader_connections(ClientPid, Topic),
  {_, Conn} = lists:keyfind(Partition, 1, Conns),
  Msgs0 = fetch(Conn, Topic, Partition, Offset, 1000),
  Msgs = dedup(Msgs0, undefined),
  match_messages(ExpectedMsgs, Msgs).

match_messages([], []) -> ok;
match_messages([], Received) -> erlang:throw({<<"received more than expected">>, Received});
match_messages(Expected, []) -> erlang:throw({<<"expected not received">>, Expected});
match_messages([E | TE], [R | TR]) ->
  ok = match_message(E, R),
  match_messages(TE, TR).

match_message(#{key := Key}, #kafka_message{key = Key}) -> ok;
match_message(#{key := Key1}, #kafka_message{key = Key2}) -> erlang:throw({Key1, Key2}).

dedup([], _) -> [];
dedup([#kafka_message{value = Value} | Rest], Value) ->
  dedup(Rest, Value);
dedup([#kafka_message{value = V} = Msg | Rest], _LastV) ->
  [Msg | dedup(Rest, V)].

fetch(Connection, Topic, Partition, Offset, MaxBytes) ->
  Opts = #{ max_wait_time => 500
          , max_bytes => MaxBytes
          },
  {ok, {_, Vsn}} = kpro:get_api_vsn_range(Connection, fetch),
  Req = kpro_req_lib:fetch(Vsn, Topic, Partition, Offset, Opts),
  {ok, #kpro_rsp{msg = Rsp}} = kpro:request_sync(Connection, Req, 5000),
  [TopicRsp] = kpro:find(responses, Rsp),
  [PartitionRsp] = kpro:find(partition_responses, TopicRsp),
  Records = kpro:find(record_set, PartitionRsp),
  Batches = kpro:decode_batches(Records),
  case Batches of
    ?incomplete_batch(Size) ->
      fetch(Connection, Topic, Partition, Offset, Size);
    _ ->
      lists:append([Msgs || {_Meta, Msgs} <- Batches])
  end.

client_config() -> #{}.

producer_config(Name) ->
  #{replayq_dir => "test-data",
    name => Name
   }.

key(Name) ->
  iolist_to_binary(io_lib:format("~0p/~0p/~0p", [Name, calendar:local_time(), erlang:system_time()])).

count_partitions(Topic) ->
  Cmd = kafka_topic_cmd_base(Topic) ++ " --describe | grep Configs: | awk -F'PartitionCount[: ]*' '{print $2}' | awk '{print $1}'",
  Result = os:cmd(Cmd),
  list_to_integer(string:strip(Result, right, $\n)).

ensure_partitions(Topic, Partitions) ->
  Cmd = kafka_topic_cmd_base(Topic) ++ " --alter --partitions " ++ integer_to_list(Partitions),
  Result = os:cmd(Cmd),
  Pattern = "Adding partitions succeeded!",
  ?assert(Result =:= [] orelse string:str(Result, Pattern) > 0, Result),
  ok.

kafka_topic_cmd_base(Topic) ->
  wolff_test_utils:topics_cmd_base(Topic).

get_telemetry_seq(Table, Event) ->
    wolff_tests:get_telemetry_seq(Table, Event).

assert_last_event_is_zero(Key, Tab) ->
  case get_telemetry_seq(Tab, [wolff, Key]) of
    [] ->
      throw(no_event_recorded);
    L ->
      case lists:last(L) of
        0 ->
          ok;
        X ->
          throw({unexpected_eventual_state, X})
      end
  end.

get_leader_connections(Client, Topic) ->
    wolff_client:get_leader_connections(Client, ?NO_GROUP, Topic).

create_topic(Topic, Partitions) ->
  Cmd = kafka_topic_cmd_base(Topic) ++ " --create --replication-factor 1 --partitions " ++ integer_to_list(Partitions),
  Result = os:cmd(Cmd),
  Pattern = "Created topic",
  ?assert(string:str(Result, Pattern) > 0, Result),
  ok.

delete_topic(Topic) ->
  wolff_test_utils:delete_topic(Topic).

stop_app() ->
  _ = application:stop(wolff), %% ensure stopped
  ok.

start_app() ->
  ok = stop_app(),
  {ok, _} = application:ensure_all_started(wolff),
  ok.
