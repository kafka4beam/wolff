-module(wolff_dynamic_topics_tests).

-include("wolff.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(KEY, key(?FUNCTION_NAME)).
-define(HOSTS, [{"localhost", 9092}]).

dynamic_topics_test() ->
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientId = <<"dynamic-topics">>,
  ClientCfg = client_config(),
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  Group = atom_to_binary(?FUNCTION_NAME),
  ProducerCfg = #{required_acks => all_isr,
                  group => Group,
                  partitioner => 0
                 },
  {ok, Producers} = start(ClientId, ProducerCfg),
  ?assertEqual({ok, Producers}, start(ClientId, ProducerCfg)),
  Children = supervisor:which_children(wolff_producers_sup),
  ?assertMatch([_], Children),
  %% We can send from each producer.
  Msg = #{key => ?KEY, value => <<"value">>},
  Self = self(),
  AckFun = fun(_Partition, _BaseOffset) -> Self ! acked, ok end,
  T1 = <<"test-topic">>,
  T2 = <<"test-topic-2">>,
  T3 = <<"test-topic-3">>,
  N = 100,
  %% send N messages
  lists:foreach(fun(_I) ->
    ?assertMatch({0, Pid} when is_pid(Pid), wolff:send2(Producers, T1, [Msg], AckFun))
  end, lists:seq(1, N)),
  %% wait for N acks
  lists:foreach(fun(_I) ->
    receive acked -> ok end
  end, lists:seq(1, N)),
  ok = assert_producers_state(Producers, [T1, T2, T3]),
  ?assertMatch({0, Offset} when is_integer(Offset), send(Producers, T2, Msg)),
  ?assertMatch({0, Offset} when is_integer(Offset), cast(Producers, T3, Msg)),
  ?assertMatch(#{metadata_ts := #{T1 := _, T2 := _, T3 := _},
                 known_topics := #{T1 := _, T2 := _, T3 := _},
                 conns := #{{T1, 0} := _, {T2, 0} := _, {T3, 0} := _}
                }, sys:get_state(ClientPid)),
  %% We now stop one of the producers.  The other should keep working.
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  %% idempotent
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  EmptyMap = #{},
  ?assertMatch(#{metadata_ts := EmptyMap,
                 known_topics := EmptyMap,
                 conns := EmptyMap
                },
               sys:get_state(ClientPid)),
  ?assertEqual([], ets:tab2list(?WOLFF_PRODUCERS_GLOBAL_TABLE)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok = application:stop(wolff),
  ok.

ack_cb_interlave_test() ->
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientId = <<"ack_cb_interleave_tes">>,
  ClientCfg = client_config(),
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  Group = atom_to_binary(?FUNCTION_NAME),
  ProducerCfg = #{required_acks => all_isr,
                  group => Group,
                  partitioner => 0
                 },
  {ok, Producers} = start(ClientId, ProducerCfg),
  ?assertEqual({ok, Producers}, start(ClientId, ProducerCfg)),
  Children = supervisor:which_children(wolff_producers_sup),
  ?assertMatch([_], Children),
  %% We can send from each producer.
  Msg = #{key => ?KEY, value => <<"value">>},
  Self = self(),
  AckFun1 = fun(_Partition, _BaseOffset) -> Self ! acked1, ok end,
  AckFun2 = fun(_Partition, _BaseOffset) -> Self ! acked2, ok end,
  T1 = <<"test-topic">>,
  N = 10,
  {0, Pid} = wolff:send2(Producers, T1, [Msg], AckFun1),
  {0, Pid} = wolff:send2(Producers, T1, [Msg], AckFun2),
  receive acked1 -> ok end,
  receive acked2 -> ok end,
  %% suspend the connection, to make sure the acks will be pending
  #{conn := Conn} = sys:get_state(Pid),
  sys:suspend(Conn),
  %% send N messages
  lists:foreach(fun(_I) ->
    ?assertMatch({0, Pid} when is_pid(Pid), wolff:cast2(Producers, T1, [Msg], AckFun1)),
    ?assertMatch({0, Pid} when is_pid(Pid), wolff:cast2(Producers, T1, [Msg], AckFun2))
  end, lists:seq(1, N)),
  %% inspect the pending acks
  #{conn := Conn, pending_acks := Acks} = sys:get_state(Pid),
  ?assertEqual(N * 2, wolff_pendack:count(Acks)),
  #{inflight := Inflight, backlog := Backlog} = Acks,
  ?assertEqual(N * 2, queue:len(Backlog) + queue:len(Inflight)),
  %% resume the connection
  sys:resume(Conn),
  %% wait for 2*N acks
  lists:foreach(fun(_I) -> receive acked1 -> ok end end, lists:seq(1, N)),
  lists:foreach(fun(_I) -> receive acked2 -> ok end end, lists:seq(1, N)),
  ok = assert_producers_state(Producers, [T1]),
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok = application:stop(wolff),
  ok.

assert_producers_state(_Producers, []) ->
  ok;
assert_producers_state(Producers, [Topic | Topics]) ->
  ok = assert_producer_state(Producers, Topic),
  assert_producers_state(Producers, Topics).

assert_producer_state(Producers, Topic) ->
  {0, Pid} = wolff_producers:pick_producer2(Producers, Topic, [#{value => <<"a">>}]),
  #{pending_acks := Acks, sent_reqs := SentReqs} = sys:get_state(Pid),
  ?assertEqual(0, wolff_pendack:count(Acks)),
  ?assertEqual(0, queue:len(SentReqs)),
  ok.

send(Producers, Topic, Message) ->
  wolff:send_sync2(Producers, Topic, [Message], 10_000).

cast(Producers, Topic, Message) ->
  Self = self(),
  Ref = make_ref(),
  AckFun = fun(Partition, BaseOffset) ->
    Self ! {Ref, Partition, BaseOffset},
    ok
  end,
  {_, _} = wolff:cast2(Producers, Topic, [Message], AckFun),
  receive
    {Ref, Partition, BaseOffset} -> {Partition, BaseOffset}
  after 10_000 ->
    error(timeout)
  end.

unknown_topic_expire_test() ->
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientId = <<"dynamic-topics">>,
  ClientCfg = client_config(),
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  Group = atom_to_binary(?FUNCTION_NAME),
  ProducerCfg = #{required_acks => all_isr,
                  group => Group,
                  partitioner => 0
                 },
  {ok, Producers} = start(ClientId, ProducerCfg),
  Children = supervisor:which_children(wolff_producers_sup),
  ?assertMatch([_], Children),
  %% We can send from each producer.
  Msg = #{key => ?KEY, value => <<"value">>},
  Self = self(),
  AckFun = fun(_Partition, _BaseOffset) -> Self ! acked, ok end,
  Ts0 = erlang:system_time(millisecond),
  Topic = iolist_to_binary([<<"tmp_topic_">>, integer_to_list(Ts0)]),
  ?assertThrow(#{cause := unknown_topic_or_partition,
                 group := Group,
                 topic := Topic,
                 response := refreshed
                }, wolff:send2(Producers, Topic, [Msg], AckFun)),
  [{{Group, Topic, partition_count}, ?UNKNOWN(Ts1)}] = ets:tab2list(?WOLFF_PRODUCERS_GLOBAL_TABLE),
  %% try to send again, should not result in a new ts
  ?assertThrow(#{cause := unknown_topic_or_partition,
                 group := Group,
                 topic := Topic,
                 response := cached
                }, wolff:send2(Producers, Topic, [Msg], AckFun)),
  ?assertEqual([{{Group, Topic, partition_count}, ?UNKNOWN(Ts1)}], ets:tab2list(?WOLFF_PRODUCERS_GLOBAL_TABLE)),
  %% force the unknown mark to expire
  ets:insert(?WOLFF_PRODUCERS_GLOBAL_TABLE, {{Group, Topic, partition_count}, ?UNKNOWN(0)}),
  %% create the topic in Kafka
  ok = create_topic(Topic),
  ?assertMatch({0, Offset} when is_integer(Offset),
               wolff:send_sync2(Producers, Topic, [Msg], 10_000)),
  ?assertMatch([C] when C > 0,
               ets:lookup(?WOLFF_PRODUCERS_GLOBAL_TABLE, {Group, Topic, partition_count})),
  %% cleanup
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], ets:tab2list(?WOLFF_PRODUCERS_GLOBAL_TABLE)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok = application:stop(wolff),
  ok = delete_topic(Topic).

bad_producers_test() ->
  Producers = #{group => ?NO_GROUP, client_id => <<"foobar">>, topic => <<"test-topic">>},
  Msg = #{value => <<"v">>},
  ?assertError("cannot_add_topic_to_non_dynamic_producer", wolff:send_sync2(Producers, <<"test-topic">>, [Msg], 1000)),
  ok.

topic_add_remove_test() ->
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientId = <<"dynamic-topics-add-remove">>,
  ClientCfg = client_config(),
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  Group = atom_to_binary(?FUNCTION_NAME),
  ProducerCfg = #{required_acks => all_isr,
                  group => Group,
                  partitioner => 0
                 },
  {ok, Producers} = start(ClientId, ProducerCfg),
  Topic = <<"test-topic">>,
  ?assertEqual(ok, wolff:add_topic(Producers, Topic)),
  ?assertMatch([{{Group, Topic, 0}, Pid}] when is_pid(Pid),
               ets:lookup(?WOLFF_PRODUCERS_GLOBAL_TABLE, {Group, Topic, 0})),
  ?assertEqual({error, unknown_topic_or_partition},
               wolff:add_topic(Producers, <<"unknown-topic">>)),
  ok = wolff:remove_topic(Producers, Topic),
  EmptyMap = #{},
  ?assertMatch(#{metadata_ts := EmptyMap,
                 known_topics := EmptyMap,
                 conns := EmptyMap
                },
               sys:get_state(ClientPid)),
  ?assertMatch([{{Group, <<"unknown-topic">>, partition_count}, ?UNKNOWN(_)}],
               ets:tab2list(?WOLFF_PRODUCERS_GLOBAL_TABLE)),
  %% cleanup
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], ets:tab2list(?WOLFF_PRODUCERS_GLOBAL_TABLE)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok = application:stop(wolff),
  ok.

faile_to_fetch_initial_metadata_test() ->
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientId = <<"dynamic-topics">>,
  ClientCfg = client_config(),
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  Group = atom_to_binary(?FUNCTION_NAME),
  ProducerCfg = #{required_acks => all_isr,
                  group => Group,
                  partitioner => 0
                 },
  ok = meck:new(wolff_client, [non_strict, no_history, no_link, passthrough]),
  %% inject an error
  meck:expect(wolff_client, get_leader_connections,
              fun(_ClientId, _Group, _Topic, _MaxPartitions) ->
                      {error, ?FUNCTION_NAME}
              end),
  {ok, Producers} = start(ClientId, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  AckFun = fun(_Partition, _BaseOffset) -> ok end,
  ?assertThrow(#{error := ?FUNCTION_NAME}, wolff:send2(Producers, <<"test-topic">>, [Msg], AckFun)),
  %% remove the error
  meck:expect(wolff_client, get_leader_connections,
              fun(ClientId1, Group1, Topic1, MaxPartitions1) ->
                      meck:passthrough([ClientId1, Group1, Topic1, MaxPartitions1])
              end),
  %% trigger an immediate re-init
  [{_, ProducersPid, _, _}] = supervisor:which_children(wolff_producers_sup),
  ProducersPid ! init_producers,
  ?assertMatch({Partition, Pid} when is_integer(Partition) andalso is_pid(Pid),
               wolff:send2(Producers, <<"test-topic">>, [Msg], AckFun)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok = application:stop(wolff),
  meck:unload(wolff_client),
  ok.

%% helpers

client_config() -> #{}.

key(Name) ->
  iolist_to_binary(io_lib:format("~0p/~0p/~0p", [Name, calendar:local_time(), erlang:system_time()])).

create_topic(Topic) when is_binary(Topic) ->
  create_topic(binary_to_list(Topic));
create_topic(Topic) ->
  Cmd = kafka_topic_cmd_base(Topic) ++ " --create --partitions 1 --replication-factor 1",
  Result = os:cmd(Cmd),
  Pattern = "Created topic " ++ Topic ++ ".",
  ?assert(string:str(Result, Pattern) > 0),
  ok.

delete_topic(Topic) when is_binary(Topic) ->
  delete_topic(binary_to_list(Topic));
delete_topic(Topic) ->
  Cmd = kafka_topic_cmd_base(Topic) ++ " --delete",
  _ = os:cmd(Cmd),
  ok.

kafka_topic_cmd_base(Topic) ->
  "docker exec wolff-kafka-1 /opt/kafka/bin/kafka-topics.sh" ++
  " --zookeeper zookeeper:2181" ++
  " --topic '" ++ Topic ++ "'".

start(ClientId, ProducerCfg) ->
  wolff:ensure_supervised_dynamic_producers(ClientId, ProducerCfg).
