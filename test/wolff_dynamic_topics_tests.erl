-module(wolff_dynamic_topics_tests).

-include("wolff.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").

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
  ?assertMatch({0, Pid} when is_pid(Pid), wolff:send2(Producers, T1, [Msg], AckFun)),
  receive acked -> ok end,
  ?assertMatch({0, Offset} when is_integer(Offset), wolff:send_sync2(Producers, T2, [Msg], 10_000)),
  ?assertMatch({0, Offset} when is_integer(Offset), wolff:send_sync2(Producers, T3, [Msg], 10_000)),
  ?assertMatch(#{known_topics := #{T1 := _, T2 := _, T3 := _},
                 conns := #{{T1, 0} := _, {T2, 0} := _, {T3, 0} := _}
                }, sys:get_state(ClientPid)),
  %% We now stop one of the producers.  The other should keep working.
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  %% idempotent
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertMatch(#{known_topics := Topics,
                 conns := Conns
                } when map_size(Topics) =:= 0 andalso map_size(Conns) =:= 0,
               sys:get_state(ClientPid)),
  ?assertEqual([], ets:tab2list(?WOLFF_PRODUCERS_GLOBAL_TABLE)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok = application:stop(wolff),
  ok.

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
  ?assertMatch(#{known_topics := Topics,
                 conns := Conns
                } when map_size(Topics) =:= 0 andalso map_size(Conns) =:= 0,
               sys:get_state(ClientPid)),
  ?assertMatch([{{Group, <<"unknown-topic">>, partition_count}, ?UNKNOWN(_)}],
               ets:tab2list(?WOLFF_PRODUCERS_GLOBAL_TABLE)),
  %% cleanup
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], ets:tab2list(?WOLFF_PRODUCERS_GLOBAL_TABLE)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok = application:stop(wolff),
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
