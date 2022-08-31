-module(wolff_supervised_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").

-define(KEY, key(?FUNCTION_NAME)).
-define(HOSTS, [{"localhost", 9092}]).

supervised_client_test() ->
  ClientId = <<"supervised-wolff-client">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = client_config(),
  {ok, Client} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  %% start it again should result in the same client pid
  ?assertEqual({ok, Client},
               wolff:ensure_supervised_client(ClientId, [{"localhost", 9092}], ClientCfg)),
  ProducerCfg0 = producer_config(),
  ProducerCfg = ProducerCfg0#{required_acks => leader_only},
  {ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg),
  Msg = #{key => ?KEY, value => <<"value">>},
  {Partition, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000),
  io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
            [Partition, BaseOffset]),
  ?assertMatch(#{send_oct := O, send_cnt := C} when O > 0 andalso C > 0,
               wolff_stats:getstat()),
  ?assertMatch(#{send_oct := O, send_cnt := C} when O > 0 andalso C > 0,
               wolff_stats:getstat(ClientId, <<"test-topic">>, Partition)),
  ok = wolff:stop_producers(Producers),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = application:stop(wolff),
  ?assertEqual(undefined, whereis(wolff_sup)),
  ?assertEqual(undefined, whereis(wolff_stats)),
  ok.

supervised_producers_test() ->
  ClientId = <<"supervised-producers">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = client_config(),
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ProducerCfg0 = producer_config(),
  ProducerCfg = ProducerCfg0#{required_acks => all_isr},
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, <<"test-topic">>, ProducerCfg),
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, <<"test-topic">>, ProducerCfg), %% assert
  Msg = #{key => ?KEY, value => <<"value">>},
  Self = self(),
  AckFun = fun(_Partition, _BaseOffset) -> Self ! acked, ok end,
  {_Partition, _ProducerPid} = wolff:send(Producers, [Msg], AckFun),
  receive acked -> ok end,
  ok = wolff:stop_and_delete_supervised_producers(Producers),
  ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = application:stop(wolff),
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
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = #{connection_strategy => per_broker},
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ProducerCfg = #{replayq_dir => "test-data/client-restart-test",
                  required_acks => all_isr,
                  partitioner => Partition, %% always send to the same partition
                  partition_count_refresh_interval_seconds => 0
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
  ok = application:stop(wolff),
  ok.

%% Test against a bad host.
%% No connection will be established at all.
%% Producer workers should not crash, async APIs should work.
bad_host_test() ->
  ClientId = <<"bad-host-test">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  {ok, _} = wolff:ensure_supervised_client(ClientId, [{"badhost", 9092}], #{}),
  ?assertMatch({error, _}, wolff:ensure_supervised_producers(ClientId, <<"t">>, #{})),
  ok = wolff:stop_and_delete_supervised_client(ClientId).

producer_restart_test() ->
  ClientId = <<"producer-restart">>,
  Topic = <<"test-topic">>,
  Partition = 0,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = #{},
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ProducerCfg = #{replayq_dir => "test-data/producer-restart-test",
                  required_acks => all_isr,
                  partitioner => Partition,
                  reconnect_delay_ms => 0
                 },
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg),
  #{workers := Ets} = Producers,
  GetPid = fun() -> [{Partition, Pid}] = ets:lookup(Ets, Partition), Pid end,
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
  ok = application:stop(wolff),
  ok.

stop_with_name_test() ->
  ClientId = <<"stop-with-name">>,
  Topic = <<"test-topic">>,
  Partition = 0,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = #{},
  {ok, _} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  Name = ?FUNCTION_NAME,
  ProducerCfg = #{required_acks => all_isr,
                  partitioner => Partition,
                  reconnect_delay_ms => 0,
                  name => Name
                 },
  {ok, _} = wolff:ensure_supervised_producers(ClientId, Topic, ProducerCfg),
  %% cleanup
  ok = wolff:stop_and_delete_supervised_producers(ClientId, Topic, Name),
  ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ?assertEqual([], supervisor:which_children(wolff_client_sup)),
  ok = application:stop(wolff),
  ok.

partition_count_refresh_test_() ->
  {timeout, 30, %% it takes time to alter topic via cli in docker container
   fun test_partition_count_refresh/0}.

test_partition_count_refresh() ->
  ClientId = <<"test-add-more-partitions">>,
  Topic = <<"test-topic-3">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = #{},
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  {ok, Connections0} = wolff_client:get_leader_connections(ClientPid, Topic),
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
  {Partition0, _} = wolff:send_sync(Producers, [Msg], 3000),
  ?assertEqual(Partitions0 - 1, Partition0),
  %% create a new partition
  ensure_partitions(Topic, Partitions0 + 1),
  %% wait for the new partition to be discovered
  timer:sleep(timer:seconds(IntervalSeconds * 2)),
  {Partition1, _} = wolff:send_sync(Producers, [Msg], 3000),
  ?assertEqual(Partitions0, Partition1).

non_existing_topic_test() ->
  ClientId = atom_to_binary(?FUNCTION_NAME),
  Topic = <<"non-existing-topic">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = #{},
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ?assertMatch({error, _}, wolff:ensure_supervised_producers(ClientId, Topic, #{name => ?FUNCTION_NAME})),
  ?assertMatch([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_producers(ClientId, Topic, ?FUNCTION_NAME),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok.

start_producers_with_dead_client_test() ->
  ClientId = atom_to_binary(?FUNCTION_NAME),
  Topic = <<"non-existing-topic">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ?assertMatch({error, _}, wolff:ensure_supervised_producers(<<"never-started">>, Topic, #{name => ?FUNCTION_NAME})),
  ?assertMatch([], supervisor:which_children(wolff_producers_sup)),
  ok = wolff:stop_and_delete_supervised_producers(ClientId, Topic, ?FUNCTION_NAME),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok.

%% helpers
wait_for_pid(F) ->
  Pid = F(),
  case is_pid(Pid) of
    true -> Pid;
    false -> timer:sleep(100), wait_for_pid(F)
  end.

fetch_and_match(ClientPid, Topic, Partition, Offset, ExpectedMsgs) ->
  {ok, Conns} = wolff_client:get_leader_connections(ClientPid, Topic),
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
  Req = kpro_req_lib:fetch(_Vsn = 0, Topic, Partition, Offset, Opts),
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

producer_config() ->
  #{replayq_dir => "test-data"}.

key(Name) ->
  iolist_to_binary(io_lib:format("~p/~p/~p", [Name, calendar:local_time(),
                                              erlang:system_time()])).

count_partitions(Topic) ->
    Cmd = kafka_topic_cmd_base(Topic) ++ " --describe | grep Configs | awk '{print $4}'",
    list_to_integer(string:strip(os:cmd(Cmd), right, $\n)).

ensure_partitions(Topic, Partitions) ->
    Cmd = kafka_topic_cmd_base(Topic) ++ " --alter --partitions " ++ integer_to_list(Partitions),
    Result = os:cmd(Cmd),
    Pattern = "Adding partitions succeeded!",
    ?assert(string:str(Result, Pattern) > 0),
    ok.

kafka_topic_cmd_base(Topic) when is_binary(Topic) ->
    kafka_topic_cmd_base(binary_to_list(Topic));
kafka_topic_cmd_base(Topic) ->
    "docker exec wolff-kafka-1 /opt/kafka/bin/kafka-topics.sh" ++
    " --zookeeper zookeeper:2181" ++
    " --topic '" ++ Topic ++ "'".
