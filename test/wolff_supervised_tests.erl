-module(wolff_supervised_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").

-define(KEY, key(?FUNCTION_NAME)).
-define(HOSTS, [{"localhost", 9092}]).

supervised_client_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientId = <<"supervised-wolff-client">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = client_config(),
  {ok, Client} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  %% start it again should result in the same client pid
  ?assertEqual({ok, Client},
               wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg)),
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
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
  ok.

supervised_producers_test_() ->
  [{"atom-name", fun() -> test_supervised_producers(test_producers) end},
   {"binary-name", fun() -> test_supervised_producers(<<"test-producers">>) end}
  ].

test_supervised_producers(Name) ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientId = <<"supervised-producers">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
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
  ok = application:stop(wolff),
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  [1] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
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
  [1,1] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
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
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
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
  [1,2] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
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
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
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
  ?assertEqual(Partitions0, Partition1),
  [1,1] = get_telemetry_seq(CntrEventsTable, [wolff,success]),
  assert_last_event_is_zero(queuing, CntrEventsTable),
  assert_last_event_is_zero(inflight, CntrEventsTable),
  ets:delete(CntrEventsTable),
  wolff_tests:deinstall_event_logging(?FUNCTION_NAME),
  ok.

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

fail_retry_success_test() ->
  CntrEventsTable = ets:new(cntr_events, [public]),
  wolff_tests:install_event_logging(?FUNCTION_NAME, CntrEventsTable, false),
  ClientId = <<"supervised-producers">>,
  _ = application:stop(wolff), %% ensure stopped
  {ok, _} = application:ensure_all_started(wolff),
  ClientCfg = client_config(),
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  ProducerCfg0 = producer_config(),
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
      ok = application:stop(wolff),
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
     _ = application:stop(wolff), %% ensure stopped
     {ok, _} = application:ensure_all_started(wolff),
     ClientCfg = client_config(),
     {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
     ProducerCfg0 = producer_config(),
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
         ok = application:stop(wolff),
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
  producer_config(_Name = wolff_producers).

producer_config(Name) ->
  #{replayq_dir => "test-data",
    enable_global_stats => true,
    name => Name,
    workers_table => case is_atom(Name) of
      true -> undefined;
      false -> ets:new(?MODULE, [public])
    end
   }.

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
