-module(wolff_noack_alias_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").

no_ack_memory_test_() ->
  {timeout, 120, fun no_ack_memory/0}.

no_ack_memory() ->
  assert_allocator_tagging(),
  Hosts = [{"localhost", 9092}],
  Topic = <<"test-topic">>,
  {ok, _} = application:ensure_all_started(wolff),
  {ok, Client} = wolff_client:start_link(<<"no-ack-memory-test">>, Hosts, #{}),
  Config = #{required_acks => none, partitioner => fun(_, _) -> 0 end},
  {ok, Producers} = wolff:start_producers(Client, Topic, Config),
  {ok, Conn} = kpro:connect_partition_leader(Hosts, #{}, Topic, 0),
  Producer = wolff:get_producer(Producers, 0),
  try
    Send = fun(_) ->
      {0, -1} = wolff:send_sync(Producers, [#{key => <<>>, value => <<"no-ack">>}], 5000)
    end,
    %% Warm up the producer and connection before measuring live allocations.
    Offset0 = latest_offset(Conn, Topic),
    Send(warmup),
    wait_for_offset(Conn, Topic, Offset0 + 1),
    {Before, 0} = alias_allocations(),
    lists:foreach(Send, lists:seq(1, 100000)),
    wait_for_offset(Conn, Topic, Offset0 + 100001),
    {After, UnscannedBytes} = alias_allocations(),
    io:format(user, "std_alloc aliases: before=~p, after=~p, unscanned_bytes=~p~n",
              [Before, After, UnscannedBytes]),
    %% Measure before stopping the producer, which would release leaked aliases.
    ?assert(is_process_alive(Producer)),
    ?assert(After =< Before),
    %% A partial scan cannot establish that no allocations leaked.
    ?assertEqual(0, UnscannedBytes)
  after
    kpro:close_connection(Conn),
    wolff:stop_producers(Producers),
    wolff_client:stop(Client)
  end.

assert_allocator_tagging() ->
  [_ | _] = Instances = erlang:system_info({allocator, std_alloc}),
  lists:foreach(fun({instance, _, Info}) ->
    Options = proplists:get_value(options, Info),
    ?assertEqual(true, proplists:get_value(atags, Options))
  end, Instances).

alias_allocations() ->
  {ok, {_, UnscannedBytes, Allocations}} = instrument:allocations(#{allocator_types => [std_alloc]}),
  Count = maps:fold(fun(_, Types, Acc) ->
    Acc + lists:sum(tuple_to_list(maps:get(alias, Types, {})))
  end, 0, Allocations),
  {Count, UnscannedBytes}.

latest_offset(Conn, Topic) ->
  {ok, {_, Vsn}} = kpro:get_api_vsn_range(Conn, list_offsets),
  Req = kpro_req_lib:list_offsets(Vsn, Topic, 0, latest),
  {ok, #kpro_rsp{msg = Rsp}} = kpro:request_sync(Conn, Req, 5000),
  [TopicRsp] = kpro:find(responses, Rsp),
  [PartitionRsp] = kpro:find(partition_responses, TopicRsp),
  no_error = kpro:find(error_code, PartitionRsp),
  kpro:find(offset, PartitionRsp).

wait_for_offset(Conn, Topic, Expected) ->
  Deadline = erlang:monotonic_time(millisecond) + 60000,
  wait_for_offset(Conn, Topic, Expected, Deadline).

wait_for_offset(Conn, Topic, Expected, Deadline) ->
  case latest_offset(Conn, Topic) of
    Expected -> ok;
    Actual ->
      ?assert(Actual < Expected),
      ?assert(erlang:monotonic_time(millisecond) < Deadline),
      timer:sleep(100),
      wait_for_offset(Conn, Topic, Expected, Deadline)
  end.
