-module(wolff_noack_alias_tests).

-include_lib("eunit/include/eunit.hrl").

completed_request_alias_test_() ->
  [{atom_to_list(Acks), {timeout, 30, fun() -> completed_request_alias(Acks) end}}
   || Acks <- [none, leader_only, all_isr]].

%% A completed request must not leave its reply alias active.
completed_request_alias(Acks) ->
  Hosts = [{os:getenv("WOLFF_TEST_KAFKA_HOST", "localhost"), 9092}],
  Topic = list_to_binary(os:getenv("WOLFF_TEST_KAFKA_TOPIC", "test-topic")),
  {ok, _} = application:ensure_all_started(wolff),
  {ok, Client} = wolff_client:start_link(<<"no-ack-alias-test">>, Hosts, #{}),
  {ok, Producers} = wolff:start_producers(Client, Topic, #{required_acks => Acks}),
  Producer = wolff:get_producer(Producers, 0),
  Self = self(),
  erlang:trace_pattern({wolff_sender, do, 8}, true, [local]),
  erlang:trace(Producer, true, [call, {tracer, Self}]),
  try
    {0, Offset} = wolff:send_sync(Producers, [#{key => <<>>, value => <<"alias-check">>}], 5000),
    case Acks of
      none -> ?assertEqual(-1, Offset);
      _ -> ?assert(Offset >= 0)
    end,
    Ref = receive
      {trace, Producer, call, {wolff_sender, do, [_, _, {alias, Alias} | _]}} -> Alias
    after 5000 -> error(missing_send_trace)
    end,
    sys:replace_state(Producer, fun(State) ->
      Self ! {alias_was_active, erlang:unalias(Ref)},
      State
    end),
    receive
      {alias_was_active, Active} -> ?assertEqual(false, Active)
    after 5000 -> error(missing_alias_check)
    end
  after
    erlang:trace(Producer, false, [call]),
    erlang:trace_pattern({wolff_sender, do, 8}, false, [local]),
    wolff:stop_producers(Producers),
    wolff_client:stop(Client)
  end.
