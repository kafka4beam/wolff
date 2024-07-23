-module(wolff_dynamic_topics_tests).

-include("wolff.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").

-define(KEY, key(?FUNCTION_NAME)).
-define(HOSTS, [{"localhost", 9092}]).

%% Checks that having different producers to the same kafka topic works.
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
  {ok, Producers} = wolff:ensure_supervised_producers(ClientId, [], ProducerCfg),
  ?assertEqual({ok, Producers}, wolff:ensure_supervised_producers(ClientId, [], ProducerCfg)),
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
  ?assertMatch(#{known_topics := Topics,
                 conns := Conns
                } when map_size(Topics) =:= 0 andalso map_size(Conns) =:= 0,
               sys:get_state(ClientPid)),
  ok = wolff:stop_and_delete_supervised_client(ClientId),
  ok = application:stop(wolff),
  ok.

unknown_topic_expire_test() ->
  ok.

%% helpers

client_config() -> #{}.

key(Name) ->
  iolist_to_binary(io_lib:format("~0p/~0p/~0p", [Name, calendar:local_time(), erlang:system_time()])).
