-module(wolff_bench).

-export([start/0, start/1]).

-define(TOPIC, <<"wolff-bench">>).

start() -> start(2000).

start(WorkersCnt) ->
  {ok, _} = application:ensure_all_started(wolff),
  ClientId = <<"wolff-bench">>,
  ClientConfig = #{},
  {ok, Client} = wolff:ensure_supervised_client(ClientId, [{"localhost", 9092}], ClientConfig),
  {ok, Producers} = start_producers(Client),
  SendFun = fun(Msgs) ->
                {_, _} = wolff:send_sync(Producers, Msgs, timer:seconds(10))
            end,
  ok = spawn_workers(SendFun, WorkersCnt).

start_producers(Client) ->
  ProducerCfg = #{required_acks => all_isr,
                  max_batch_bytes => 800*1000,
                  max_linger_ms => 1000,
                  max_send_ahead => 100
                 },
  wolff:start_producers(Client, ?TOPIC, ProducerCfg).

spawn_workers(_SendFun, 0) -> ok;
spawn_workers(SendFun, N) ->
  erlang:spawn_link(
    fun() ->
        timer:sleep(rand:uniform(10)),
        worker_loop(SendFun)
    end),
  spawn_workers(SendFun, N - 1).

worker_loop(SendFun) ->
  Value = binary:copy(<<0>>, 1000),
  Msgs = [#{key => <<I>>, value => Value} || I <- lists:seq(1,100)],
  SendFun(Msgs),
  worker_loop(SendFun).
