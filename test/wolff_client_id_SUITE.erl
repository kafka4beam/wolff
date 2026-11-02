%% Observe the client ID on the wire through wolff's public connectivity API.
-module(wolff_client_id_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").

all() -> [configured_client_id, default_client_id, undefined_client_id].
init_per_suite(Config) ->
  {ok, Apps} = application:ensure_all_started(wolff),
  [{apps, Apps} | Config].
end_per_suite(Config) ->
  lists:foreach(fun application:stop/1, lists:reverse(proplists:get_value(apps, Config))).
init_per_testcase(_, Config) ->
  {ok, Listener} = gen_tcp:listen(0, [binary, {packet,4}, {active,false}, {ip,{127,0,0,1}}]),
  {ok, Port} = inet:port(Listener),
  Owner = self(),
  Broker = spawn_link(fun() -> accept_loop(Listener, Owner, Port) end),
  [{listener, Listener}, {port, Port}, {broker, Broker} | Config].
end_per_testcase(_, Config) ->
  Broker = proplists:get_value(broker, Config),
  unlink(Broker),
  exit(Broker, shutdown),
  gen_tcp:close(proplists:get_value(listener, Config)).
accept_loop(Listener, Owner, Port) ->
  case gen_tcp:accept(Listener) of
    {ok, Sock} ->
      spawn_link(fun() -> connection_loop(Sock, Owner, Port) end),
      accept_loop(Listener, Owner, Port);
    {error, closed} -> ok
  end.
connection_loop(Sock, Owner, Port) ->
  case gen_tcp:recv(Sock, 0, 5000) of
    {ok, <<API:16, 0:16, Corr:32, Len:16, ClientId:Len/binary, Body/binary>>} ->
      Owner ! {client_id, self(), API, ClientId},
      Response = response(API, Body, Port),
      ok = gen_tcp:send(Sock, <<Corr:32, Response/binary>>),
      connection_loop(Sock, Owner, Port);
    {error, _} -> gen_tcp:close(Sock)
  end.
response(18, _, _) -> <<0:16, 1:32, 3:16, 0:16, 0:16>>;
response(3, <<1:32, Len:16, Topic:Len/binary>>, Port) ->
  <<1:32, 0:32, 9:16, "127.0.0.1", Port:32, 1:32, 0:16, Len:16, Topic/binary,
    1:32, 0:16, 0:32, 0:32, 1:32, 0:32, 1:32, 0:32>>.
configured_client_id(Config) ->
  probe(<<"internal-client">>, #{client_id => <<"wire-client">>}, <<"wire-client">>, Config).
default_client_id(Config) ->
  probe(<<"default-client">>, #{}, <<"default-client">>, Config).
undefined_client_id(Config) ->
  probe(<<"undefined-client">>, #{client_id => undefined}, <<"undefined-client">>, Config).
probe(ClientId, ClientConfig, Expected, Config) ->
  Hosts = [{"127.0.0.1", proplists:get_value(port, Config)}],
  {ok, Pid} = wolff:ensure_supervised_client(ClientId, Hosts, ClientConfig),
  try
    ok = wolff_client:check_connectivity(Pid),
    {ok, [{0, Leader}]} = wolff_client:get_leader_connections(Pid, <<"test-group">>, <<"test-topic">>),
    ?assert(is_pid(Leader)),
    Requests = collect_requests(),
    ?assert(lists:all(fun({_, _, Value}) -> Value =:= Expected end, Requests)),
    ?assert(length(lists:usort([Conn || {Conn, 18, _} <- Requests])) >= 2),
    ?assert(lists:any(fun({_, API, _}) -> API =:= 3 end, Requests)),
    ?assertEqual(ClientId, wolff_client:get_id(Pid)),
    ?assertEqual({ok, Pid}, wolff_client_sup:find_client(ClientId))
  after wolff:stop_and_delete_supervised_client(ClientId) end.

collect_requests() ->
  receive {client_id, Conn, API, Value} -> [{Conn, API, Value} | collect_requests()]
  after 0 -> [] end.
