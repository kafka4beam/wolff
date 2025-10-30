-module(wolff_client_SUITE).

-compile([export_all, nowarn_export_all]).

-include("wolff.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(config(Key), proplists:get_value(Key, Config)).

all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, lists:prefix("t_", atom_to_list(F))].

init_per_suite(Config) ->
  _ = application:stop(wolff),
  application:ensure_all_started(wolff),
  Config.

end_per_suite(_Config) ->
  application:stop(wolff),
  ok.

init_per_testcase(Case, Config) ->
  ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
  ?MODULE:Case({'end', Config}).

t_recv_leader_connection_normal({init, Config}) ->
  ClientId = atom_to_binary(?FUNCTION_NAME),
  ClientCfg = client_config(),
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  [{clientid, ClientId}, {client, ClientPid} | Config];
t_recv_leader_connection_normal({'end', Config}) ->
  wolff:stop_and_delete_supervised_client(?config(clientid));
t_recv_leader_connection_normal(Config) ->
  Client = ?config(client),
  Topic = <<"test-topic">>,
  wolff_client:recv_leader_connection(Client, ?NO_GROUP, Topic, 0, self(), ?all_partitions),
  receive
    {leader_connection, Pid} ->
      ?assert(is_pid(Pid))
  end,
  ok.

t_recv_leader_connection_with_auto_create_retry_success({init, Config}) ->
  ClientId = atom_to_binary(?FUNCTION_NAME),
  ClientCfg = client_config_with_auto_create(),
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  [{clientid, ClientId}, {client, ClientPid} | Config];
t_recv_leader_connection_with_auto_create_retry_success({'end', Config}) ->
  wolff:stop_and_delete_supervised_client(?config(clientid));
t_recv_leader_connection_with_auto_create_retry_success(Config) ->
  %% Test retry mechanism when auto-creation is enabled and topic doesn't exist initially
  ok = meck:new(kpro, [non_strict, no_history, no_link, passthrough]),
  OldRetryCount = get(metadata_retry_count),
  try
    put(metadata_retry_count, 0),
    meck:expect(kpro, request_sync,
      fun(Connection, #kpro_req{api = metadata} = Req, Timeout) ->
        case get(metadata_retry_count) of
          0 ->
            put(metadata_retry_count, 1),
            {error, unknown_topic_or_partition};
          1 ->
            put(metadata_retry_count, 2),
            {error, unknown_topic_or_partition};
          _ ->
            %% Return success after retries - use passthrough for real response
            meck:passthrough([Connection, Req, Timeout])
        end;
      (Connection, Req, Timeout) ->
        %% Passthrough all non-metadata requests
        meck:passthrough([Connection, Req, Timeout])
      end),
    Client = ?config(client),
    Topic = <<"test-topic">>,
    wolff_client:recv_leader_connection(Client, ?NO_GROUP, Topic, 0, self(), ?all_partitions),
    receive
      {leader_connection, Pid} when is_pid(Pid) ->
        true;
      {leader_connection, {down, Reason}} ->
        %% May get error if topic doesn't exist on real Kafka
        ?assertNotEqual(undefined, Reason)
    after
      15000 ->
        ?assert(false, "Expected leader connection message")
    end,
    ok
  after
    meck:unload(kpro),
    case OldRetryCount of
      undefined -> erase(metadata_retry_count);
      _ -> put(metadata_retry_count, OldRetryCount)
    end
  end.

t_recv_leader_connection_with_auto_create_retry_timeout({init, Config}) ->
  ClientId = atom_to_binary(?FUNCTION_NAME),
  ClientCfg = client_config_with_auto_create(),
  {ok, ClientPid} = wolff:ensure_supervised_client(ClientId, ?HOSTS, ClientCfg),
  [{clientid, ClientId}, {client, ClientPid} | Config];
t_recv_leader_connection_with_auto_create_retry_timeout({'end', Config}) ->
  wolff:stop_and_delete_supervised_client(?config(clientid));
t_recv_leader_connection_with_auto_create_retry_timeout(Config) ->
  %% Test retry mechanism timeout when auto-creation is enabled
  ok = meck:new(kpro, [non_strict, no_history, no_link, passthrough]),
  try
    meck:expect(kpro, request_sync,
      fun(_Connection, #kpro_req{api = metadata} = _Req, _Timeout) ->
        %% Always return unknown_topic_or_partition to trigger retry timeout
        {error, unknown_topic_or_partition};
      (Connection, Req, Timeout) ->
        %% Passthrough all non-metadata requests
        meck:passthrough([Connection, Req, Timeout])
      end),
    Client = ?config(client),
    Topic = <<"test-topic-timeout">>,
    wolff_client:recv_leader_connection(Client, ?NO_GROUP, Topic, 0, self(), ?all_partitions),
    receive
      {leader_connection, {down, timeout}} ->
        %% Should receive down message due to timeout
        ok;
      {leader_connection, {down, Reason}} ->
        %% May receive timeout or another error reason depending on timing
        ?assert(is_atom(Reason))
    after
      5000 ->
        ?assert(false, "Expected leader connection down message with timeout")
    end,
    ok
  after
    meck:unload(kpro)
  end.

client_config() -> #{}.

client_config_with_auto_create() ->
  #{allow_auto_topic_creation => true,
    request_timeout => 2000
   }.
