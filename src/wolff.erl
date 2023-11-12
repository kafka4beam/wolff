%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(wolff).

%% Supervised client management APIs
-export([ensure_supervised_client/3,
         stop_and_delete_supervised_client/1
        ]).

%% Primitive producer worker management APIs
-export([start_producers/3,
         stop_producers/1
        ]).

%% Supervised producer management APIs
-export([ensure_supervised_producers/3,
         stop_and_delete_supervised_producers/1,
         stop_and_delete_supervised_producers/3
        ]).

%% Messaging APIs
-export([send/3,
         send_sync/3
        ]).

-export([check_connectivity/1,
         check_connectivity/2,
         check_if_topic_exists/2,
         check_if_topic_exists/3]).

%% for test
-export([get_producer/2]).

-export_type([client_id/0, host/0, producers/0, msg/0, ack_fun/0, partitioner/0,
              name/0, offset_reply/0]).

-type client_id() :: binary().
-type host() :: kpro:endpoint().
-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type name() :: atom() | binary().
-type offset() :: kpro:offset().
-type offset_reply() :: offset() | buffer_overflow_discarded.
-type producers_cfg() :: wolff_producers:config().
-type producers() :: wolff_producers:producers().
-type partitioner() :: wolff_producers:partitioner().

-type msg() :: #{key := binary(),
                 value := binary(),
                 ts => pos_integer(),
                 headers => [{binary(), binary()}]
                }.
-type ack_fun() :: fun((partition(), offset_reply()) -> ok)
                 | {fun(), [term()]}. %% apply(F, [Partition, Offset | Args])

%% @doc Start supervised client process.
-spec ensure_supervised_client(client_id(), [host()], wolff_client:config()) ->
  {ok, pid()} | {error, any()}.
ensure_supervised_client(ClientId, Hosts, Config) ->
  wolff_client_sup:ensure_present(ClientId, Hosts, Config).

%% @doc Stop and delete client under supervisor.
-spec stop_and_delete_supervised_client(client_id()) -> ok.
stop_and_delete_supervised_client(ClientId) ->
  wolff_client_sup:ensure_absence(ClientId).

%% @doc Start producers with the per-partition workers linked to caller.
-spec start_producers(pid(), topic(), producers_cfg()) -> {ok, producers()} | {error, any()}.
start_producers(Client, Topic, ProducerCfg) when is_pid(Client) ->
  wolff_producers:start_linked_producers(Client, Topic, ProducerCfg).

%% @doc Stop linked producers.
-spec stop_producers(#{workers := map(), _ => _}) -> ok.
stop_producers(Producers) ->
  wolff_producers:stop_linked(Producers).

%% @doc Ensure supervised producers are started.
-spec ensure_supervised_producers(client_id(), topic(), producers_cfg()) ->
  {ok, producers()} | {error, any()}.
ensure_supervised_producers(ClientId, Topic, ProducerCfg) ->
  wolff_producers:start_supervised(ClientId, Topic, ProducerCfg).

%% @doc Ensure supervised producers are stopped then deleted.
-spec stop_and_delete_supervised_producers(client_id(), topic(), name()) -> ok.
stop_and_delete_supervised_producers(ClientId, Topic, Name) ->
  wolff_producers:stop_supervised(ClientId, Topic, Name).

%% @doc Ensure supervised producers are stopped then deleted.
-spec stop_and_delete_supervised_producers(wolff_producers:producers()) -> ok.
stop_and_delete_supervised_producers(Producers) ->
  wolff_producers:stop_supervised(Producers).

%% @doc Pick a partition producer and send a batch asynchronously.
%% The callback function is evaluated by producer process when ack is received from kafka.
%% In case `required_acks' is configured to `none', the callback is evaluated immediately after send.
%% The partition number and the per-partition worker pid are returned in a tuple to caller,
%% so it may use them to correlate the future `AckFun' evaluation.
%% NOTE: This API has no backpressure,
%%       high produce rate may cause execussive ram and disk usage.
%% NOTE: It's possible that two or more batches get included into one produce request.
%%       But a batch is never split into produce requests.
%%       Make sure it will not exceed the `max_batch_bytes' limit when sending a batch.
%% NOTE: In case producers are configured with `required_acks = none',
%%       the second arg for callback function will always be `?UNKNOWN_OFFSET' (`-1').
-spec send(producers(), [msg()], ack_fun()) -> {partition(), pid()}.
send(Producers, Batch, AckFun) ->
  {Partition, ProducerPid} = wolff_producers:pick_producer(Producers, Batch),
  ok = wolff_producer:send(ProducerPid, Batch, AckFun),
  {Partition, ProducerPid}.

%% @doc Pick a partition producer and send a batch synchronously.
%% Raise error exception in case produce pid is down or when timed out.
%% NOTE: In case producers are configured with `required_acks => none',
%%       the returned offset will always be `?UNKNOWN_OFFSET' (`-1').
%%       In case the batch is discarded due to buffer overflow, the offset
%%       is `buffer_overflow_discarded'.
-spec send_sync(producers(), [msg()], timeout()) -> {partition(), offset_reply()}.
send_sync(Producers, Batch, Timeout) ->
  {_Partition, ProducerPid} = wolff_producers:pick_producer(Producers, Batch),
  wolff_producer:send_sync(ProducerPid, Batch, Timeout).

%% @hidden For test only.
get_producer(Producers, Partition) ->
  wolff_producers:lookup_producer(Producers, Partition).

%% @doc Check if the client is connected to the cluster.
-spec check_connectivity(client_id()) ->
        ok | {error, [{FormatedHostPort :: binary(), any()}]}.
check_connectivity(ClientId) ->
    case wolff_client_sup:find_client(ClientId) of
      {ok, Pid} -> wolff_client:check_connectivity(Pid);
      {error, Error} -> {error, Error}
    end.

%% @doc Check if the cluster is reachable.
-spec check_connectivity([host()], wolff_client:config()) ->
        ok | {error, [{FormatedHostPort :: binary(), any()}]}.
check_connectivity(Hosts, ConnConfig) ->
   wolff_client:check_connectivity(Hosts, ConnConfig).

%% @doc Check if the cluster is reachable and the topic is created.
-spec check_if_topic_exists([host()], wolff_client:config(), topic()) ->
        ok | {error, unknown_topic_or_partition | [#{host := binary(), reason := term()}] | any()}.
check_if_topic_exists(Hosts, ConnConfig, Topic) ->
  wolff_client:check_if_topic_exists(Hosts, ConnConfig, Topic).

%% @doc Check if a topic exists using a supervised client or a client porcess.
-spec check_if_topic_exists(client_id() | pid(), topic()) -> ok | {error, unknown_topic_or_partition | any()}.
check_if_topic_exists(ClientId, Topic) when is_binary(ClientId) ->
  case wolff_client_sup:find_client(ClientId) of
    {ok, Pid} -> check_if_topic_exists(Pid, Topic);
    {error, Error} -> {error, Error}
  end;
check_if_topic_exists(ClientPid, Topic) when is_pid(ClientPid) ->
  wolff_client:check_topic_exists_with_client_pid(ClientPid, Topic).
