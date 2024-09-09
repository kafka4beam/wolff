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

-include("wolff.hrl").

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
         ensure_supervised_dynamic_producers/2,
         stop_and_delete_supervised_producers/1
        ]).

%% Messaging APIs
-export([send/3,
         send_sync/3,
         cast/3
        ]).

%% Messaging APIs of dynamic producer.
-export([send2/4,
         cast2/4,
         send_sync2/4,
         add_topic/2,
         remove_topic/2
        ]).

-export([check_connectivity/1,
         check_connectivity/2,
         check_if_topic_exists/2,
         check_if_topic_exists/3]).

%% for test
-export([get_producer/2]).

-export_type([client_id/0, host/0, producers/0, msg/0, ack_fun/0, partitioner/0,
              name/0, offset_reply/0, topic/0, gname/0]).

-deprecated({check_if_topic_exists, 3}).

-type gname() :: wolff_producers:gname().
-type client_id() :: binary().
-type host() :: kpro:endpoint().
-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type name() :: atom() | binary().
-type offset() :: kpro:offset().
-type offset_reply() :: offset() | buffer_overflow_discarded | message_too_large.
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

%% @doc Ensure supervised dynamic-producers are started.
-spec ensure_supervised_dynamic_producers(client_id(), producers_cfg()) ->
  {ok, producers()} | {error, any()}.
ensure_supervised_dynamic_producers(ClientId, ProducerCfg) ->
  wolff_producers:start_supervised_dynamic(ClientId, ProducerCfg).

%% @doc Ensure supervised producers are stopped then deleted.
-spec stop_and_delete_supervised_producers(wolff_producers:producers()) -> ok.
stop_and_delete_supervised_producers(Producers) ->
  wolff_producers:stop_supervised(Producers).

%% @doc Pick a partition producer and send a batch asynchronously.
%% The callback function is evaluated by producer process when ack is received from kafka.
%% In case `required_acks' is configured to `none', the callback is evaluated immediately after send.
%% The partition number and the per-partition worker pid are returned in a tuple to caller,
%% so it may use them to correlate the future `AckFun' evaluation.
%% NOTE: This API is blocked until the batch is enqueued to the producer buffer, otherwise no backpressure.
%%       High produce rate may cause excessive ram and disk usage.
%% NOTE: In case producers are configured with `required_acks = none',
%%       the second arg for callback function will always be `?UNKNOWN_OFFSET' (`-1').
-spec send(producers(), [msg()], ack_fun()) -> {partition(), pid()}.
send(Producers, Batch, AckFun) ->
  {Partition, ProducerPid} = wolff_producers:pick_producer(Producers, Batch),
  ok = wolff_producer:send(ProducerPid, Batch, AckFun),
  {Partition, ProducerPid}.

%% @doc Topic as argument for dynamic producers, otherwise equivalent to `send/3'.
-spec send2(producers(), topic(), [msg()], ack_fun()) -> {partition(), pid()}.
send2(Producers, Topic, Batch, AckFun) ->
  {Partition, ProducerPid} = wolff_producers:pick_producer2(Producers, Topic, Batch),
  ok = wolff_producer:send(ProducerPid, Batch, AckFun),
  {Partition, ProducerPid}.

%% @doc Cast a batch to a partition producer.
%% Even less backpressure than `send/3'.
%% It does not wait for the batch to be enqueued to the producer buffer.
-spec cast(producers(), [msg()], ack_fun()) -> {partition(), pid()}.
cast(Producers, Batch, AckFun) ->
  {Partition, ProducerPid} = wolff_producers:pick_producer(Producers, Batch),
  ok = wolff_producer:send(ProducerPid, Batch, AckFun, no_wait_for_queued),
  {Partition, ProducerPid}.

%% @doc Topic as argument for dynamic producers, otherwise equivalent to `cast/3'.
-spec cast2(producers(), topic(), [msg()], ack_fun()) -> {partition(), pid()}.
cast2(Producers, Topic, Batch, AckFun) ->
  {Partition, ProducerPid} = wolff_producers:pick_producer2(Producers, Topic, Batch),
  ok = wolff_producer:send(ProducerPid, Batch, AckFun, no_wait_for_queued),
  {Partition, ProducerPid}.

%% @doc Pick a partition producer and send a batch synchronously.
%% Raise error exception in case produce pid is down or when timed out.
%% NOTE: In case producers are configured with `required_acks => none',
%%       the returned offset will always be `?UNKNOWN_OFFSET' (`-1').
%%       In case the batch is discarded due to buffer overflow, the offset
%%       is `buffer_overflow_discarded'.
%%       In case a single message is too large (Kafka topic config max.message.bytes)
%%       the offset is `message_too_large'.
-spec send_sync(producers(), [msg()], timeout()) -> {partition(), offset_reply()}.
send_sync(Producers, Batch, Timeout) ->
  {_Partition, ProducerPid} = wolff_producers:pick_producer(Producers, Batch),
  wolff_producer:send_sync(ProducerPid, Batch, Timeout).

%% @doc Topic as argument for dynamic producers, otherwise equivalent to `send_sync/3'.
-spec send_sync2(producers(), topic(), [msg()], timeout()) -> {partition(), offset_reply()}.
send_sync2(Producers, Topic, Batch, Timeout) ->
  {_Partition, ProducerPid} = wolff_producers:pick_producer2(Producers, Topic, Batch),
  wolff_producer:send_sync(ProducerPid, Batch, Timeout).

%% @doc Add a topic to dynamic producer.
%% Returns `ok' if the topic is already addded.
-spec add_topic(producers(), topic()) -> ok | {error, any()}.
add_topic(Producers, Topic) ->
  wolff_producers:add_topic(Producers, Topic).

%% @doc Remove a topic from dynamic producer.
%% Returns `ok' if the topic is already removed.
-spec remove_topic(producers(), topic()) -> ok.
remove_topic(Producers, Topic) ->
  wolff_producers:remove_topic(Producers, Topic).

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

%% @hidden Deprecated. Check if the cluster is reachable and the topic is created.
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
