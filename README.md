## NOTICE

This lib is a fork of the upstream repo disclosed by
[EMQ Technologies Co., Ltd.](https://www.emqx.io)
GitHub/[emqx](https://github.com/emqx)

## Wolff

Kafka's publisher. See [why the name](https://en.wikipedia.org/wiki/Kurt_Wolff_(publisher))

## How is it different from [brod](https://github.com/kafka4beam/brod)

### More resilient to network and Kafka disturbances

With `replayq_dir` producer config set to a directory,
`wolff` will queue pending messages on disk so it can survive from message loss
in case of application, network or kafka disturbances.

In case of producer restart, messages queued on disk are replayed towards kafka,
however, async callback functions are not evaluated upon acknowledgements received
from kafka for replayed messages.

### More flexible connection management

`wolff` provides `per_partition` and `per_broker` connection management strategy.
In case of `per_partition` strategy, `wolff` establishes one TCP connection
per-partition leader. `brod` however, only establishes connections per-broker,
that is, if two partition leaders happen to reside on the same broker,
they will have to share the same TCP connection.

There is still a lack of benchmarking to tell the difference of how performant
`per_partition` is though.

### Auto partition count refresh

`wolff` periodically refreshes topic metata to discover partition increase
and automatically rebalance the partitioner.

## Example Code

### Sync Produce

```
application:ensure_all_started(wolff).
ClientCfg = #{}.
{ok, Client} = wolff:ensure_supervised_client(<<"client-1">>, [{"localhost", 9092}], ClientCfg).
ProducerCfg = #{replayq_dir => "/tmp/wolff-replayq-1"}.
{ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg).
Msg = #{key => <<"key">>, value => <<"value">>}.
{Partition, BaseOffset} = wolff:send_sync(Producers, [Msg], 3000).
io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
          [Partition, BaseOffset]).
ok = wolff:stop_producers(Producers).
ok = wolff:stop_client(Client).
```

If you want to use more than one producer pointing to the same topic, be sure to define an unique `alias` for each one to avoid clashes.

```erlang
Topic = <<"test-topic">>.
{ok, Producers1} = wolff:start_producers(Client, Topic, ProducerCfg#{alias => <<"a1">>}).
{ok, Producers2} = wolff:start_producers(Client, Topic, ProducerCfg#{alias => <<"a2">>}).
```

### Async Produce with Callback

```
application:ensure_all_started(wolff).
ClientCfg = #{}.
{ok, Client} = wolff:ensure_supervised_client(<<"client-2">>, [{"localhost", 9092}], ClientCfg).
ProducerCfg = #{replayq_dir => "/tmp/wolff-replayq-2"}.
{ok, Producers} = wolff:start_producers(Client, <<"test-topic">>, ProducerCfg).
Msg = #{headers => [{<<"foo">>, <<"bar">>}], key => <<"key">>, value => <<"value">>}.
Self = self().
AckFun = fun(Partition, BaseOffset) ->
            io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
                      [Partition, BaseOffset]),
            ok
         end.
wolff:send(Producers, [Msg], AckFun).
```

For upgrade safety, it's recommended to avoid using anonymous function as ack callback.
In production code, the caller should provide a `{fun module:handle_ack/3, [ExtraArg]}` for `AckFun`.
The `handle_ack` function should expect first two args as `Partition` and `BaseOffset`,
and the third arg `ExtraArg` is served back to the caller. For example.

```
-export([handle_ack/3]).

send(...) ->
  wolff:send(Producers, Messages, {fun ?MODULE/handle_ack, [self()]}).

handle_ack(Partition, Offset, Caller) ->
  Caller ! {kafka_acked, Partition, Offset},
  ok. % must return ok

```

### Supervised Producers

```
application:ensure_all_started(wolff).
Client = <<"client-1">>.
ClientCfg = #{}.
{ok, _ClientPid} = wolff:ensure_supervised_client(Client, [{"localhost", 9092}], ClientCfg).
ProducerCfg = #{replayq_dir => "/tmp/wolff-replayq-3"}.
{ok, Producers} = wolff:ensure_supervised_producers(Client, <<"test-topic">>, ProducerCfg).
Msg = #{headers => [{<<"foo">>, <<"bar">>}], key => <<"key">>, value => <<"value">>}.
Self = self().
AckFun = fun(Partition, BaseOffset) ->
            io:format(user, "\nmessage produced to partition ~p at offset ~p\n",
                      [Partition, BaseOffset]),
            ok
         end.
wolff:send(Producers, [Msg], AckFun).
```

## Client Config

* `reg_name` register the client process to a name. e.g. `{local, client_1}`

* `connection_strategy`: default `per_partition`, can also be `per_broker`.
   This is to configure how client manages connections: one connection
   per-partition or one connection per-broker.
   `per_partition` may give better throughput, but it could be quite exhausting
   for both beam and kafka cluster when there is a great number of partitions

* `min_metadata_refresh_interval`: default 1000 (milliseconds).
   This is to avoid excessive metadata refresh and partition leader reconnect
   when a lot of connections restart around the same moment.
   Also, when kafka partition leader broker is down, it usually takes a few
   seconds to get a new leader elacted, hence it is a good idea to have
   a delay before trying to reconnect.

* Connection level configs are merged into `wolff` client config, including:
  `connect_timeout`, `client_id`, `extra_sock_opts`, `query_api_versions`,
  `request_timeout`, `sasl` and `ssl`.
  Ref: [kpro_connection.erl](https://github.com/klarna/kafka_protocol/blob/master/src/kpro_connection.erl)

## Producer Config

* `replayq_dir`: Base directory for `replayq` to store messages on disk.
   If this config entry if missing or set to `undefined`, replayq works in a mem-only
   manner. i.e. messages are not queued on disk -- in such case, the `send` or `send_sync`
   API callers are responsible for possible message loss in case of application,
   network or kafka disturbances. For instance, in the `wolff:send` API caller may
   `trap_exit` then react on parition-producer worker pid's `'EXIT'` message to issue
   a retry after restarting the producer.

* `replayq_seg_bytes`: default=10MB, replayq segment size.

* `required_acks`: `all_isr`, `leader_only` or `none`, see `kafka_protocol` lib doc.

* `ack_timeout`: default=10000. Timeout leader wait for replicas before reply to producer.

* `max_batch_bytes`: Most number of bytes to collect into a produce request.
   NOTE: This is only a rough estimation of the batch size towards kafka,
         NOT the exact max allowed message size configured in kafka.

* `max_linger_ms`: Age in milliseconds a batch can stay in queue when the connection
   is idle (as in no pending acks from kafka). Default=0 (as in send immediately).

* `max_linger_bytes`: Number of bytes to collect before sending it to Kafka. If set to 0, `max_batch_bytes` is taken for mem-only mode, otherwise it's 10 times `max_batch_bytes` (but never exceeds 10MB) to optimize disk write.

* `max_send_ahead`: Number of batches to be sent ahead without receiving ack for
   the last request. Must be 0 if messages must be delivered in strict order.

* `compression`: `no_compression` (default) `snappy` or `gzip`.

* `partitioner`: default `random`. other possible values are:
   `first_key_dispatch`: `erlang:phash2(Key) rem PartitionCount` where Key is the `key`
   field of the first message in the batch to produce.
   `fun((PartitionCount, [msg()]) -> partition())`: Caller defined callback.
   `partition()`: Caller specified exact partition.

* `name`: `atom() | binary()`, Mandatory when started under wolff's supervision tree.
   The name, (eg. `{clientid}-{topicname}`) should be globally unique as it
   is used as the namespace for partition-producder process registration.
   An atom name is also used to register `wolff_producers` process.

* `partition_count_refresh_interval_seconds`: default=`300` (5 minutes)
  Non-negative integer to refresh topic metadata in order to auto-discover newly added partitions.
  Set `0` to disable auto-discover.

## Beam Telemetry Hooks

[Beam Telemetry](https://github.com/beam-telemetry/telemetry) is a library for
defining telemetry events. Wolff defines such telemetry events. Users of Wolff
can attach functions to the events, for example, to record when a message has
been successfully sent to Kafka. Wolff's telemetry events are described in the
`wolff_metrics` module. One can read more about how to attach code to the
events in [Beam Telemetry's documentation](https://github.com/beam-telemetry/telemetry).
The third parameter of the Beam Telemetry handler function is a meta data map.
One can send a custom meta data map for each Kafka producer instance by setting
the Kafka producer configuration parameter `telemetry_meta_data` to the map one
wants to use.

## How to Test

Start Kafka in docker containers from dokcer-compose files in ./scripts.

```
# Set Kafka version.
# See supported versions [here](https://github.com/zmstone/docker-kafka/blob/master/Makefile)
export KAFKA_VERSION=4.0
. scripts/env
make test-env
make eunit
make ct
```

## License

Apache License Version 2.0
