* 4.1.0
  - Fix 'failed' telemetry counter double-increment due to race condition. [#102](https://github.com/kafka4beam/wolff/pull/102)
  - Added producer process label `{wolff_producer, KafkaClientId, Topic, Partition}` [#102](https://github.com/kafka4beam/wolff/pull/102)
  - Upgrade to `kafka_protocol-4.3.0` for better CRC32C performance.
  - Add client config `allow_auto_topic_creation` (default = false).

* 4.0.13 (merge 1.5.19)
  - Handle `record_list_too_large` error returned from Kafka.
    Similar to `message_too_large`  error, the batch is split, then dropped if single call is still too large.

* 4.0.12 (merge 1.5.18)
  - Partition metadata handling.
    - Fixed an issue introduced in 4.0.7 (1.5.15) where temporarily missing partitions in the metadata response could leave a `wolff_producer` process permanently disconnected.
      `wolff_producer` now always attempts to reconnect, even if a partition disappears and reappears.
    - Ensured that partition numbers in the metadata response are always sequential, even when Kafka returns malformed metadata.

* 4.0.11
  - Upgrade to `kafka_protocol-4.2.8`. Fixed build speed and link issue for crc32c.

* 4.0.10
  - Upgrade to `kafka_protocol-4.2.6`.
    Update test environment to test against Kafka 4.0.0 (KRaft mode)

* 4.0.9 (merge 1.5.16)
  - Fix a remaining issue in 4.0.7: the pending acks in lost partitions should be replied to the caller, and the failed counters should be incremented.

* 4.0.8
  - Upgrade to `kafka_protocol-4.2.3` for crc32c performance improvements

* 4.0.7 (merge 1.5.15)
  - Upgrade to `kafka_protocol-4.2.2`
    - Support `zstd` compression.
    - Avoid `kpro_connection` crash log when socket error (terminates with `{shutdown, ErrorReason}`).
  - Handle topic recreation with fewer partitions.
    Previously, Wolff only handled topic alteration with more partitions, but not topic re-creation with fewer partitions.
    Now deleted partition producers will be gracefully shut down once new metadata is fetched, and the buffered messages will be dropped.
    NOTE: As before, topic deletion (unknown_topic_or_partition) does not cause all partition producers to shut down.
  - Improve logging for leader connection down reason.
    Previously, if the connection is closed immediately after connected, the producer process may not get the chance to monitor the pid to get its exit reason.
    Now wolff_client handles the 'EXIT' signal and keep it for future logging purpose.

* 4.0.6
  - Use more aggressive buffer overflow mode when using memory mode buffer and the system memory usage is high, to reduce risk of OOM.
  - Upgrade replayq from 0.3.10 to 0.3.12.
  - Upgrade lc from 0.3.2 to 0.3.4.

* 4.0.5
  - Fix `unexpected_id` crash introduced in 4.0.1.

* 4.0.4
  - Upgrade to kafka_protocol-4.1.10 for discover/connect timeout fix.
  - Upgrade to replayq from 0.3.4 to 0.3.10.

* 4.0.3
  - Added the `[wolff, queuing_bytes]` telemetry event which reports the amount of RAM/disk used by producer queues.

* 4.0.2
  - Fix dynamic topic producer initialization failure handling (introduced in 3.0.0).
  - Fix `unexpected_id` crash when replayq overflow (introduced in 4.0.1).

* 4.0.1
  - Minimize callback context for sync call.
  - Upgrade to kafka_protocol-4.1.9 for OTP 27.

* 4.0.0
  - Delete global stats (deprecated since 1.9).
  - Move linger delay to front of the buffer queue.
    The default value for `max_linger_ms` is `0` as before.
    Setting `max_linger_ms=10` will make the disk write batch larger when buffer is configured to disk mode or disk-offload mode.
  - Lower RAM usage with compact `pending_acks` data structure when the callbacks are static.

* 3.0.4
  - Upgrade to kafka_protocol-4.1.8

* 3.0.3
  - Use alias for OTP_RELEASE >= 24 to avoid contaminating send_sync caller's mailbox get contaminated by stale replies.

* 3.0.2
  - Fixed an issue where metadata was not correctly updated after terminating a producer.

* 3.0.1
  - Support dynamic topics for supervised producers.
    Call `wolff:ensure_supervised_dynamic_producers(ClientId, #{group => GroupName, ...})` to
    start a group-producer with no topics added in advance.
    And call `wolff:send2` or `wolff:send_sync2` to publish messages with topic provided as an argument.

* 3.0.0
  - Deleted `round_robin` partition strategy.
  - Change `alias` to `group`.
    Add `#{group => <<"group1">>}` to producer config for namespacing the topic,
    so multiple producers for one topic will not clash each other when sharing the same client.

* 2.0.0
  - Added the `alias` producer config option to make producers to the same topic be independent.

* 1.10.4 (merge 1.5.14)
  - Split batch if `message_too_large` error code is received.
    Prior to this fix, `wolff_producer` would retry the same batch indefinitely for any error code received from Kafka (`message_too_large` included).
    After this fix, if `message_too_large` happens, `wolff_producer` splits the batch into single-call batches and retry.
    It then ajdust the `max_batch_bytes` config to half of its original value for future batches.

* 1.10.3
  - Fixed typespec for `wolff_client:get_leader_connections/3`.

* 1.10.2 (merge 1.5.13)
  - Use long-lived metadata connection.
    This is to avoid having to excessively re-establish connection when there are many concurrent connectivity checks.
  - Fix connection error reason translation, the error log is now more compact when e.g. connect timeout happens.

* 1.10.1
  - Add `max_partitions` producer config to limit the number of partition producers so the client side is also possible to have control over resource utilization.

* 1.9.1
  - Use ETS (named `wolff_clients_global`) for client ID registration.
    When there are thousands of clients, `supervisor:which_children` becomes quite expensive.

* 1.9.0
  - No global stats collection by default.
    There is a ets table based stats collector to record the number of sent bytes and messages. Consider this feature deprecated.
    Since 1.7.0, there there is a better integration for metrics.
  - For supervised producers, use a global ets table (named `wolff_producers_global`) to store producer workers.
    This should avoid having to create an atom for each supervised topic producer.
  - Respect `request_timeout` in connection config when fetching metadata.

* 1.8.0
  - Add wolff:check_if_topic_exists/2 for checking if a topic exists making use of an existing client process. [#52](https://github.com/kafka4beam/wolff/pull/52)
  - Improved logs when reporting connection errors. (merged 1.5.12)
* 1.7.7 (merged 1.5.11)
  - Fixed a try catch pattern in `gen_server` call towards client process, this should prevent `wolff_producers` from crash if `wolff_client` is killed during initialization. [#49](https://github.com/kafka4beam/wolff/pull/49)
  - Enhance: use `off_heap` spawn option in producer processes for better gc performance. [#47](https://github.com/kafka4beam/wolff/pull/47)
* 1.7.6
  - Expose wolff:check_if_topic_exists/3 for checking if a topic is created. [#45](https://github.com/kafka4beam/wolff/pull/45)
* 1.7.5
  - Fixed eport number of caller issued requests but not Kafka requests in 'inflight' gauge.
* 1.7.4
  - Refactor the gauge metrics to handle multiple workers changing the
    same gauge. [#41](https://github.com/kafka4beam/wolff/pull/41)
  - Fix potential bug where the internal state of the producer process
    could have been swapped by the atom
    ok. [#41](https://github.com/kafka4beam/wolff/pull/41)
* 1.7.3
  - Upgrade `kafka_protocol` from version 4.1.1 to 4.1.2 to allow handling multiply nested wrapped secrets.
* 1.7.2
  - Upgrade `kafka_protocol` from version 4.1.0 to 4.1.1 to enable customizing the SNI without needing to set the `verify_peer` option.
* 1.7.1 (merged 1.5.9)
  - Fix: when picking a producer PID, if it was dead, it could lead to an error being raised. [#37](https://github.com/kafka4beam/wolff/pull/37)
* 1.7.0
  - Upgrade `kafka_protocol` from version 4.0.3 to version to 4.1.0 for SASL/GSSAPI auth support.
  - Also added beam-telemetry for better metrics report.
* 1.6.4 (merged 1.5.8)
* 1.6.3 (merged 1.5.7)
  - Stop supervised producer if failed to start. Otherwise the caller may have to call the wolff:stop_and_delete_supervised_producers/3
    after matching an error return. If they don't, then it may appear as a resource leak. [#26](https://github.com/kafka4beam/wolff/pull/26)
  - Ensure `{{Topic, Partition}, Connection}` record exists even if there are errors returned at partition level metadata.
    Fixed in PR [#29](https://github.com/kafka4beam/wolff/pull/29).
    There were two issues before this fix:
    * `wolff_client` may crash when trying to find partition leader connection for a producer worker.
      When there is error code in partition metadata, the connection record is not added,
      causing a `badmatch` error in this expression `{_, MaybePid} = lists:keyfind(Partition, 1, Partitions)`.
    * `wolff_producers` may crash when fewer partitions found in partition counter refresh.
      Although Kafka does not support topic down-scale, the assertion has been removed.
* 1.6.2
  - New producer option 'drop\_if\_highmem' to limit the growth of replayq(in mem) size
  - Drop otp22 support
  - Bring in local control application
* 1.6.1
  - Enhance: expose wolff:check_connectivity/2 for connectivity check before starting a client. (PR #18)
  - Fix: fix badarg pid monitoring error log when trying to close a dead connection. (PR #20)
  - Fix: better error logs (PR #16, PR #17)
    * No need to report stacktrace for timeout and connection refused errors.
    * Report host:port in connection failure reasons.
    * Fixed a bad logging arg which causes failure attempts to be logged as strings
* 1.6.0
  - Enhancement: upgrade dependencies, kafka_protocol-4.0.2 and replayq-0.3.3
  - Refactor: replaced error_logger with logger
* 1.5.4
  - Fix: no delay before the first re-connect attempt after disconnected while idling.
* 1.5.3
  - Enhancement: refine logging, reduce the number of retry logs, report error level log every 10 failures.
* 1.5.2
  - Enhancement: On-the-fly upgrade of anonymous function in client `gen_server` state to avoid badfun during hot-beam upgrade
  - Started using github action for CI
* 1.5.1
  - Fix: connection DOWN reason. Should not be a pid, otherwise a producer may not attempt to reconnect.
