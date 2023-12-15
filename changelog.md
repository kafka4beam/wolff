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
