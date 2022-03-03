* 1.6.1
  - Enhance: expose wolff_client:check_connectivity/2 for connectivity check before starting a client. (PR #18)
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

