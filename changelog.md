* 1.5.7
  - Stop supervised producer if failed to start. Otherwise the caller may have to call the wolff:stop_and_delete_supervised_producers/3
    after matching an error return. If they don't, then it may appear as a resource leak.
* 1.5.6
  - New producer option 'drop\_if\_highmem' to limit the growth of replayq(in mem) size
  - Drop otp22 support
  - Bring in local control application
* 1.5.5
  - Fix: fix badarg pid monitoring error log when trying to close a dead connection. (PR #20)
  - Enhance: expose wolff_client:check_connectivity/2 for connectivity check before starting a client. (PR #18)
  - Fix: better error logs (PR #16, PR #17)
    * No need to report stacktrace for timeout and connection refused errors.
    * Report host:port in connection failure reasons.
    * Fixed a bad logging arg which causes failure attempts to be logged as strings
* 1.5.4
  - Fix: no delay before the first re-connect attempt after disconnected while idling.
* 1.5.3
  - Enhancement: refine logging, reduce the number of retry logs, report error level log every 10 failures.
* 1.5.2
  - Enhancement: On-the-fly upgrade of anonymous function in client `gen_server` state to avoid badfun during hot-beam upgrade
  - Started using github action for CI
* 1.5.1
  - Fix: connection DOWN reason. Should not be a pid, otherwise a producer may not attempt to reconnect.

