-ifndef(WOLFF_HRL).
-define(WOLFF_HRL, true).

-define(conn_down(Reason), {down, Reason}).
-define(conn_error(Reason), {error, Reason}).
-define(leader_connection(Pid), {leader_connection, Pid}).
-define(UNKNOWN_OFFSET, -1).
-define(buffer_overflow_discarded, buffer_overflow_discarded).

%% Kafka has default batch size limit 1000012.
%% Since Kafka 2.4, it has been extended to 1048588.
%% We keep it backward compatible here.
-define(WOLFF_KAFKA_DEFAULT_MAX_MESSAGE_BYTES, 1000000).
-endif.
