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

%% Table to register ClientID -> Pid mapping.
%% Applications may often need to find the client pid and run
%% some ad-hoc requests e.g. for health check purposes.
%% This talbe helps to avoid calling supervisor:which_children intensively.
-define(WOLFF_CLIENTS_GLOBAL_TABLE, wolff_clients_global).

%% Table to register {NS, Topic, Partition} -> Pid mapping.
%% This allows all producers to share this one ETS table for quick
%% partition-worker lookup.
%% A special record {{NS, Topic, partition_count}, Count}
%% is inserted to cache the partition count.
%% NS is either `{client, ClientId}` for regular producers
%% or `Group` if topic is assigned to a group.
-define(WOLFF_PRODUCERS_GLOBAL_TABLE, wolff_producers_global).

-define(NS_TOPIC(NS, TOPIC), {NS, TOPIC}).
-define(NO_GROUP, no_group).
-define(DYNAMIC, dynamic).

-define(all_partitions, all_partitions).

-endif.
