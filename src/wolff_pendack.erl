%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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


%% @doc Implement a data structure to hold pending acks towards `send' or `cast'
%% callers.
%% The pending acks are stored in a queue,
%% each item in the queue is a pair of call IDs and callback.
%% The call ID is a monotonically increasing integer, starting from current
%% time in microseconds.
-module(wolff_pendack).

-export([new/0, count/1, insert_backlog/2]).
-export([take_inflight/2, drop_backlog/2, move_backlog_to_inflight/2]).
-export([fold/3]).

-export_type([acks/0]).

-type call_id() :: pos_integer().
-type key() :: call_id() | {call_id(), call_id()}.
-type cb() :: term().
-opaque acks() :: #{next_id := integer(),
                    backlog := queue:queue({key(), cb()}),
                    inflight := queue:queue({key(), cb()}),
                    count := non_neg_integer()
                   }.

%% @doc Initialize a new data structure.
new() ->
    %% use a timestamp for call ID base so the items recovered from disk
    %% will not be possible to clash with newer generation call after
    %% the process crashed or node restarted.
    Now = erlang:system_time(microsecond),
    #{next_id => Now,
      backlog => queue:new(),
      inflight => queue:new(),
      count => 0
     }.

%% @doc count the total number of pending acks.
-spec count(acks()) -> non_neg_integer().
count(#{count := Count}) ->
    Count.

%% @doc insert a callback into the backlog queue.
-spec insert_backlog(acks(), cb()) -> {call_id(), acks()}.
insert_backlog(#{next_id := Id, backlog := Cbs, count := Count} = X, Cb) ->
    NewCbs = insert_cb(Cbs, Id, Cb),
    {Id, X#{next_id => Id + 1, backlog => NewCbs, count => Count + 1}}.

%% For inflights, there can be a gap if some calls have been dropped
%% from  the backlog.
%% So we catch the non_sequential_call_id error and enqueue as a
%% new ID range.
insert_inflight(Cbs, Id, Cb) ->
    try
        insert_cb(Cbs, Id, Cb)
    catch
        error:{non_sequential_call_id, _, _} ->
            queue:in({Id, Cb}, Cbs)
    end.

insert_cb(Cbs, Id, Cb) ->
    case queue:out_r(Cbs) of
        {empty, _} ->
            queue:in({Id, Cb}, Cbs);
        {{value, {Key1, Cb1}}, Cbs1} ->
            insert_cb1(Cbs1, Key1, Cb1, Id, Cb)
    end.

%% If the callback is identical to the previous one, then just update the
%% call ID range.
%% Otherwise, insert the new callback.
insert_cb1(Cbs, Key, Cb, Id, Cb1) when Cb =:= Cb1 ->
    Key1 = expand_id(Key, Id),
    queue:in({Key1, Cb1}, Cbs);
insert_cb1(Cbs, Key, Cb, Id, Cb1) ->
    queue:in({Id, Cb1}, queue:in({Key, Cb}, Cbs)).

%% If the ID is a single integer, then expand it to a range.
%% Assert that new ID is the very next (+1) ID.
%% The assertion should not fail for backlog queue because the next_id
%% keeps track of it (otherwise a bug).
%% The assertion may fail for inflight queue if backlog was dropped
%% (e.g. due to OOM protection) so it is expected to be caught.
expand_id(Id0, Id) when is_integer(Id0) ->
    Id =:= Id0 + 1 orelse error({non_sequential_call_id, Id0, Id}),
    expand_id({Id0, Id0}, Id);
expand_id({MinId, MaxId}, Id) ->
    Id =:= MaxId + 1 orelse error({non_sequential_call_id, {MinId, MaxId}, Id}),
    {MinId, Id}.

%% @doc Take the callback from the inflight queue.
%% The ID is expected to be the oldest in the inflight queue.
%% Return the callback and the updated data structure.
-spec take_inflight(acks(), call_id()) -> {ok, cb(), acks()} | false.
take_inflight(#{inflight := Cbs, count := Count} = X, Id) ->
    case take1(Cbs, Id) of
        false ->
            %% stale ack
            false;
        {ok, Cb, Cbs1} ->
            {ok, Cb, X#{inflight => Cbs1, count => Count - 1}}
    end.

take1(Cbs0, Id) ->
    case queue:out(Cbs0) of
        {empty, _} ->
            false;
        {{value, {Key, Cb}}, Cbs} ->
            take2(Cbs, Key, Cb, Id)
    end.

take2(Cbs, Id0, Cb, Id) when is_integer(Id0) ->
    take2(Cbs, {Id0, Id0}, Cb, Id);
take2(_Cbs, {MinId, _MaxId}, _Cb, Id) when Id < MinId ->
    %% stale ack
    false;
take2(Cbs, {MinId, MaxId}, Cb, Id) when Id =:= MinId ->
    %% ack the oldest item
    case MaxId =:= MinId of
        true ->
            {ok, Cb, Cbs};
        false ->
            {ok, Cb, queue:in_r({{Id + 1, MaxId}, Cb}, Cbs)}
    end;
take2(_Cbs, {MinId, MaxId}, _Cb, Id) ->
    error(#{cause => unexpected_id, min => MinId, max => MaxId, got => Id}).

%% @doc Drop calls from the head (older end) of the backlog queue.
%% Return list of callbacks and the remaining.
-spec drop_backlog(acks(), [call_id()]) -> {[cb()], acks()}.
drop_backlog(X, Ids) ->
    drop1(X, Ids, []).

drop1(X, [], Acc) ->
    {lists:reverse(Acc), X};
drop1(#{backlog := Cbs0, count := Count} = X, [Id | Ids], Acc) ->
    case take1(Cbs0, Id) of
        false ->
            drop1(X, Ids, Acc);
        {ok, Cb, Cbs}  ->
            drop1(X#{backlog := Cbs, count := Count - 1}, Ids, [Cb | Acc])
    end.

%% @doc Move a list of calls from the head of the backlog queue
%% and push it to the inflight queue.
-spec move_backlog_to_inflight(acks(), [call_id()]) -> acks().
move_backlog_to_inflight(X, []) ->
    X;
move_backlog_to_inflight(X, [Id | Ids]) ->
    move_backlog_to_inflight(move1(X, Id), Ids).

move1(#{backlog := Backlog0, inflight := Inflight0} = X, Id) ->
    case take1(Backlog0, Id) of
        false ->
            X;
        {ok, Cb, Backlog} ->
            Inflight = insert_inflight(Inflight0, Id, Cb),
            X#{backlog := Backlog, inflight := Inflight}
    end.

%% @doc Fold the pending acks from the head (older end, smallest ID) to the tail (newer end, largest ID).
-spec fold(acks(), fun((cb(), term()) -> term()), term()) -> term().
fold(#{backlog := Backlog, inflight := Inflight}, Fun, Acc) ->
    Acc1 = fold2(Inflight, Fun, Acc),
    fold2(Backlog, Fun, Acc1).

fold2(Q, Fun, Acc) ->
    case queue:out(Q) of
        {empty, _} ->
            Acc;
        {{value, {Key, Cb}}, Q1} ->
            Acc1 = fold3(Key, Cb, Fun, Acc),
            fold2(Q1, Fun, Acc1)
    end.

fold3(Id, Cb, Fun, Acc) when is_integer(Id) ->
    Fun(Cb, Acc);
fold3({MinId, MaxId}, _Cb, _Fun, Acc) when MinId > MaxId ->
    Acc;
fold3({MinId, MaxId}, Cb, Fun, Acc) ->
    Acc1 = Fun(Cb, Acc),
    fold3({MinId + 1, MaxId}, Cb, Fun, Acc1).
