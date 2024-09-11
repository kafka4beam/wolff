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

-export([new/0, count/1, insert/2, take/2]).

-export_type([acks/0]).

-type call_id() :: pos_integer().
-type key() :: call_id() | {call_id(), call_id()}.
-type cb() :: term().
-opaque acks() :: #{next_id := integer(),
                    cbs := queue:queue({key(), cb()})}.

%% @doc Initialize a new data structure.
new() ->
    %% use a timestamp for call ID base so the items recovered from disk
    %% will not be possible to clash with newer generation call after
    %% the process crashed or node restarted.
    Now = erlang:system_time(microsecond),
    #{next_id => Now,
      cbs => queue:new()
     }.

%% @doc count the total number of pending acks.
-spec count(acks()) -> non_neg_integer().
count(#{cbs := Cbs}) ->
    sum(queue:to_list(Cbs), 0).

sum([], Acc) ->
    Acc;
sum([{CallId, _} | Rest], Acc) when is_integer(CallId) ->
    sum(Rest, Acc + 1);
sum([{{MinCallId, MaxCallId}, _} | Rest], Acc) ->
    sum(Rest, Acc + MaxCallId - MinCallId + 1).

%% @doc insert a callback into the data structure.
-spec insert(acks(), cb()) -> {call_id(), acks()}.
insert(#{next_id := Id, cbs := Cbs} = X, Cb) ->
    NewCbs = insert_cb(Cbs, Id, Cb),
    {Id, X#{next_id => Id + 1, cbs => NewCbs}}.

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
expand_id(Id0, Id) when is_integer(Id0) ->
    Id =:= Id0 + 1 orelse error({unexpected_id, Id0, Id}),
    expand_id({Id0, Id0}, Id);
expand_id({MinId, MaxId}, Id) ->
    Id =:= MaxId + 1 orelse error({unexpected_id, {MinId, MaxId}, Id}),
    {MinId, Id}.

%% @doc Take the callback for a given call ID.
%% The ID is expected to be the oldest in the queue.
%% Return the callback and the updated data structure.
-spec take(acks(), call_id()) -> {ok, cb(), acks()} | false.
take(#{cbs := Cbs} = X, Id) ->
    case take1(Cbs, Id) of
        false ->
            %% stale ack
            false;
        {ok, Cb, Cbs1} ->
            {ok, Cb, X#{cbs => Cbs1}}
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
