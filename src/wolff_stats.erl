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

-module(wolff_stats).

-behaviour(gen_server).

%% APIs
-export([start_link/0, recv/4, sent/4, getstat/0, getstat/3]).

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

-define(SERVER, ?MODULE).
-define(ETS, ?MODULE).
-define(send_cnt(C, T, P), {send_cnt, C, T, P}).
-define(send_oct(C, T, P), {send_oct, C, T, P}).
-define(recv_cnt(C, T, P), {recv_cnt, C, T, P}).
-define(recv_oct(C, T, P), {recv_oct, C, T, P}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Bump numbers as input to wolff.
recv(ClientId, Topic, Partition, #{cnt := Cnt, oct := Oct} = Numbers) ->
  ok = bump_counter(?recv_cnt(ClientId, Topic, Partition), Cnt),
  ok = bump_counter(?recv_oct(ClientId, Topic, Partition), Oct),
  gen_server:cast(?SERVER, {recv, Numbers}).

%% @doc Bump numbers as output of wolff.
sent(ClientId, Topic, Partition, #{cnt := Cnt, oct := Oct} = Numbers) ->
  ok = bump_counter(?send_cnt(ClientId, Topic, Partition), Cnt),
  ok = bump_counter(?send_oct(ClientId, Topic, Partition), Oct),
  gen_server:cast(?SERVER, {sent, Numbers}).

getstat() ->
  gen_server:call(?SERVER, getstat, infinity).

getstat(ClientId, Topic, Partition) ->
  #{send_cnt => get_counter(?send_cnt(ClientId, Topic, Partition)),
    send_oct => get_counter(?send_oct(ClientId, Topic, Partition)),
    recv_cnt => get_counter(?recv_cnt(ClientId, Topic, Partition)),
    recv_oct => get_counter(?recv_oct(ClientId, Topic, Partition))
   }.

init([]) ->
  {ok, #{ets => ets:new(?ETS, [named_table, public, {write_concurrency, true}]),
         send_cnt => 0,
         send_oct => 0,
         recv_cnt => 0,
         recv_oct => 0
        }}.

handle_call(getstat, _From, St) ->
  Result = maps:with([send_cnt, send_oct, recv_cnt, recv_oct], St),
  {reply, Result, St};
handle_call(_Call, _From, St) ->
  {noreply, St}.

handle_cast({recv, Numbers}, #{recv_oct := TotalOct, recv_cnt := TotalCnt} = St) ->
  #{cnt := Cnt, oct := Oct} = Numbers,
  {noreply, St#{recv_oct := TotalOct + Oct, recv_cnt := TotalCnt + Cnt}};
handle_cast({sent, Numbers}, #{send_oct := TotalOct, send_cnt := TotalCnt} = St) ->
  #{cnt := Cnt, oct := Oct} = Numbers,
  {noreply, St#{send_oct := TotalOct + Oct, send_cnt := TotalCnt + Cnt}};
handle_cast(_Cast, St) ->
  {noreply, St}.

handle_info(_Info, St) ->
  {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
  {ok, St}.

terminate(_Reason, _St) ->
  ok.

bump_counter(Key, Inc) ->
  try _ = ets:update_counter(?ETS, Key, Inc, {Key, 0}), ok
  catch _ : _ -> ok
  end.

get_counter(Key) ->
  case ets:lookup(?ETS, Key) of
    [] -> 0;
    [{_, Value}] -> Value
  end.

