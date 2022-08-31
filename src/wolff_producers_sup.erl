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

-module(wolff_producers_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([ensure_present/3, ensure_absence/2]).

-define(SUPERVISOR, ?MODULE).
-define(WORKER_ID(ClientId, Topic), {ClientId, Topic}).

start_link() ->
  supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

init([]) ->
  SupFlags = #{strategy => one_for_one,
               intensity => 10,
               period => 5
              },
  Children = [], %% dynamically added/stopped
  {ok, {SupFlags, Children}}.

%% ensure a client started under supervisor
-spec ensure_present(wolff:client_id(), kpro:topic(), wolff_producer:config()) ->
  {ok, pid()} | {error, term()}.
ensure_present(ClientId, Topic, Config) ->
  ChildSpec = child_spec(ClientId, Topic, Config),
  case supervisor:start_child(?SUPERVISOR, ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, already_present} -> {error, not_running}
  end.

%% ensure client stopped and deleted under supervisor
-spec ensure_absence(wolff:client_id(), wolff:name()) -> ok.
ensure_absence(ClientId, Name) ->
  Id = ?WORKER_ID(ClientId, Name),
  case supervisor:terminate_child(?SUPERVISOR, Id) of
    ok -> ok = supervisor:delete_child(?SUPERVISOR, Id);
    {error, not_found} -> ok
  end.

child_spec(ClientId, Topic, Config) ->
  #{id => ?WORKER_ID(ClientId, get_name(Config)),
    start => {wolff_producers, start_link, [ClientId, Topic, Config]},
    restart => transient,
    type => worker,
    modules => [wolff_producers]
   }.

get_name(Config) -> maps:get(name, Config, wolff_producers).
