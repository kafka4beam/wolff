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

-export([ensure_present/3, ensure_absence/1]).
-export([get_producers_pid/1]).

-define(SUPERVISOR, ?MODULE).

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
-spec ensure_present(wolff:client_id(), wolff_producers:id(), wolff_producers:config()) ->
  {ok, pid()} | {error, term()}.
ensure_present(ClientId, ProducerId, Config) ->
  ChildSpec = child_spec(ClientId, ProducerId, Config),
  case supervisor:start_child(?SUPERVISOR, ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, already_present} -> {error, not_running}
  end.

%% ensure client stopped and deleted under supervisor
-spec ensure_absence(wolff_producers:id()) -> ok.
ensure_absence(ProducerId) ->
  case supervisor:terminate_child(?SUPERVISOR, ProducerId) of
    ok ->
      ok = wolff_producers:cleanup_workers_table(ProducerId),
      ok = supervisor:delete_child(?SUPERVISOR, ProducerId);
    {error, not_found} ->
      ok
  end.

child_spec(ClientId, ProducerId, Config) ->
  #{id => ProducerId,
    start => {wolff_producers, start_link, [ClientId, ProducerId, Config]},
    restart => transient,
    type => worker
   }.

%% Find the running process (gen_server of wolff_producers) from thie child ID.
get_producers_pid(ID) ->
  Children = supervisor:which_children(?SUPERVISOR),
  case lists:keyfind(ID, 1, Children) of
    {ID, Pid, _Type, _Modules} when is_pid(Pid) ->
      Pid;
    Other ->
      throw(#{cause => producers_not_found_uder_supervisor,
              child_id => ID,
              lookup_result => Other
             })
  end.
