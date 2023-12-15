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

-module(wolff_client_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([ensure_present/3, ensure_absence/1, find_client/1]).
-export([register_client/1, deregister_client/1]).

-include("wolff.hrl").

-define(SUPERVISOR, ?MODULE).

start_link() -> supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

init([]) ->
  SupFlags = #{strategy => one_for_one,
               intensity => 10,
               period => 5
              },
  ok = create_clients_table(),
  Children = [], %% dynamically added/stopped
  {ok, {SupFlags, Children}}.

%% @doc Ensure a client started under supervisor.
-spec ensure_present(wolff:client_id(), [wolff:host()], wolff_client:config()) ->
  {ok, pid()} | {error, client_not_running}.
ensure_present(ClientId, Hosts, Config) ->
  ChildSpec = child_spec(ClientId, Hosts, Config),
  case supervisor:start_child(?SUPERVISOR, ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, already_present} -> {error, client_not_running}
  end.

%% @doc Ensure client stopped and deleted under supervisor.
-spec ensure_absence(wolff:client_id()) -> ok.
ensure_absence(ClientId) ->
  case supervisor:terminate_child(?SUPERVISOR, ClientId) of
    ok -> ok = supervisor:delete_child(?SUPERVISOR, ClientId);
    {error, not_found} -> ok
  end,
  %% wolff_client process' terminate callback deregisters itself
  %% but we make sure it's deregistered in case the client is killed
  ok = deregister_client(ClientId).

%% @doc Find client pid from client id.
-spec find_client(wolff:client_id()) -> {ok, pid()} | {error, any()}.
find_client(ClientID) ->
    try
        case ets:lookup(?WOLFF_CLIENTS_GLOBAL_TABLE, ClientID) of
            [{ClientID, Pid}] ->
                {ok, Pid};
            [] ->
                {error, no_such_client}
        end
    catch
        error : badarg ->
            {error, client_supervisor_not_initialized}
    end.


%% @private Make supervisor child spec.
child_spec(ClientId, Hosts, Config) ->
  #{id => ClientId,
    start => {wolff_client, start_link, [ClientId, Hosts, Config]},
    restart => transient,
    type => worker,
    modules => [wolff_client]
   }.

%% @doc Create a ets table which is used for client registration.
%% Records are of format: {ClientId, Pid}
create_clients_table() ->
  EtsName = ?WOLFF_CLIENTS_GLOBAL_TABLE,
  EtsOpts = [named_table, public, ordered_set, {read_concurrency, true}],
  EtsName = ets:new(EtsName, EtsOpts),
  ok.

%% @doc Insert the client in the registration table.
register_client(ClientId) ->
    Pid = self(),
    true = ets:insert(?WOLFF_CLIENTS_GLOBAL_TABLE, {ClientId, Pid}),
    ok.

%% @doc Delete the client from the registration table.
deregister_client(ClientId) ->
    _ = ets:delete(?WOLFF_CLIENTS_GLOBAL_TABLE, ClientId),
    ok.
