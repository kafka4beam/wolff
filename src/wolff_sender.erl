%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This module encodes input batch into Kafka produce request and send
%% the encoded batch to kpro_connection process.
%% It offloads the heavy lifting work:
%% - Batch encoding
%% - CRC32C
%% - Compression
%% From wolff_producer process.
-module(wolff_sender).

-behaviour(gen_server).

-export([start_link/4, do/8]).

-include_lib("kafka_protocol/include/kpro.hrl").

%% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

-spec do(pid(), pid(), {alias, reference()}, kpro:vsn(),
         kpro:topic(), kpro:partition(), [kpro:msg_input()], kpro:produce_opts()) -> ok.
do(SenderPid, ConnPid, Ref, Vsn, Topic, Partition, Batch, Opts) ->
    gen_server:cast(SenderPid, {send, ConnPid, Ref, Vsn, Topic, Partition, Batch, Opts}).

%% @doc Start the wolff_sender gen_server.
-spec start_link(pid(), wolff:client_id(), kpro:topic(), kpro:partition()) -> {ok, pid()} | {error, any()}.
start_link(Owner, ClientId, Topic, Partition) ->
    gen_server:start_link(?MODULE, {Owner, ClientId, Topic, Partition}, []).

%% gen_server callbacks

init({Owner, ClientId, Topic, Partition}) ->
    ok = set_process_label(ClientId, Topic, Partition),
    {ok, #{owner => Owner, client_id => ClientId, topic => Topic, partition => Partition}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({send, ConnPid, Ref, Vsn, Topic, Partition, Batch, Opts}, State) ->
    Req = kpro_req_lib:produce(Vsn, Topic, Partition, Batch, Opts, Ref),
    ok = kpro:send(ConnPid, Req),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-if(?OTP_RELEASE >= 27).
set_process_label(ClientId, Topic, Partition) ->
    proc_lib:set_label({?MODULE, ClientId, Topic, Partition}).
-else.
set_process_label(_ClientId, _Topic, _Partition) ->
    ok.
-endif.
