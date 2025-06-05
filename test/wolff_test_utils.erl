-module(wolff_test_utils).

-compile([nowarn_export_all, export_all]).

dedup_list(Xs) ->
    dedup_list(Xs, []).

dedup_list([], Acc) ->
    lists:reverse(Acc);
dedup_list([X | Xs], Acc) ->
    case Acc of
        [X | _] ->
            dedup_list(Xs, Acc);
        _ ->
            dedup_list(Xs, [X | Acc])
    end.

maybe_zookeeper() ->
  {Major, _} = kafka_version(),
  case Major >= 3 of
    true ->
      %% Kafka 2.2 started supporting --bootstap-server, but 2.x still supports --zookeeper
      %% Starting from 3.0, --zookeeper is no longer supported, must use --bootstrap-server
      "--bootstrap-server localhost:9092";
    false ->
      "--zookeeper " ++ env("ZOOKEEPER_IP") ++ ":2181"
  end.

kafka_version() ->
  VsnStr = env("KAFKA_VERSION"),
  [Major, Minor | _] = string:tokens(VsnStr, "."),
  {list_to_integer(Major), list_to_integer(Minor)}.

env(Var) ->
  case os:getenv(Var) of
    [_|_] = Val-> Val;
    _ -> error({env_var_missing, Var})
  end.

topics_cmd_base(Topic) when is_binary(Topic) ->
  topics_cmd_base(binary_to_list(Topic));
topics_cmd_base(Topic) when is_list(Topic) ->
  "docker exec kafka-1 kafka-topics.sh " ++
  maybe_zookeeper() ++
  " --topic '" ++ Topic ++ "'".

delete_topic(Topic) when is_binary(Topic) ->
  delete_topic(binary_to_list(Topic));
delete_topic(Topic) ->
  Cmd = topics_cmd_base(Topic) ++ " --delete",
  Result = os:cmd(Cmd),
  Checks =
    [fun() -> Result =:= [] end,
     fun() -> 1 =:= string:str(Result, "Topic " ++ Topic ++ " is marked for deletion.") end,
     fun() -> string:str(Result, "does not exist") > 0 end
    ],
  case lists:any(fun(F) -> F() end, Checks) of
    true -> ok;
    false -> throw(Result)
  end.
