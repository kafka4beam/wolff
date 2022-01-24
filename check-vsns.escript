#!/usr/bin/env escript

-mode(compile).

main(_) ->
    {ok, [{application, wolff, App}]} = file:consult("src/wolff.app.src"),
    {ok, [{VsnAppup, _Up, _Down}]} = file:consult("src/wolff.appup.src"),
    {vsn, VsnApp} = lists:keyfind(vsn, 1, App),
    case VsnAppup =:= VsnApp of
        true -> ok;
        false -> error([{appup, VsnAppup}, {app, VsnApp}])
    end.
