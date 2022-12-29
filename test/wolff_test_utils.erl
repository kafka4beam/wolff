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
