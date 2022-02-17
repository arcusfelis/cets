-module(kiss_SUITE).
-include_lib("common_test/include/ct.hrl").
 
-compile([export_all]).
 
all() -> [ets_tests].
 
init_per_suite(Config) ->
    Node2 = start_node(ct2),
    Node3 = start_node(ct3),
    Node4 = start_node(ct4),
    [{nodes, [Node2, Node3, Node4]}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(ets_tests, Config) ->
    Config.
 
end_per_testcase(ets_tests, Config) ->
    ok.
 
ets_tests(Config) ->
    Node1 = node(),
    [Node2, Node3, Node4] = proplists:get_value(nodes, Config),
    Tab = tab1,
    start(Node1, Tab),
    start(Node2, Tab),
    start(Node3, Tab),
    start(Node4, Tab),
    join(Node1, Node3, Tab),
    join(Node2, Node4, Tab),
    insert(Node1, Tab, {a}),
    insert(Node2, Tab, {b}),
    insert(Node3, Tab, {c}),
    insert(Node4, Tab, {d}),
    ct:pal("n1 ~p~n", [dump(Node1, Tab)]),
    ct:pal("n2 ~p~n", [dump(Node2, Tab)]),
    join(Node1, Node2, Tab),
    ct:pal("n1 ~p~n", [dump(Node1, Tab)]),
    ct:pal("n2 ~p~n", [dump(Node2, Tab)]),
    insert(Node1, Tab, {f}),
    insert(Node4, Tab, {e}),
%   [insert(Node1, Tab, {X}) || X <- lists:seq(1, 100000)],
    ct:pal("n1 ~p~n", [dump(Node1, Tab)]),
    ct:pal("n2 ~p~n", [dump(Node2, Tab)]),
    error(oops),
    ok.

start(Node, Tab) ->
    rpc(Node, kiss, start, [Tab, #{}]).

insert(Node, Tab, Rec) ->
    rpc(Node, kiss, insert, [Tab, Rec]).

dump(Node, Tab) ->
    rpc(Node, kiss, dump, [Tab]).

join(Node1, Node2, Tab) ->
    rpc(Node1, kiss, join, [Node2, Tab]).

rpc(Node, M, F, Args) ->
    case rpc:call(Node, M, F, Args) of
        {badrpc, Error} ->
            ct:fail({badrpc, Error});
        Other ->
            Other
    end.

start_node(Sname) ->
    {ok, Node} = ct_slave:start(Sname, [{monitor_master, true}]),
    rpc:call(Node, code, add_paths, [code:get_path()]),
    Node.
