-module(kiss_SUITE).
-include_lib("common_test/include/ct.hrl").
 
-compile([export_all]).
 
all() -> [test_multinode, test_multinode_auto_discovery, test_locally,
          handle_down_is_called].
 
init_per_suite(Config) ->
    Node2 = start_node(ct2),
    Node3 = start_node(ct3),
    Node4 = start_node(ct4),
    [{nodes, [Node2, Node3, Node4]}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(test_multinode_auto_discovery, Config) ->
    ct:make_priv_dir(),
    Config;
init_per_testcase(_, Config) ->
    Config.
 
end_per_testcase(_, _Config) ->
    ok.
 
test_multinode(Config) ->
    Node1 = node(),
    [Node2, Node3, Node4] = proplists:get_value(nodes, Config),
    Tab = tab1,
    {ok, _Pid1} = start(Node1, Tab),
    {ok, Pid2} = start(Node2, Tab),
    {ok, Pid3} = start(Node3, Tab),
    {ok, Pid4} = start(Node4, Tab),
    join(Node1, Pid3, Tab),
    join(Node2, Pid4, Tab),
    insert(Node1, Tab, {a}),
    insert(Node2, Tab, {b}),
    insert(Node3, Tab, {c}),
    insert(Node4, Tab, {d}),
    [{a},{c}] = dump(Node1, Tab),
    [{b},{d}] = dump(Node2, Tab),
    join(Node1, Pid2, Tab),
    [{a},{b},{c},{d}] = dump(Node1, Tab),
    [{a},{b},{c},{d}] = dump(Node2, Tab),
    insert(Node1, Tab, {f}),
    insert(Node4, Tab, {e}),
    Same = fun(X) ->
               X = dump(Node1, Tab),
               X = dump(Node2, Tab),
               X = dump(Node3, Tab),
               X = dump(Node4, Tab)
           end,
    Same([{a},{b},{c},{d},{e},{f}]),
    [Node2, Node3, Node4] = other_nodes(Node1, Tab),
    [Node1, Node3, Node4] = other_nodes(Node2, Tab),
    [Node1, Node2, Node4] = other_nodes(Node3, Tab),
    [Node1, Node2, Node3] = other_nodes(Node4, Tab),
    delete(Node1, Tab, e),
    Same([{a},{b},{c},{d},{f}]),
    delete(Node4, Tab, a),
    Same([{b},{c},{d},{f}]),
    %% Bulk operations are supported
    insert(Node4, Tab, [{m},{a},{n},{y}]),
    Same([{a},{b},{c},{d},{f},{m},{n},{y}]),
    delete_many(Node4, Tab, [a,n]),
    Same([{b},{c},{d},{f},{m},{y}]),
    ok.

test_multinode_auto_discovery(Config) ->
    Node1 = node(),
    [Node2, Node3, Node4] = proplists:get_value(nodes, Config),
    Tab = tab2,
    {ok, _Pid1} = start(Node1, Tab),
    {ok, Pid2} = start(Node2, Tab),
    Dir = proplists:get_value(priv_dir, Config),
    ct:pal("Dir ~p", [Dir]),
    FileName = filename:join(Dir, "disco.txt"),
    ok = file:write_file(FileName, io_lib:format("~s~n~s~n", [Node1, Node2])),
    {ok, Disco} = kiss_discovery:start(#{tables => [Tab], disco_file => FileName}),
    %% Waits for the first check
    ok = gen_server:call(Disco, ping),
    [Node2] = other_nodes(Node1, Tab),
    ok.

test_locally(_Config) ->
    {ok, _Pid1} = kiss:start(t1, #{}),
    {ok, Pid2} = kiss:start(t2, #{}),
    kiss:join(lock1, t1, Pid2),
    kiss:insert(t1, {1}),
    kiss:insert(t1, {1}),
    kiss:insert(t2, {2}),
    D = kiss:dump(t1),
    D = kiss:dump(t2).

handle_down_is_called(_Config) ->
    Parent = self(),
    DownFn = fun(#{remote_pid := _RemotePid, table := _Tab}) ->
                     Parent ! down_called
             end,
    {ok, Pid1} = kiss:start(d1, #{handle_down => DownFn}),
    {ok, Pid2} = kiss:start(d2, #{}),
    kiss:join(lock1, d1, Pid2),
    exit(Pid2, oops),
    receive
        down_called -> ok
    after 5000 -> ct:fail(timeout)
    end.

start(Node, Tab) ->
    rpc(Node, kiss, start, [Tab, #{}]).

insert(Node, Tab, Rec) ->
    rpc(Node, kiss, insert, [Tab, Rec]).

delete(Node, Tab, Key) ->
    rpc(Node, kiss, delete, [Tab, Key]).

delete_many(Node, Tab, Keys) ->
    rpc(Node, kiss, delete_many, [Tab, Keys]).

dump(Node, Tab) ->
    rpc(Node, kiss, dump, [Tab]).

other_nodes(Node, Tab) ->
    rpc(Node, kiss, other_nodes, [Tab]).

join(Node1, Node2, Tab) ->
    rpc(Node1, kiss, join, [lock1, Tab, Node2]).

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