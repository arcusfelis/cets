%% Very simple multinode ETS writer
%% One file, everything is simple, but we don't silently hide race conditions
%% No transactions
%% We don't use rpc module, because it is one gen_server


%% We don't use monitors to avoid round-trips (that's why we don't use calls neither)
-module(kiss).
-export([start/2, dump/1, insert/2, join/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-behaviour(gen_server).

%% Table and server has the same name
start(Tab, Opts) when is_atom(Tab) ->
    gen_server:start({local, Tab}, ?MODULE, [Tab], []).

stop(Tab) ->
    gen_server:stop(Tab).

dump(Tab) ->
    ets:tab2list(Tab).

join(RemoteNode, Tab) when RemoteNode =/= node() ->
    gen_server:call(Tab, {join, RemoteNode}, infinity).

remote_add_node_to_schema(RemotePid, ServerPid, OtherNodes) ->
    gen_server:call(RemotePid, {remote_add_node_to_schema, ServerPid, OtherNodes}, infinity).

send_dump_to_remote_node(RemotePid, FromPid, OurDump) ->
    gen_server:call(RemotePid, {send_dump_to_remote_node, FromPid, OurDump}, infinity).

%% Key is {USR, Sid, UpdateNumber}
%% Where first UpdateNumber is 0
insert(Tab, Rec) ->
    Nodes = other_nodes(Tab),
    ets:insert_new(Tab, Rec),
    %% Insert to other nodes and block till written
    Monitors = insert_to_remote_nodes(Nodes, Rec),
    wait_for_inserted(Monitors).

insert_to_remote_nodes([{RemoteNode, RemotePid, ProxyPid}|Nodes], Rec) ->
    Mon = erlang:monitor(process, ProxyPid),
    erlang:send(RemotePid, {insert_from_remote_node, Mon, self(), Rec}, [noconnect]),
    [Mon|insert_to_remote_nodes(Nodes, Rec)];
insert_to_remote_nodes([], _Rec) ->
    [].

wait_for_inserted([Mon|Monitors]) ->
    receive
        {inserted, Mon2} when Mon2 =:= Mon ->
            wait_for_inserted(Monitors);
        {'DOWN', Mon2, process, _Pid, _Reason} when Mon2 =:= Mon ->
            wait_for_inserted(Monitors)
    end;
wait_for_inserted([]) ->
    ok.

other_nodes(Tab) ->
%   gen_server:call(Tab, get_other_nodes).
    kiss_pt:get(Tab).

init([Tab]) ->
    ets:new(Tab, [ordered_set, named_table,
                  public, {read_concurrency, true}]),
    update_pt(Tab, []),
    {ok, #{tab => Tab, other_nodes => []}}.

handle_call({join, RemoteNode}, From, State) ->
    handle_join(RemoteNode, State);
handle_call({remote_add_node_to_schema, ServerPid, OtherNodes}, _From, State) ->
    handle_remote_add_node_to_schema(ServerPid, OtherNodes, State);
handle_call({send_dump_to_remote_node, FromPid, Dump}, _From, State) ->
    handle_send_dump_to_remote_node(FromPid, Dump, State);
handle_call(get_other_nodes, _From, State = #{other_nodes := Nodes}) ->
    {reply, Nodes, State};
handle_call({insert, Rec}, From, State = #{tab := Tab}) ->
    ets:insert_new(Tab, Rec),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Mon, Pid, _Reason}, State) ->
    handle_remote_down(Pid, State);
handle_info({insert_from_remote_node, Mon, Pid, Rec}, State = #{tab := Tab}) ->
    ets:insert_new(Tab, Rec),
    Pid ! {inserted, Mon},
    {noreply, State}.

terminate(_Reason, _State = #{tab := Tab}) ->
    kiss_pt:put(Tab, []),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_join(RemoteNode, State = #{tab := Tab, other_nodes := Nodes}) ->
    case lists:keymember(RemoteNode, 1, Nodes) of
        true ->
            %% Already added
            {reply, ok, State};
        false ->
            RemotePid = rpc:call(RemoteNode, erlang, whereis, [Tab]),
            case is_pid(RemotePid) of
                false ->
                    {reply, {error, remote_process_not_found}, State};
                true ->
                    case remote_add_node_to_schema(RemotePid, self(), Nodes) of
                        {ok, Dump, OtherNodes} ->
                            Mon = erlang:monitor(process, RemotePid),
                            OtherPids = [Pid || {_, Pid, _} <- OtherNodes],
                            Nodes2 = start_proxies_for([RemotePid|OtherPids]) ++ Nodes,
                            %% Ask our node to replicate data there before applying the dump
                            update_pt(Tab, Nodes2),
                            OurDump = dump(Tab),
                            send_dump_to_remote_node(RemotePid, self(), OurDump),
                            insert_many(Tab, Dump),
                            %% Add ourself into remote schema
                            %% Add remote nodes into our schema
                            %% Copy from our node / Copy into our node
                            {reply, ok, State#{other_nodes => Nodes2}};
                       Other ->
                            error_logger:error_msg("remote_add_node_to_schema failed ~p", [Other]),
                            {reply, {error, remote_add_node_to_schema_failed}, State}
                    end
            end
    end.

handle_remote_add_node_to_schema(ServerPid, OtherNodes, State = #{tab := Tab, other_nodes := Nodes}) ->
    RemoteNode = node(ServerPid),
    case lists:member(RemoteNode, Nodes) of
        true ->
            %% Already added (should not happen)
            {reply, {error, already_added}, State};
        false ->
            Mon = erlang:monitor(process, ServerPid),
            OtherPids = [Pid || {_, Pid, _} <- OtherNodes],
            Nodes2 = start_proxies_for([ServerPid|OtherPids]) ++ Nodes,
            update_pt(Tab, Nodes2),
            {reply, {ok, dump(Tab), Nodes}, State#{other_nodes => Nodes2}}
    end.

start_proxies_for([RemotePid|OtherPids]) ->
    RemoteNode = node(RemotePid),
    {ok, ProxyPid} = kiss_proxy:start(RemotePid),
    [{RemoteNode, RemotePid, ProxyPid}|start_proxies_for(OtherPids)];
start_proxies_for([]) ->
    [].

handle_send_dump_to_remote_node(_FromPid, Dump, State = #{tab := Tab}) ->
    insert_many(Tab, Dump),
    {reply, ok, State}.

insert_many(Tab, [Rec|Recs]) ->
    ets:insert_new(Tab, Rec),
    insert_many(Tab, Recs);
insert_many(_Tab, []) ->
    ok.

handle_remote_down(Pid, State = #{tab := Tab, other_nodes := Nodes}) ->
    Nodes2 = lists:keydelete(Pid, 2, Nodes),
    update_pt(Tab, Nodes2),
    {noreply, State#{other_nodes => Nodes2}}.

%% Called each time other_nodes changes
update_pt(Tab, Nodes2) ->
    kiss_pt:put(Tab, Nodes2).
