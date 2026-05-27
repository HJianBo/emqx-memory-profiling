%% Usage in an EMQX remote console:
%%   c("/tmp/emqx_mqueue_check.erl").
%%   emqx_mqueue_check:start().
%%
%% Optional:
%%   emqx_mqueue_check:start(#{
%%       batch_size => 1000,
%%       interval_ms => 1000,
%%       max_full_scans => 2,
%%       nodes => cluster, %% cluster | local | [node()]
%%       rpc_timeout_ms => 10000,
%%       load_remote_module => true,
%%       log_file => "/tmp/emqx_mqueue_check.log"
%%   }).
%%
%% Check or stop:
%%   emqx_mqueue_check:status().
%%   emqx_mqueue_check:stop().

-module(emqx_mqueue_check).

-export([start/0, start/1, stop/0, status/0]).
-export([loop/2, loop_tick/2, tick/2]).
-export([scan_next_batch_local/3]).

-define(NAME, emqx_mqueue_check).
-define(DEFAULT_TAB, emqx_channel_info).

start() ->
    start(#{}).

start(Overrides) when is_map(Overrides) ->
    case whereis(?NAME) of
        Pid when is_pid(Pid) ->
            {already_running, Pid};
        undefined ->
            Cfg = normalize_cfg(maps:merge(default_cfg(), Overrides)),
            Parent = self(),
            {Pid, Mon} = spawn_monitor(fun() -> init(Cfg, Parent) end),
            receive
                {?NAME, started, Pid} ->
                    _ = erlang:demonitor(Mon, [flush]),
                    {started, Pid, log, maps:get(log_file, Cfg)};
                {'DOWN', Mon, process, Pid, Reason} ->
                    {error, {start_failed, Reason}}
            after 5000 ->
                _ = erlang:demonitor(Mon, [flush]),
                {error, {start_timeout, Pid}}
            end
    end.

stop() ->
    case whereis(?NAME) of
        Pid when is_pid(Pid) ->
            Pid ! stop,
            {stopping, Pid};
        undefined ->
            stopped
    end.

status() ->
    case whereis(?NAME) of
        Pid when is_pid(Pid) ->
            Ref = make_ref(),
            Pid ! {status, self(), Ref},
            receive
                {Ref, Status} -> Status
            after 5000 ->
                {running, Pid, status_timeout}
            end;
        undefined ->
            stopped
    end.

default_cfg() ->
    #{
        tab => ?DEFAULT_TAB,
        batch_size => 1000,
        interval_ms => 1000,
        max_full_scans => 2,
        nodes => cluster,
        rpc_timeout_ms => 10000,
        load_remote_module => true,
        log_batches => true,
        log_file => "/tmp/emqx_mqueue_check.log"
    }.

normalize_cfg(Cfg0) ->
    Cfg0#{
        batch_size := pos_int(maps:get(batch_size, Cfg0), 1000),
        interval_ms := pos_int(maps:get(interval_ms, Cfg0), 1000),
        rpc_timeout_ms := pos_int(maps:get(rpc_timeout_ms, Cfg0), 10000),
        max_full_scans := max_scans(maps:get(max_full_scans, Cfg0))
    }.

pos_int(V, _Default) when is_integer(V), V > 0 -> V;
pos_int(_, Default) -> Default.

max_scans(infinity) -> infinity;
max_scans(V) when is_integer(V), V > 0 -> V;
max_scans(_) -> 2.

init(Cfg0, Parent) ->
    set_local_group_leader(),
    true = register(?NAME, self()),
    Parent ! {?NAME, started, self()},
    RequestedNodes = resolve_nodes(maps:get(nodes, Cfg0, cluster)),
    RemoteLoad = ensure_remote_modules(Cfg0, RequestedNodes),
    {Nodes, SkippedNodes} = filter_scan_nodes(RequestedNodes, RemoteLoad),
    Cfg = Cfg0#{
        nodes := Nodes,
        requested_nodes => RequestedNodes,
        skipped_nodes => SkippedNodes,
        remote_module_load => RemoteLoad
    },
    ok = append_log(
        Cfg,
        "~p start pid=~w node=~w tab=~w batch_size=~w interval_ms=~w max_full_scans=~w "
        "rpc_timeout_ms=~w requested_nodes=~p scan_nodes=~p skipped_nodes=~p "
        "remote_module_load=~p log=~s~n",
        [
            now_ms(),
            self(),
            node(),
            maps:get(tab, Cfg),
            maps:get(batch_size, Cfg),
            maps:get(interval_ms, Cfg),
            maps:get(max_full_scans, Cfg),
            maps:get(rpc_timeout_ms, Cfg),
            RequestedNodes,
            Nodes,
            SkippedNodes,
            RemoteLoad,
            maps:get(log_file, Cfg)
        ]
    ),
    State = new_state(Nodes),
    ok = log_scan_start(Cfg, State),
    ?MODULE:loop(Cfg, State).

loop(Cfg, State) ->
    receive
        stop ->
            append_log(Cfg, "~p stop pid=~w status=stopped~n", [now_ms(), self()]),
            stopped;
        {status, From, Ref} ->
            From ! {Ref, public_status(Cfg, State)},
            ?MODULE:loop(Cfg, State)
    after maps:get(interval_ms, Cfg) ->
        ?MODULE:loop_tick(Cfg, State)
    end.

loop_tick(Cfg, State) ->
    case ?MODULE:tick(Cfg, State) of
        {continue, State1} ->
            ?MODULE:loop(Cfg, State1);
        {stop, State1} ->
            State1
    end.

tick(Cfg, State) ->
    case scan_batch(Cfg, State) of
        {continue, State1} ->
            {continue, State1};
        {scan_done, State1} ->
            finish_scan(Cfg, State1);
        {fatal, Reason, State1} ->
            append_log(Cfg, "~p fatal reason=~p~n", [now_ms(), Reason]),
            {stop, State1#{last_error => Reason}}
    end.

resolve_nodes(local) ->
    [node()];
resolve_nodes(cluster) ->
    RunningNodes =
        try emqx:running_nodes() of
            Nodes when is_list(Nodes) -> Nodes;
            _ -> nodes()
        catch
            _:_ -> nodes()
        end,
    normalize_nodes([node() | RunningNodes]);
resolve_nodes(Nodes) when is_list(Nodes) ->
    case normalize_nodes(Nodes) of
        [] -> [node()];
        Nodes1 -> Nodes1
    end;
resolve_nodes(Node) when is_atom(Node) ->
    [Node];
resolve_nodes(_) ->
    [node()].

normalize_nodes(Nodes) ->
    lists:usort([Node || Node <- Nodes, is_atom(Node)]).

ensure_remote_modules(#{load_remote_module := false}, Nodes) ->
    maps:from_list([{Node, no_load_status(Node)} || Node <- Nodes]);
ensure_remote_modules(Cfg, Nodes) ->
    case module_binary() of
        {ok, Mod, Beam, File} ->
            maps:from_list([
                {Node, ensure_remote_module(Cfg, Node, Mod, Beam, File)}
             || Node <- Nodes
            ]);
        {error, Reason} ->
            maps:from_list([{Node, module_binary_error_status(Node, Reason)} || Node <- Nodes])
    end.

no_load_status(Node) when Node =:= node() ->
    local;
no_load_status(_Node) ->
    skipped.

module_binary_error_status(Node, _Reason) when Node =:= node() ->
    local;
module_binary_error_status(_Node, Reason) ->
    {error, Reason}.

module_binary() ->
    case code:get_object_code(?MODULE) of
        {Mod, Beam, File} ->
            {ok, Mod, Beam, File};
        error ->
            module_binary_from_loaded_file()
    end.

module_binary_from_loaded_file() ->
    case code:which(?MODULE) of
        File when is_list(File) ->
            case file:read_file(File) of
                {ok, Beam} ->
                    {ok, ?MODULE, Beam, File};
                {error, Reason} ->
                    module_binary_from_source({read_beam_failed, File, Reason})
            end;
        Other ->
            module_binary_from_source({no_object_code, Other})
    end.

module_binary_from_source(Why) ->
    case lists:keyfind(source, 1, ?MODULE:module_info(compile)) of
        {source, Source} when is_list(Source) ->
            case compile:file(Source, [binary]) of
                {ok, Mod, Beam} ->
                    {ok, Mod, Beam, Source};
                {ok, Mod, Beam, _Warnings} ->
                    {ok, Mod, Beam, Source};
                error ->
                    {error, {Why, {compile_failed, Source}}};
                Other ->
                    {error, {Why, {unexpected_compile_result, Source, Other}}}
            end;
        _ ->
            {error, {Why, no_source_file}}
    end.

ensure_remote_module(_Cfg, Node, _Mod, _Beam, _File) when Node =:= node() ->
    local;
ensure_remote_module(Cfg, Node, Mod, Beam, File) ->
    Timeout = maps:get(rpc_timeout_ms, Cfg, 10000),
    case rpc:call(Node, code, load_binary, [Mod, File, Beam], Timeout) of
        {module, Mod} ->
            ok;
        {error, not_purged} ->
            reload_remote_module(Cfg, Node, Mod, Beam, File);
        Other ->
            {error, Other}
    end.

reload_remote_module(Cfg, Node, Mod, Beam, File) ->
    Timeout = maps:get(rpc_timeout_ms, Cfg, 10000),
    case rpc:call(Node, code, soft_purge, [Mod], Timeout) of
        true ->
            _ = rpc:call(Node, code, delete, [Mod], Timeout),
            case rpc:call(Node, code, load_binary, [Mod, File, Beam], Timeout) of
                {module, Mod} -> ok;
                Other -> {error, Other}
            end;
        false ->
            {error, not_purged}
    end.

filter_scan_nodes(Nodes, RemoteLoad) ->
    lists:foldr(
        fun(Node, {ScanNodes, Skipped}) ->
            case scan_node_allowed(Node, RemoteLoad) of
                true ->
                    {[Node | ScanNodes], Skipped};
                false ->
                    {ScanNodes, [{Node, maps:get(Node, RemoteLoad, missing)} | Skipped]}
            end
        end,
        {[], []},
        Nodes
    ).

scan_node_allowed(Node, RemoteLoad) ->
    case maps:get(Node, RemoteLoad, missing) of
        local -> true;
        ok -> true;
        skipped -> true;
        _ -> false
    end.

new_state(Nodes) ->
    #{
        scan_no => 1,
        done_scans => 0,
        nodes => Nodes,
        pending_nodes => Nodes,
        node_cursors => maps:from_list([{Node, start} || Node <- Nodes]),
        node_progress => init_node_progress(Nodes),
        batch_no => 0,
        scan_started_ms => now_ms(),
        stats => empty_stats(),
        client_summary => [],
        last_error => undefined
    }.

reset_for_next_scan(State) ->
    Nodes = maps:get(nodes, State),
    State#{
        scan_no := maps:get(scan_no, State) + 1,
        pending_nodes := Nodes,
        node_cursors := maps:from_list([{Node, start} || Node <- Nodes]),
        node_progress := init_node_progress(Nodes),
        batch_no := 0,
        scan_started_ms := now_ms(),
        stats := empty_stats(),
        client_summary := [],
        last_error := undefined
    }.

init_node_progress(Nodes) ->
    maps:from_list([{Node, empty_node_progress()} || Node <- Nodes]).

empty_node_progress() ->
    #{
        batches => 0,
        scanned => 0,
        last_clientid => undefined
    }.

empty_stats() ->
    #{
        sessions => 0,
        nonzero_sessions => 0,
        nonzero_mqueue_sessions => 0,
        nonzero_inflight_sessions => 0,
        total_mqueue_len => 0,
        total_inflight_cnt => 0,
        max_mqueue_len => 0,
        max_inflight_cnt => 0,
        node_errors => 0
    }.

scan_batch(_Cfg, State = #{pending_nodes := []}) ->
    {scan_done, State};
scan_batch(Cfg, State) ->
    [Node | RestNodes] = maps:get(pending_nodes, State),
    Cursor = maps:get(Node, maps:get(node_cursors, State), start),
    Res = rpc_next_batch(Cfg, Node, Cursor),
    case Res of
        {ok, Batch, NextCursor, Done} ->
            State1 = process_batch_result(Cfg, Node, Batch, State),
            case Done of
                true ->
                    State2 = mark_node_done(Cfg, Node, State1#{pending_nodes := RestNodes}),
                    continue_or_finish(State2);
                false ->
                    Cursors1 = maps:put(Node, NextCursor, maps:get(node_cursors, State1)),
                    {continue, State1#{
                        pending_nodes := RestNodes ++ [Node],
                        node_cursors := Cursors1
                    }}
            end;
        {rpc_error, Reason} ->
            State1 = mark_node_error(Cfg, Node, Reason, State#{pending_nodes := RestNodes}),
            continue_or_finish(State1);
        Other ->
            Reason = #{unexpected_scan_result => Other},
            State1 = mark_node_error(Cfg, Node, Reason, State#{pending_nodes := RestNodes}),
            continue_or_finish(State1)
    end.

rpc_next_batch(Cfg, Node, Cursor) ->
    rpc_call(
        Node,
        ?MODULE,
        scan_next_batch_local,
        [maps:get(tab, Cfg), Cursor, maps:get(batch_size, Cfg)],
        maps:get(rpc_timeout_ms, Cfg)
    ).

rpc_call(Node, Mod, Fun, Args, Timeout) ->
    try rpc:call(Node, Mod, Fun, Args, Timeout) of
        {badrpc, Reason} -> {rpc_error, {badrpc, Reason}};
        Res -> Res
    catch
        C:R ->
            {rpc_error, #{class => C, reason => R}}
    end.

continue_or_finish(#{pending_nodes := []} = State) ->
    {scan_done, State};
continue_or_finish(State) ->
    {continue, State}.

scan_next_batch_local(Tab, Cursor0, BatchSize) ->
    case ets:info(Tab) of
        undefined ->
            {rpc_error, {missing_ets_table, Tab}};
        _ ->
            try
                Cursor = normalize_cursor(Tab, Cursor0),
                {Batch, NextCursor, Done} =
                    scan_next_keys(Tab, Cursor, BatchSize, empty_batch_result()),
                {ok, Batch, NextCursor, Done}
            catch
                C:R ->
                    {rpc_error, #{class => C, reason => R}}
            end
    end.

normalize_cursor(Tab, start) ->
    ets:first(Tab);
normalize_cursor(_Tab, Cursor) ->
    Cursor.

scan_next_keys(_Tab, '$end_of_table', _Left, Batch) ->
    {finish_batch_result(Batch), '$end_of_table', true};
scan_next_keys(_Tab, Key, 0, Batch) ->
    {finish_batch_result(Batch), Key, false};
scan_next_keys(Tab, Key, Left, Batch0) ->
    Batch1 = add_key_to_batch(Tab, Key, Batch0),
    NextKey = ets:next(Tab, Key),
    case NextKey of
        '$end_of_table' ->
            {finish_batch_result(Batch1), '$end_of_table', true};
        _ ->
            scan_next_keys(Tab, NextKey, Left - 1, Batch1)
    end.

empty_batch_result() ->
    #{
        sessions => 0,
        nonzero_sessions => 0,
        nonzero_mqueue_sessions => 0,
        nonzero_inflight_sessions => 0,
        total_mqueue_len => 0,
        total_inflight_cnt => 0,
        max_mqueue_len => 0,
        max_inflight_cnt => 0,
        hits => [],
        last_clientid => undefined
    }.

add_key_to_batch(Tab, {ClientId, _Pid} = Key, Batch0) ->
    try ets:lookup_element(Tab, Key, 3) of
        Stats ->
            add_stats_to_batch(ClientId, Stats, Batch0)
    catch
        error:badarg ->
            %% The row may have been deleted between ets:first/next and lookup_element.
            Batch0
    end;
add_key_to_batch(_Tab, _Key, Batch) ->
    Batch.

add_stats_to_batch(ClientId, Stats, Batch0) ->
    MQLen = stat_int(mqueue_len, Stats),
    Inflight = stat_int(inflight_cnt, Stats),
    Total = MQLen + Inflight,
    MQHit = MQLen > 0,
    InflightHit = Inflight > 0,
    Batch1 = Batch0#{
        sessions := maps:get(sessions, Batch0) + 1,
        nonzero_mqueue_sessions :=
            maps:get(nonzero_mqueue_sessions, Batch0) + bool_int(MQHit),
        nonzero_inflight_sessions :=
            maps:get(nonzero_inflight_sessions, Batch0) + bool_int(InflightHit),
        total_mqueue_len := maps:get(total_mqueue_len, Batch0) + MQLen,
        total_inflight_cnt := maps:get(total_inflight_cnt, Batch0) + Inflight,
        max_mqueue_len := max(maps:get(max_mqueue_len, Batch0), MQLen),
        max_inflight_cnt := max(maps:get(max_inflight_cnt, Batch0), Inflight),
        last_clientid := ClientId
    },
    case Total > 0 of
        true ->
            Client = #{
                clientid => ClientId,
                mqueue_len => MQLen,
                inflight_cnt => Inflight,
                total => Total
            },
            Batch1#{
                nonzero_sessions := maps:get(nonzero_sessions, Batch1) + 1,
                hits := [Client | maps:get(hits, Batch1)]
            };
        false ->
            Batch1
    end.

finish_batch_result(Batch) ->
    Batch#{hits := lists:reverse(maps:get(hits, Batch))}.

mark_node_done(Cfg, Node, State) ->
    ok = append_log(
        Cfg,
        "~p node_done scan=~w node=~w node_scanned=~w node_batches=~w last_clientid=~s~n",
        [
            now_ms(),
            maps:get(scan_no, State),
            Node,
            node_progress_value(Node, scanned, State),
            node_progress_value(Node, batches, State),
            fmt(node_progress_value(Node, last_clientid, State))
        ]
    ),
    State#{node_cursors := maps:remove(Node, maps:get(node_cursors, State))}.

mark_node_error(Cfg, Node, Reason, State) ->
    Stats0 = maps:get(stats, State),
    Stats1 = Stats0#{node_errors := maps:get(node_errors, Stats0) + 1},
    ok = append_log(
        Cfg,
        "~p node_error scan=~w node=~w reason=~p~n",
        [now_ms(), maps:get(scan_no, State), Node, Reason]
    ),
    State#{
        node_cursors := maps:remove(Node, maps:get(node_cursors, State)),
        stats := Stats1,
        last_error := {Node, Reason}
    }.

process_batch_result(Cfg, Node, Batch, State) ->
    ScanNo = maps:get(scan_no, State),
    BatchNo = maps:get(batch_no, State) + 1,
    Stats0 = maps:get(stats, State),
    Stats1 = merge_batch_stats(Stats0, Batch),
    Summary1 = lists:reverse(
        compact_clients_with_node(Node, Batch),
        maps:get(client_summary, State, [])
    ),
    ok = log_batch_hits(Cfg, Node, ScanNo, BatchNo, maps:get(hits, Batch)),
    State1 = update_node_progress(Node, Batch, State#{
        batch_no := BatchNo,
        stats := Stats1,
        client_summary := Summary1
    }),
    ok = maybe_log_batch(Cfg, Node, ScanNo, BatchNo, Batch, Stats1, State1),
    State1.

merge_batch_stats(Stats0, Batch) ->
    Stats0#{
        sessions := maps:get(sessions, Stats0) + maps:get(sessions, Batch),
        nonzero_sessions :=
            maps:get(nonzero_sessions, Stats0) + maps:get(nonzero_sessions, Batch),
        nonzero_mqueue_sessions :=
            maps:get(nonzero_mqueue_sessions, Stats0) + maps:get(nonzero_mqueue_sessions, Batch),
        nonzero_inflight_sessions :=
            maps:get(nonzero_inflight_sessions, Stats0) + maps:get(nonzero_inflight_sessions, Batch),
        total_mqueue_len :=
            maps:get(total_mqueue_len, Stats0) + maps:get(total_mqueue_len, Batch),
        total_inflight_cnt :=
            maps:get(total_inflight_cnt, Stats0) + maps:get(total_inflight_cnt, Batch),
        max_mqueue_len :=
            max(maps:get(max_mqueue_len, Stats0), maps:get(max_mqueue_len, Batch)),
        max_inflight_cnt :=
            max(maps:get(max_inflight_cnt, Stats0), maps:get(max_inflight_cnt, Batch))
    }.

log_batch_hits(_Cfg, _Node, _ScanNo, _BatchNo, []) ->
    ok;
log_batch_hits(Cfg, Node, ScanNo, BatchNo, [Client | Rest]) ->
    ok = log_hit(Cfg, Node, ScanNo, BatchNo, Client),
    log_batch_hits(Cfg, Node, ScanNo, BatchNo, Rest).

bool_int(true) -> 1;
bool_int(false) -> 0.

update_node_progress(Node, Batch, State) ->
    Progress0 = maps:get(Node, maps:get(node_progress, State), empty_node_progress()),
    Progress1 = Progress0#{
        batches := maps:get(batches, Progress0) + 1,
        scanned := maps:get(scanned, Progress0) + maps:get(sessions, Batch),
        last_clientid := maps:get(last_clientid, Batch)
    },
    ProgressMap1 = maps:put(Node, Progress1, maps:get(node_progress, State)),
    State#{node_progress := ProgressMap1}.

stat_int(Key, Stats) ->
    case stat_value(Key, Stats, 0) of
        V when is_integer(V) -> V;
        _ -> 0
    end.

stat_value(Key, Stats, Default) when is_list(Stats) ->
    case lists:keyfind(Key, 1, Stats) of
        {Key, V} -> V;
        false -> Default
    end;
stat_value(_, _, Default) ->
    Default.

finish_scan(Cfg, State) ->
    Done = maps:get(done_scans, State) + 1,
    ScanNo = maps:get(scan_no, State),
    Stats = maps:get(stats, State),
    Elapsed = now_ms() - maps:get(scan_started_ms, State),
    ClientSummary = lists:reverse(maps:get(client_summary, State, [])),
    ok = append_log(
        Cfg,
        "~p scan_done scan=~w elapsed_ms=~w sessions=~w nonzero=~w total_mqueue_len=~w "
        "total_inflight_cnt=~w total_buffered=~w max_mqueue_len=~w max_inflight_cnt=~w "
        "mqueue_nonzero_sessions=~w inflight_nonzero_sessions=~w node_errors=~w "
        "done_scans=~w nodes=~p~n",
        [
            now_ms(),
            ScanNo,
            Elapsed,
            maps:get(sessions, Stats),
            maps:get(nonzero_sessions, Stats),
            maps:get(total_mqueue_len, Stats),
            maps:get(total_inflight_cnt, Stats),
            maps:get(total_mqueue_len, Stats) + maps:get(total_inflight_cnt, Stats),
            maps:get(max_mqueue_len, Stats),
            maps:get(max_inflight_cnt, Stats),
            maps:get(nonzero_mqueue_sessions, Stats),
            maps:get(nonzero_inflight_sessions, Stats),
            maps:get(node_errors, Stats),
            Done,
            maps:get(nodes, State)
        ]
    ),
    ok = log_client_summary(Cfg, ScanNo, ClientSummary),
    State1 = State#{done_scans := Done},
    case reached_max_scans(Cfg, Done) of
        true ->
            append_log(
                Cfg,
                "~p all_done done_scans=~w max_full_scans=~w pid=~w~n",
                [now_ms(), Done, maps:get(max_full_scans, Cfg), self()]
            ),
            {stop, State1};
        false ->
            State2 = reset_for_next_scan(State1),
            ok = log_scan_start(Cfg, State2),
            {continue, State2}
    end.

reached_max_scans(#{max_full_scans := infinity}, _Done) -> false;
reached_max_scans(#{max_full_scans := Max}, Done) -> Done >= Max.

log_scan_start(Cfg, State) ->
    append_log(
        Cfg,
        "~p scan_start scan=~w done_scans=~w nodes=~p~n",
        [
            now_ms(),
            maps:get(scan_no, State),
            maps:get(done_scans, State),
            maps:get(nodes, State)
        ]
    ).

maybe_log_batch(
    #{log_batches := false},
    _Node,
    _ScanNo,
    _BatchNo,
    _Batch,
    _Stats,
    _State
) ->
    ok;
maybe_log_batch(Cfg, Node, ScanNo, BatchNo, Batch, Stats, State) ->
    {NodeIndex, NodeTotal} = node_position(Node, maps:get(nodes, State)),
    Clients = compact_clients(Batch),
    append_log(
        Cfg,
        "~p batch scan=~w batch=~w node=~w node_pos=~w/~w node_batch=~w rows=~w "
        "node_scanned=~w total_scanned=~w hits=~w mqueue_nonzero=~w inflight_nonzero=~w "
        "total_mqueue_len=~w total_inflight_cnt=~w last_clientid=~s "
        "client_mqueue_inflight=~s~n",
        [
            now_ms(),
            ScanNo,
            BatchNo,
            Node,
            NodeIndex,
            NodeTotal,
            node_progress_value(Node, batches, State),
            maps:get(sessions, Batch),
            node_progress_value(Node, scanned, State),
            maps:get(sessions, Stats),
            maps:get(nonzero_sessions, Batch),
            maps:get(nonzero_mqueue_sessions, Batch),
            maps:get(nonzero_inflight_sessions, Batch),
            maps:get(total_mqueue_len, Stats),
            maps:get(total_inflight_cnt, Stats),
            fmt(maps:get(last_clientid, Batch)),
            fmt(Clients)
        ]
    ).

compact_clients(Batch) ->
    [
        {maps:get(clientid, Client), maps:get(mqueue_len, Client), maps:get(inflight_cnt, Client)}
     || Client <- maps:get(hits, Batch)
    ].

compact_clients_with_node(Node, Batch) ->
    [
        {Node, maps:get(clientid, Client), maps:get(mqueue_len, Client), maps:get(inflight_cnt, Client)}
     || Client <- maps:get(hits, Batch)
    ].

log_client_summary(Cfg, ScanNo, ClientSummary) ->
    append_log(
        Cfg,
        "~p client_summary scan=~w count=~w node_client_mqueue_inflight=~s~n",
        [now_ms(), ScanNo, length(ClientSummary), fmt(ClientSummary)]
    ).

log_hit(Cfg, Node, ScanNo, BatchNo, Client) ->
    append_log(
        Cfg,
        "~p hit scan=~w batch=~w node=~w clientid=~s mqueue_len=~w inflight_cnt=~w total=~w~n",
        [
            now_ms(),
            ScanNo,
            BatchNo,
            Node,
            fmt(maps:get(clientid, Client)),
            maps:get(mqueue_len, Client),
            maps:get(inflight_cnt, Client),
            maps:get(total, Client)
        ]
    ).

public_status(Cfg, State) ->
    Stats = maps:get(stats, State),
    #{
        status => running,
        pid => self(),
        node => node(),
        tab => maps:get(tab, Cfg),
        log_file => maps:get(log_file, Cfg),
        batch_size => maps:get(batch_size, Cfg),
        interval_ms => maps:get(interval_ms, Cfg),
        max_full_scans => maps:get(max_full_scans, Cfg),
        rpc_timeout_ms => maps:get(rpc_timeout_ms, Cfg),
        requested_nodes => maps:get(requested_nodes, Cfg, maps:get(nodes, Cfg)),
        nodes => maps:get(nodes, Cfg),
        skipped_nodes => maps:get(skipped_nodes, Cfg, []),
        remote_module_load => maps:get(remote_module_load, Cfg, #{}),
        scan_no => maps:get(scan_no, State),
        done_scans => maps:get(done_scans, State),
        batch_no => maps:get(batch_no, State),
        pending_nodes => maps:get(pending_nodes, State),
        node_progress => maps:get(node_progress, State),
        scan_position => scan_position(maps:get(pending_nodes, State)),
        current_scan_stats => Stats,
        client_summary_count => length(maps:get(client_summary, State, [])),
        last_error => maps:get(last_error, State)
    }.

scan_position([]) -> finishing;
scan_position(_) -> scanning.

node_progress_value(Node, Key, State) ->
    Progress = maps:get(Node, maps:get(node_progress, State), empty_node_progress()),
    maps:get(Key, Progress).

node_position(Node, Nodes) ->
    node_position(Node, Nodes, 1, length(Nodes)).

node_position(_Node, [], _Index, Total) ->
    {0, Total};
node_position(Node, [Node | _], Index, Total) ->
    {Index, Total};
node_position(Node, [_ | Rest], Index, Total) ->
    node_position(Node, Rest, Index + 1, Total).

append_log(Cfg, Fmt, Args) ->
    LogFile = maps:get(log_file, Cfg),
    _ = filelib:ensure_dir(LogFile),
    case file:write_file(LogFile, io_lib:format(Fmt, Args), [append]) of
        ok -> ok;
        {error, _Reason} -> ok
    end.

fmt(Term) ->
    io_lib:format("~p", [Term]).

now_ms() ->
    erlang:system_time(millisecond).

set_local_group_leader() ->
    case whereis(user) of
        Pid when is_pid(Pid) ->
            group_leader(Pid, self());
        _ ->
            ok
    end.
