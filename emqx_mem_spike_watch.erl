%% Usage:
%%   1. Copy this file to the EMQX node, for example:
%%        scp emqx_mem_spike_watch.erl HOST:/tmp/
%%   2. Open `bin/emqx remote_console`, then run:
%%        c("/tmp/emqx_mem_spike_watch.erl").
%%        emqx_mem_spike_watch:start().
%%   3. Check or stop:
%%        emqx_mem_spike_watch:status().
%%        emqx_mem_spike_watch:stop().
%%
%% Optional:
%%   emqx_mem_spike_watch:start(#{max_dumps => 5, out_dir => "/tmp"}).
%%
%% Update:
%%   Copy the changed file again and run c("/tmp/emqx_mem_spike_watch.erl").
%%   c/1 compiles and loads the new module. Restart only when changing config.
%%
%% Output:
%%   /tmp/emqx_spike_watch.log
%%   /tmp/emqx_spike_<timestamp_ms>.term

-module(emqx_mem_spike_watch).

-export([start/0, start/1, status/0, stop/0]).
%% Exported so a running watcher can switch code after c/1 reloads the module.
-export([loop/2, loop_tick/2]).

-define(NAME, emqx_mem_spike_watch).

start() ->
    start(#{}).

start(Overrides) when is_map(Overrides) ->
    case whereis(?NAME) of
        Pid when is_pid(Pid) ->
            {already_running, Pid};
        undefined ->
            Cfg = maps:merge(default_cfg(), Overrides),
            Parent = self(),
            {Pid, Mon} = spawn_monitor(fun() -> watcher_init(Cfg, Parent) end),
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

status() ->
    case whereis(?NAME) of
        Pid when is_pid(Pid) -> {running, Pid};
        undefined -> stopped
    end.

stop() ->
    case whereis(?NAME) of
        Pid when is_pid(Pid) ->
            Pid ! stop,
            {stopping, Pid};
        undefined ->
            stopped
    end.

default_cfg() ->
    #{
        interval_ms => 1000,
        cooldown_ms => 3000,
        dump_window_ms => 1 * 60 * 1000,
        dump_timeout_ms => 30 * 1000,
        max_dumps => 3,
        total_jump_threshold => 1024 * 1024 * 1024,
        binary_jump_threshold => 512 * 1024 * 1024,
        processes_jump_threshold => 512 * 1024 * 1024,
        dist_delta_threshold => 128 * 1024 * 1024,
        top_n => 20,
        out_dir => "/tmp",
        log_file => "/tmp/emqx_spike_watch.log"
    }.

watcher_init(Cfg, Parent) ->
    set_local_group_leader(),
    true = register(?NAME, self()),
    M0 = mem(),
    D0 = dist_stats(),
    ok = append_log(
        Cfg,
        "~p started pid=~w interval=~w cooldown=~w window=~w timeout=~w max=~w top_n=~w out=~s log=~s~n",
        [
            now_ms(),
            self(),
            maps:get(interval_ms, Cfg),
            maps:get(cooldown_ms, Cfg),
            maps:get(dump_window_ms, Cfg),
            maps:get(dump_timeout_ms, Cfg),
            maps:get(max_dumps, Cfg),
            maps:get(top_n, Cfg),
            maps:get(out_dir, Cfg),
            maps:get(log_file, Cfg)
        ]
    ),
    Parent ! {?NAME, started, self()},
    ?MODULE:loop(Cfg, #{
        last_mem => M0,
        last_dist => D0,
        last_dump_ms => 0,
        dump_seq => 0,
        dumped_in_window => 0,
        dump_window_start_ms => 0,
        dumping => false,
        last_skip_log => undefined
    }).

loop(Cfg, State) ->
    receive
        stop ->
            append_log(Cfg, "~p stopped pid=~w~n", [now_ms(), self()]),
            stopped;
        {emqx_mem_spike_dump_done, DumpNo, Status} ->
            {IsActiveDone, _ActiveDump, StateAfterDone} = clear_active_dump(DumpNo, State),
            append_log(
                Cfg,
                "~p dump_done no=~w status=~w active=~w~n",
                [now_ms(), DumpNo, Status, IsActiveDone]
            ),
            ?MODULE:loop(Cfg, StateAfterDone);
        {emqx_mem_spike_dump_done, DumpNo} ->
            {IsActiveDone, _ActiveDump, StateAfterDone} = clear_active_dump(DumpNo, State),
            append_log(
                Cfg,
                "~p dump_done no=~w status=unknown active=~w~n",
                [now_ms(), DumpNo, IsActiveDone]
            ),
            ?MODULE:loop(Cfg, StateAfterDone);
        {'DOWN', DumpMon, process, DumpPid, Reason} ->
            ?MODULE:loop(Cfg, handle_dump_down(Cfg, DumpMon, DumpPid, Reason, State))
    after maps:get(interval_ms, Cfg) ->
        ?MODULE:loop_tick(Cfg, State)
    end.

loop_tick(Cfg, State) ->
    ?MODULE:loop(Cfg, tick(Cfg, State)).

tick(Cfg, State) ->
    Now = now_ms(),
    State0 = maybe_timeout_active_dump(Cfg, Now, State),
    {WindowStart0, DumpedInWindow0, WindowRemainingMs} = window_state(Cfg, Now, State0),
    M = mem(),
    LastM = maps:get(last_mem, State0),
    Dist = dist_stats(),
    DistDeltaMap = dist_deltas(Dist, maps:get(last_dist, State0)),
    LastDumpMs = maps:get(last_dump_ms, State0),
    DumpSeq0 = maps:get(dump_seq, State0),
    Dumping = maps:get(dumping, State0, false),
    {DoDump, Event0} = should_dump(
        Cfg,
        M,
        LastM,
        DistDeltaMap,
        LastDumpMs,
        DumpedInWindow0
    ),
    handle_dump_decision(
        Cfg,
        DoDump,
        Event0,
        Now,
        M,
        Dist,
        DumpSeq0,
        Dumping,
        WindowStart0,
        WindowRemainingMs,
        DumpedInWindow0,
        State0
    ).

handle_dump_decision(
    Cfg,
    true,
    Event0,
    Now,
    M,
    Dist,
    DumpSeq0,
    false,
    WindowStart0,
    WindowRemainingMs,
    DumpedInWindow0,
    State0
) ->
    DumpNo = DumpSeq0 + 1,
    DumpedInWindow = DumpedInWindow0 + 1,
    WindowMs = maps:get(dump_window_ms, Cfg),
    WindowStart =
        case WindowStart0 of
            0 -> Now;
            _ -> WindowStart0
        end,
    DumpWindowRemainingMs =
        case WindowStart0 of
            0 -> WindowMs;
            _ -> WindowRemainingMs
        end,
    Event = Event0#{
        dump_no => DumpNo,
        dumped_in_window => DumpedInWindow,
        dump_window_start_ms => WindowStart,
        monotonic_time_ms => erlang:monotonic_time(millisecond),
        wall_time_ms => Now
    },
    DumpPid = dump(Cfg, Event, DumpNo, self()),
    DumpMon = erlang:monitor(process, DumpPid),
    append_log(
        Cfg,
        "~p dump_start no=~w pid=~w in_window=~w/~w win_left=~w reasons=~w~n",
        [
            Now,
            DumpNo,
            DumpPid,
            DumpedInWindow,
            maps:get(max_dumps, Cfg),
            DumpWindowRemainingMs,
            maps:get(reasons, Event, [])
        ]
    ),
    State0#{
        last_mem => M,
        last_dist => Dist,
        last_dump_ms => Now,
        dump_seq => DumpNo,
        dumped_in_window => DumpedInWindow,
        dump_window_start_ms => WindowStart,
        dumping => {DumpNo, DumpPid, DumpMon, Now},
        last_skip_log => undefined
    };
handle_dump_decision(
    Cfg,
    false,
    Event0,
    Now,
    M,
    Dist,
    DumpSeq0,
    Dumping,
    WindowStart0,
    WindowRemainingMs,
    DumpedInWindow0,
    State0
) ->
    StateAfterSkipLog =
        case maps:get(skip_reasons, Event0, []) of
            [] ->
                State0#{last_skip_log => undefined};
            SkipReasons ->
                maybe_log_skip(
                    Cfg,
                    {skip, SkipReasons, DumpSeq0, Dumping =/= false},
                    "~p dump_skip reason=~w seq=~w in_window=~w/~w win_left=~w cooldown=~w dumping=~w triggers=~w~n",
                    [
                        Now,
                        SkipReasons,
                        DumpSeq0,
                        DumpedInWindow0,
                        maps:get(max_dumps, Cfg),
                        WindowRemainingMs,
                        maps:get(cooldown_remaining_ms, Event0, 0),
                        Dumping =/= false,
                        maps:get(reasons, Event0, [])
                    ],
                    State0
                )
        end,
    StateAfterSkipLog#{
        last_mem => M,
        last_dist => Dist,
        dumped_in_window => DumpedInWindow0,
        dump_window_start_ms => WindowStart0
    };
handle_dump_decision(
    Cfg,
    true,
    Event0,
    Now,
    M,
    Dist,
    DumpSeq0,
    _Dumping,
    WindowStart0,
    _WindowRemainingMs,
    DumpedInWindow0,
    State0
) ->
    StateAfterSkipLog = maybe_log_skip(
        Cfg,
        {already_dumping, DumpSeq0},
        "~p dump_skip reason=already_dumping seq=~w in_window=~w triggers=~w~n",
        [Now, DumpSeq0, DumpedInWindow0, maps:get(reasons, Event0, [])],
        State0
    ),
    StateAfterSkipLog#{
        last_mem => M,
        last_dist => Dist,
        dumped_in_window => DumpedInWindow0,
        dump_window_start_ms => WindowStart0
    }.

should_dump(Cfg, M, LastM, DistDeltaMap, LastDumpMs, DumpedInWindow) ->
    TotalJump = get(total, M) - get(total, LastM),
    BinaryJump = get(binary, M) - get(binary, LastM),
    ProcJump = get(processes, M) - get(processes, LastM),
    DistMax = max_dist_delta(DistDeltaMap),
    Checks = [
        {total_jump, TotalJump, maps:get(total_jump_threshold, Cfg)},
        {binary_jump, BinaryJump, maps:get(binary_jump_threshold, Cfg)},
        {processes_jump, ProcJump, maps:get(processes_jump_threshold, Cfg)},
        {dist_delta, DistMax, maps:get(dist_delta_threshold, Cfg)}
    ],
    Reasons = [{K, V} || {K, V, T} <- Checks, V >= T],
    EventReconAlloc =
        case Reasons of
            [] -> undefined;
            _ -> recon_alloc_brief()
        end,
    CooldownMs = maps:get(cooldown_ms, Cfg),
    CooldownRemainingMs = erlang:max(0, CooldownMs - (now_ms() - LastDumpMs)),
    CooldownOk = CooldownRemainingMs =:= 0,
    MaxDumps = maps:get(max_dumps, Cfg),
    DumpLimitOk = DumpedInWindow < MaxDumps,
    SkipReasons =
        case Reasons of
            [] ->
                [];
            _ ->
                [R || {Ok, R} <- [{CooldownOk, cooldown}, {DumpLimitOk, dump_limit}], Ok =:= false]
        end,
    {Reasons =/= [] andalso CooldownOk andalso DumpLimitOk, #{
        reasons => Reasons,
        skip_reasons => SkipReasons,
        cooldown_ok => CooldownOk,
        cooldown_remaining_ms => CooldownRemainingMs,
        dump_limit_ok => DumpLimitOk,
        dumped_in_window => DumpedInWindow,
        max_dumps => MaxDumps,
        checks => Checks,
        memory => M,
        recon_alloc => EventReconAlloc,
        memory_delta => #{
            total => TotalJump,
            binary => BinaryJump,
            processes => ProcJump
        },
        dist_deltas => DistDeltaMap
    }}.

dump(Cfg, Event, DumpNo, Parent) ->
    spawn(fun() -> dump_worker(Cfg, Event, DumpNo, Parent) end).

dump_worker(Cfg, Event, DumpNo, Parent) ->
    set_local_group_leader(),
    DumpStartMs = now_ms(),
    stage(Cfg, DumpNo, DumpStartMs, worker_started, DumpStartMs),
    Status =
        try
            do_dump(Cfg, Event, DumpNo, DumpStartMs)
        catch
            C:R:S ->
                append_log(
                    Cfg,
                    "~p dump_failed no=~w class=~w reason=~w stack_top=~w~n",
                    [now_ms(), DumpNo, C, R, lists:sublist(S, 3)]
                ),
                {failed, C, R}
        end,
    try
        Parent ! {emqx_mem_spike_dump_done, DumpNo, Status}
    catch
        _:_ -> ok
    end.

do_dump(Cfg, Event, DumpNo, DumpStartMs) ->
    N = maps:get(top_n, Cfg),
    TPreMemMs = now_ms(),
    PreMem = dumper_mem_snapshot(),
    stage(Cfg, DumpNo, DumpStartMs, dumper_pre_mem_done, TPreMemMs),

    TMemMs = now_ms(),
    MemTopFull = proc_count_full(memory, N),
    stage(Cfg, DumpNo, DumpStartMs, proc_count_memory_done, TMemMs),

    TMemBriefMs = now_ms(),
    MemPids = [P || {P, _V, _Info} <- MemTopFull],
    TClientIdsMs = now_ms(),
    TopMemClientIds = channel_client_ids(MemPids),
    stage(Cfg, DumpNo, DumpStartMs, channel_clientids_done, TClientIdsMs),

    ProcBriefs0 = add_proc_briefs(MemPids, TopMemClientIds, #{}),
    stage(Cfg, DumpNo, DumpStartMs, proc_brief_memory_done, TMemBriefMs),

    MemTop = [{P, V} || {P, V, _Info} <- MemTopFull],
    ProcessBriefs = [maps:get(P, ProcBriefs0) || P <- MemPids, maps:is_key(P, ProcBriefs0)],
    CandidateBinary = top_pairs(
        [{maps:get(pid, PB), get(bytes, maps:get(binary, PB, #{}))} || PB <- ProcessBriefs],
        N
    ),
    stage(Cfg, DumpNo, DumpStartMs, memory_candidates_done, TMemBriefMs),

    TPostMemMs = now_ms(),
    PostMem = dumper_mem_snapshot(),
    stage(Cfg, DumpNo, DumpStartMs, dumper_post_mem_done, TPostMemMs),

    TSnapshotMs = now_ms(),
    Snapshot = #{
        data0_dumper_meta => #{
            data1_dump_no => DumpNo,
            data2_node => node(),
            data3_top_n => N,
            data4_wall_time_ms => now_ms(),
            data5_monotonic_time_ms => erlang:monotonic_time(millisecond)
        },
        data1_watcher_event => Event,
        data2_dumper_pre_mem => PreMem,
        data3_dumper_recon_mem => #{
            data1_top_n => N,
            data2_top_memory => MemTop,
            data3_recon_proc_count_memory => MemTopFull
        },
        data4_dumper_recon_mem_proc_info => #{
            data1_top_memory_clientids => TopMemClientIds,
            data2_candidate_binary => CandidateBinary,
            data3_processes => ProcessBriefs
        },
        data5_dumper_post_mem => PostMem
    },
    stage(Cfg, DumpNo, DumpStartMs, snapshot_build, TSnapshotMs),

    TWriteSnapshotMs = now_ms(),
    Ts = integer_to_list(now_ms()),
    File = filename:join(maps:get(out_dir, Cfg), "emqx_spike_" ++ Ts ++ ".term"),
    ok = file:write_file(File, io_lib:format("~p.~n", [Snapshot])),
    stage(Cfg, DumpNo, DumpStartMs, write_snapshot, TWriteSnapshotMs),

    TWriteLogMs = now_ms(),
    ok = append_log(
        Cfg,
        "~p dump_file no=~w file=~s reasons=~w~n",
        [now_ms(), DumpNo, File, maps:get(reasons, Event, [])]
    ),
    stage(Cfg, DumpNo, DumpStartMs, write_log, TWriteLogMs),
    ok.

stage(Cfg, DumpNo, DumpStartMs, Label, SinceMs) ->
    Now = now_ms(),
    _ = catch append_log(
        Cfg,
        "~p stage no=~w s=~w ms=~w total=~w~n",
        [Now, DumpNo, Label, Now - SinceMs, Now - DumpStartMs]
    ),
    ok.

maybe_timeout_active_dump(Cfg, Now, State) ->
    case maps:get(dumping, State, false) of
        {DumpNo, DumpPid, DumpMon, DumpStartMs} when is_pid(DumpPid) ->
            maybe_kill_timed_out_dump(Cfg, Now, DumpNo, DumpPid, DumpStartMs, DumpMon, State);
        _ ->
            State
    end.

maybe_kill_timed_out_dump(Cfg, Now, DumpNo, DumpPid, DumpStartMs, DumpMon, State) ->
    DumpAgeMs = Now - DumpStartMs,
    case DumpAgeMs >= maps:get(dump_timeout_ms, Cfg) of
        true ->
            append_log(
                Cfg,
                "~p dump_timeout no=~w pid=~w age=~w timeout=~w~n",
                [Now, DumpNo, DumpPid, DumpAgeMs, maps:get(dump_timeout_ms, Cfg)]
            ),
            _ = erlang:demonitor(DumpMon, [flush]),
            exit(DumpPid, kill),
            State#{dumping => false, last_skip_log => undefined};
        false ->
            State
    end.

window_state(Cfg, Now, State) ->
    WindowMs = maps:get(dump_window_ms, Cfg),
    WindowStartRaw = maps:get(dump_window_start_ms, State),
    WindowExpired = WindowStartRaw =/= 0 andalso Now - WindowStartRaw >= WindowMs,
    {WindowStart, DumpedInWindow} =
        case WindowExpired of
            true -> {0, 0};
            false -> {WindowStartRaw, maps:get(dumped_in_window, State)}
        end,
    WindowRemainingMs =
        case WindowStart of
            0 -> 0;
            _ -> erlang:max(0, WindowMs - (Now - WindowStart))
        end,
    {WindowStart, DumpedInWindow, WindowRemainingMs}.

clear_active_dump(DoneDumpNo, State) ->
    ActiveDump = maps:get(dumping, State, false),
    case ActiveDump of
        {DoneDumpNo, _DumpPid, DumpMon, _DumpStartMs} ->
            _ = erlang:demonitor(DumpMon, [flush]),
            {true, ActiveDump, State#{dumping => false, last_skip_log => undefined}};
        _ ->
            {false, ActiveDump, State}
    end.

handle_dump_down(Cfg, DumpMon, DumpPid, Reason, State) ->
    ActiveDump = maps:get(dumping, State, false),
    case ActiveDump of
        {DumpNo, DumpPid, DumpMon, _DumpStartMs} ->
            append_log(
                Cfg,
                "~p dump_down no=~w pid=~w reason=~w active=true~n",
                [now_ms(), DumpNo, DumpPid, Reason]
            ),
            State#{dumping => false};
        _ ->
            append_log(
                Cfg,
                "~p dump_down pid=~w reason=~w active=false~n",
                [now_ms(), DumpPid, Reason]
            ),
            State
    end.

maybe_log_skip(Cfg, Key, Line, Args, State) ->
    case maps:get(last_skip_log, State, undefined) of
        Key ->
            State;
        _ ->
            append_log(Cfg, Line, Args),
            State#{last_skip_log => Key}
    end.

mem() ->
    maps:from_list(erlang:memory()).

recon_alloc_brief() ->
    #{
        allocated => safe_recon_alloc_memory(allocated),
        used => safe_recon_alloc_memory(used)
    }.

dumper_mem_snapshot() ->
    #{
        data1_memory => erlang:memory(),
        data2_alloc => recon_alloc_brief(),
        data3_fragmentation => safe_recon_alloc_fragmentation(current),
        data4_runtime => #{
            process_count => erlang:system_info(process_count),
            port_count => erlang:system_info(port_count),
            scheduler_wall_time => catch erlang:statistics(scheduler_wall_time)
        },
        data5_wall_time_ms => now_ms(),
        data6_monotonic_time_ms => erlang:monotonic_time(millisecond)
    }.

safe_recon_alloc_memory(Type) ->
    try recon_alloc:memory(Type) of
        Value -> Value
    catch
        C:R -> {error, {C, R}}
    end.

safe_recon_alloc_fragmentation(Type) ->
    try recon_alloc:fragmentation(Type) of
        Value -> Value
    catch
        C:R -> {error, {C, R}}
    end.

dist_stats() ->
    Raw =
        case catch erlang:system_info(dist_ctrl) of
            L when is_list(L) -> L;
            _ -> []
        end,
    maps:from_list([dist_stat(Item) || Item <- Raw]).

dist_stat({Node, Ctrl}) ->
    {Node, #{input => port_bytes(Ctrl, input), output => port_bytes(Ctrl, output)}};
dist_stat(Ctrl) ->
    {unknown, #{input => port_bytes(Ctrl, input), output => port_bytes(Ctrl, output)}}.

dist_deltas(Dist, LastDist) ->
    maps:map(
        fun(Node, Stat) ->
            Last = maps:get(Node, LastDist, #{}),
            In = get(input, Stat),
            Out = get(output, Stat),
            Stat#{
                input_delta => In - get(input, Last),
                output_delta => Out - get(output, Last)
            }
        end,
        Dist
    ).

max_dist_delta(Deltas) ->
    lists:max(
        [0] ++
            lists:flatmap(
                fun({_Node, S}) ->
                    [abs(get(input_delta, S)), abs(get(output_delta, S))]
                end,
                maps:to_list(Deltas)
            )
    ).

port_bytes(Port, Key) ->
    case catch erlang:port_info(Port, Key) of
        {Key, V} when is_integer(V) -> V;
        _ -> 0
    end.

proc_count(Key, N) ->
    case catch recon:proc_count(Key, N) of
        L when is_list(L) ->
            [{P, V} || {P, V, _Info} <- L];
        _ ->
            Pairs = [
                {P, V}
             || P <- erlang:processes(),
                {_InfoKey, V} <- [catch process_info(P, Key)],
                is_integer(V)
            ],
            top_pairs(Pairs, N)
    end.

proc_count_full(Key, N) ->
    case catch recon:proc_count(Key, N) of
        L when is_list(L) -> L;
        _ -> [{P, V, []} || {P, V} <- proc_count(Key, N)]
    end.

proc_brief(P, ChannelClientIds) ->
    Keys = [
        registered_name,
        current_function,
        initial_call,
        status,
        message_queue_len,
        memory,
        total_heap_size,
        heap_size,
        stack_size,
        reductions,
        garbage_collection
    ],
    #{
        pid => P,
        clientid => maps:get(P, ChannelClientIds, undefined),
        translated_initial_call => catch proc_lib:translate_initial_call(P),
        info => maps:from_list([pinfo(P, K) || K <- Keys]),
        binary => bin_brief(P),
        dict => dict_brief(P),
        stack => stack_brief(P)
    }.

add_proc_briefs(Pids, ChannelClientIds, Acc) ->
    lists:foldl(
        fun(P, A) ->
            case maps:is_key(P, A) of
                true -> A;
                false -> A#{P => proc_brief(P, ChannelClientIds)}
            end
        end,
        Acc,
        Pids
    ).

channel_client_ids(Pids) ->
    MatchSpec = [{{'$1', P}, [], [{{P, '$1'}}]} || P <- lists:usort(Pids)],
    case MatchSpec of
        [] ->
            #{};
        _ ->
            case catch ets:select(emqx_channel, MatchSpec) of
                Pairs when is_list(Pairs) -> maps:from_list(Pairs);
                _ -> #{}
            end
    end.

pinfo(P, K) ->
    case catch process_info(P, K) of
        {K, V} -> {K, V};
        undefined -> {K, undefined};
        {'EXIT', R} -> {K, {error, R}};
        Other -> {K, Other}
    end.

bin_brief(P) ->
    case catch process_info(P, binary) of
        {binary, Bs} ->
            Sizes = [Sz || {_Ptr, Sz, _Refs} <- Bs],
            #{
                count => length(Sizes),
                bytes => lists:sum(Sizes),
                top10 => lists:sublist(lists:reverse(lists:sort(Sizes)), 10)
            };
        Other ->
            #{error => Other}
    end.

dict_brief(P) ->
    case catch process_info(P, dictionary) of
        {dictionary, D} ->
            Keys = [
                '$initial_call',
                '$ancestors',
                logger_metadata,
                clientid,
                client_id,
                username,
                peername,
                sockname,
                conninfo,
                clientinfo,
                channel,
                shard
            ],
            [{K, V} || {K, V} <- D, lists:member(K, Keys)];
        _ ->
            []
    end.

stack_brief(P) ->
    case catch process_info(P, current_stacktrace) of
        {current_stacktrace, S} -> S;
        Other -> Other
    end.

top_pairs(Pairs, N) ->
    lists:sublist(lists:reverse(lists:keysort(2, Pairs)), N).

set_local_group_leader() ->
    case whereis(user) of
        Pid when is_pid(Pid) ->
            _ = catch group_leader(Pid, self()),
            ok;
        _ ->
            ok
    end.

append_log(Cfg, Format, Args) ->
    file:write_file(maps:get(log_file, Cfg), io_lib:format(Format, Args), [append]).

now_ms() ->
    erlang:system_time(millisecond).

get(K, M) ->
    maps:get(K, M, 0).
