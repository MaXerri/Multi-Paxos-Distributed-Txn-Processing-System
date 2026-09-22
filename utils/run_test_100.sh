#!/bin/bash

# All paths below (tests/integration, build/, output_res.txt, failed_logs/) are
# relative to the repo root, so run from there regardless of the caller's cwd.
cd "$(dirname "$0")/.." || exit 1

# --- single-instance guard ---------------------------------------------------
# Two concurrent run_test_100.sh loops sabotage each other: each calls
# `pkill -9 -f paxos_node` before every test, so one loop kills the OTHER loop's
# nodes mid-run and produces bogus "Running in normal mode" stalls. Refuse to
# start if another copy is already running. (Stale locks from a kill -9'd run
# self-heal: the next start sees the dead pid and takes over.)
LOCK="/tmp/run_test_100.lock"
if [ -f "$LOCK" ] && kill -0 "$(cat "$LOCK" 2>/dev/null)" 2>/dev/null; then
    echo "run_test_100.sh is already running (pid $(cat "$LOCK")). Exiting to avoid"
    echo "port/pkill collisions. If you're sure none is running: rm $LOCK"
    exit 1
fi
echo $$ > "$LOCK"
trap 'rm -f "$LOCK"' EXIT INT TERM

# Kill any leftover loops/nodes from a previous (possibly -9'd) run before we start.
pkill -9 -f 'pytest tests/integration' 2>/dev/null
pkill -9 -f paxos_node 2>/dev/null
sleep 1.5
# -----------------------------------------------------------------------------

OUTPUT="output_res.txt"
> "$OUTPUT"

# Per-run logs for FAILING tests only are kept here; passing runs are discarded
# so the directory doesn't fill up with hundreds of successful runs.
FAIL_DIR="failed_logs"
mkdir -p "$FAIL_DIR"

# Force-kill any leftover node processes and let their ports free up before the
# next set of 9 nodes binds. Skipping this causes bogus multi-hundred-second
# "stalls" from port collisions with zombies from the prior run.
cleanup() {
    # Ask every node to dump its in-memory trace ring into build/node_<id>.log before we
    # kill it. A crash dumps the ring on its own (fatal-signal / terminate handlers), but a
    # stall crashes nothing, so without this SIGKILL destroys the buffers and a hung run
    # leaves zero evidence -- which is exactly what happened to runs 28, 50 and 423.
    # Only meaningful for a PAXOS_TRACE_RING build; harmless otherwise.
    pkill -USR1 -f paxos_node 2>/dev/null
    sleep 0.4
    pkill -9 -f paxos_node 2>/dev/null
    sleep 1.3
}

# run_test <run_index> <test_file>::<test_name>
# Captures the full output of one pytest invocation to a temp log (PAXOS_LOGFILE
# mirrors the node/client stdout that conftest.py exposes). If the test fails,
# the log is moved into $FAIL_DIR; on success it is deleted.
run_test() {
    local i="$1"
    local target="$2"
    # Sanitize the target into a filename-safe slug (test_p3_q8.py::test_... -> test_p3_q8_test_...).
    local slug
    slug=$(echo "$target" | tr '/:.' '___')
    local log="$FAIL_DIR/run${i}_${slug}.log"           # pytest output
    local parent_log="$FAIL_DIR/run${i}_${slug}.parent.log"  # coordinator/client stdout
    local nodes_log="$FAIL_DIR/run${i}_${slug}.nodes.log"    # all node traces combined

    cleanup
    # PAXOS_LOGFILE (opened "w" by conftest.py) mirrors the coordinator process
    # stdout into $parent_log; pytest's own output goes to $log. Kept separate so
    # the truncating open in conftest doesn't clobber pytest's appended output.
    #
    # Each forked node process redirects its stdout+stderr to build/node_<id>.log
    # (see node_launch.cpp run_node()). Those are the actual per-node traces and
    # are NOT in the parent stdout. They are truncated at the start of every run,
    # so we must snapshot them immediately after this run finishes.
    PAXOS_LOGFILE="$parent_log" pytest "tests/integration/$target" -v 2>&1 | tee -a "$OUTPUT" | tee "$log" > /dev/null
    local rc=${PIPESTATUS[0]}

    if [ "$rc" -ne 0 ]; then
        # Ask the nodes to dump their in-memory trace rings BEFORE we snapshot the logs.
        #
        # pytest only kills the harness; its nine forked children are orphaned and still
        # running, still holding their rings. A crash dumps its own ring (fatal-signal /
        # terminate handlers) but a stall crashes nothing, so without this the buffers are
        # simply lost -- which is why runs 28, 50 and 423 left zero evidence.
        # No-op on a non-ring build.
        pkill -USR1 -f 'build/paxos_node' 2>/dev/null
        sleep 0.6

        # Combine every node's trace into one file, with a header per node.
        : > "$nodes_log"
        for nl in build/node_*.log; do
            [ -e "$nl" ] || continue
            {
                echo "==================== ${nl##*/} ===================="
                cat "$nl"
                echo
            } >> "$nodes_log"
        done
        echo ">>> FAILED (rc=$rc): $target  -> saved $log, $parent_log, $nodes_log" | tee -a "$OUTPUT"

        # Stop here and leave the hung cluster alive to be interrogated.
        #
        # pytest only kills the harness process; its nine forked node children are orphaned
        # and keep running, still holding all their state. Normally the next iteration's
        # cleanup() pkills them, which is why every stall so far has left zero evidence.
        # Stopping instead gives a live system: kill -USR1 <pid> dumps that node's trace ring,
        # lldb -p <pid> can inspect remaining_transactions / inflight_, and GetNodeInfo still
        # answers over gRPC.
        # Default: keep going and collect every failure. Run with STOP_ON_FAIL=1 to halt
        # instead and leave the hung cluster alive for lldb / GetNodeInfo / sample.
        if [ -n "$STOP_ON_FAIL" ]; then
            echo ""                                             | tee -a "$OUTPUT"
            echo ">>> STOP_ON_FAIL: leaving nodes alive for inspection." | tee -a "$OUTPUT"
            pgrep -fl 'build/paxos_node' 2>/dev/null            | tee -a "$OUTPUT"
            echo ">>> dump a ring again:  kill -USR1 <pid>   (then read build/node_<id>.log)" | tee -a "$OUTPUT"
            echo ">>> when finished:      pkill -9 -f paxos_node" | tee -a "$OUTPUT"
            rm -f "$LOCK"
            exit 1
        fi
    else
        rm -f "$log" "$parent_log"
    fi
}

for i in $(seq 1 1000); do
    echo "=== RUN $i ===" | tee -a "$OUTPUT"
    run_test "$i" "test_mod_demo3_q2.py::test_mod_demo3_q2_balances"
    run_test "$i" "test_p3_q8.py::test_p3_tests_q8_balances"
    run_test "$i" "test_p3_q9.py::test_p3_tests_q9_balances"
    run_test "$i" "test_p3_q3_2.py::test_p3_tests_q3_2_balances"
    echo "" | tee -a "$OUTPUT"
done
