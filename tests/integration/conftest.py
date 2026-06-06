import os
import pexpect
import pytest
import pytest_check as check

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TESTS_INTEGRATION_DIR = os.path.abspath(os.path.dirname(__file__))
EXECUTABLE = os.path.join(PROJECT_ROOT, "build", "paxos_node")

# Substring that appears in every interactive command prompt
PROMPT_PATTERN = "Enter command to get up to and including set"


def run_scenario(
    csv_path,
    num_nodes_per_cluster=3,
    num_clusters=1,
    timeout=120,
):
    """Launch ./paxos_node <csv> <args> and return a configured pexpect.spawn.

    The returned child is already running; callers should call
    child.expect(PROMPT_PATTERN) to wait for the first interactive prompt
    before sending commands.

    Args:
        csv_path: Filename relative to tests/test_data/ (e.g.
                  "integration_test_input/simple_transfer.csv").
                  The executable runs from build/ and hardcodes the prefix
                  "../tests/test_data/", so the file must exist at
                  <project_root>/tests/test_data/<csv_path>.
        num_nodes_per_cluster: Nodes per Paxos cluster.
        num_clusters: Number of shard clusters (determines which node handles
                      each item range: cluster k owns items
                      [(k-1)*floor(9000/num_clusters)+1 .. k*floor(9000/num_clusters)]).
        timeout: Default pexpect timeout in seconds for every subsequent
                 expect() call.

    Returns:
        pexpect.spawn instance running paxos_node.
    """
    child = pexpect.spawn(
        EXECUTABLE,
        args=[
            csv_path,
            str(num_nodes_per_cluster),
            str(num_clusters),
            "false",   # benchmark_mode
            "0",       # total_txns (unused outside benchmark mode)
            "0.0",     # read_pct
            "0.0",     # cross_shard_pct
            "0.0",     # skewness
        ],
        encoding="utf-8",
        timeout=timeout,
        cwd=os.path.join(PROJECT_ROOT, "build"),
    )
    return child


def expect_output(child, pattern, timeout=10):
    """Hard-assert a pattern. Fails immediately on mismatch or crash.

    Use for structural waits (prompt, EOF) where there is no point
    continuing if the pattern is missing.
    """
    index = child.expect([pattern, pexpect.EOF, pexpect.TIMEOUT], timeout=timeout)
    if index == 1:
        pytest.fail("System crash — process exited unexpectedly")
    if index == 2:
        if not child.isalive():
            pytest.fail("System crash — process died")
        raise AssertionError(
            f"Expected: {pattern!r}\n"
            f"Got:      {child.before.strip()}"
        )


def check_balance(child, item_id, expected, timeout=10):
    """Soft-assert the balance of an account on every node.

    Waits for the first 'Balance for item <item_id>: <value>' line and
    compares the numeric value to expected. All check_balance calls in a
    test run even if one fails.

    Args:
        child:    pexpect.spawn instance.
        item_id:  integer account / item id passed to PrintBalance.
        expected: expected integer balance, or a list of acceptable values
                  when more than one outcome is correct (e.g. [10, 11]).
        timeout:  seconds to wait for output.
    """
    pattern = rf"Balance for item {item_id}:\s*(\d+)"
    index = child.expect([pattern, pexpect.EOF, pexpect.TIMEOUT], timeout=timeout)
    if index == 1:
        pytest.fail(f"System crash — process exited while checking balance of item {item_id}")
    if index == 2:
        if not child.isalive():
            pytest.fail(f"System crash — process died while checking balance of item {item_id}")
        check.fail(f"Balance for item {item_id}: no output received within {timeout}s")
        return
    actual = int(child.match.group(1))
    if isinstance(expected, list):
        check.is_in(actual, expected, msg=f"Balance for item {item_id}")
    else:
        check.equal(actual, expected, msg=f"Balance for item {item_id}")
