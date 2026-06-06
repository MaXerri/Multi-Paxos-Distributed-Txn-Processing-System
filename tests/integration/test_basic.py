"""Basic integration tests for the paxos_node executable.

Each test launches the real binary, drives it through its terminal interface
via pexpect, and validates stdout output — no C++ internals are touched
directly.

Run from the project root:
    pip install -r requirements.txt
    pytest tests/integration/
"""

import os
import pexpect
import pytest

from conftest import run_scenario, expect_output, check_balance, PROMPT_PATTERN


def test_simple_transfer_balance():
    """Transfer 1 unit from account 1 to account 2 and verify account 2 reaches balance 11.

    Scenario  : single set, 3 nodes (1 cluster), transaction (1 → 2, amount=1).
    Initial   : all accounts start at balance 10 (paxos_node default).
    Expected  : account 2 balance = 10 + 1 = 11 on every node in its cluster.
    """
    csv_path = "integration_test_input/simple_transfer.csv"
    child = run_scenario(csv_path, num_nodes_per_cluster=3, num_clusters=1)

    try:
        # Hard wait — if the prompt never appears the process crashed
        expect_output(child, PROMPT_PATTERN, timeout=60)

        child.sendline("PrintBalance 2")
        check_balance(child, item_id=2, expected=11)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("PrintBalance 1")
        check_balance(child, item_id=1, expected=9)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("PrintBalance 3")
        check_balance(child, item_id=3, expected=10)

        expect_output(child, PROMPT_PATTERN, timeout=10)
        child.sendline("Continue")

        child.expect(pexpect.EOF, timeout=15)
    finally:
        if child.isalive():
            child.close(force=True)
