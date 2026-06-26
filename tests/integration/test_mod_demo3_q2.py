"""Integration tests for mod_demo3_q2.csv.

Scenario: 9 nodes across 3 Paxos clusters (3 nodes each), single set.
  Cluster 1 (n1–n3) owns items 1–3000   → accounts 1001, 1002, 1003, 1004
  Cluster 2 (n4–n6) owns items 3001–6000 → account 4001

Transactions and failures (in order):
  1. (1001 → 4001, 1)  — succeeds or aborts: 1001=9, 4001=11
  2. (1002 → 4001, 1)  — succeeds or aborts: 1002=9, 4001=12
  3. F(n1)             — cluster 1 loses one node, quorum intact (2/3)
  4. F(n4)             — cluster 2 loses one node, quorum intact (2/3)
  5. (1003 → 4001, 1)  — succeeds: 1003=9, 4001=13
Run from the project root:
    pip install -r requirements.txt
    pytest tests/integration/
"""

import pexpect
import pytest

from conftest import run_scenario, expect_output, check_balance, read_balance, PROMPT_PATTERN


def test_mod_demo3_q2_balances():
    """Verify final balances after node failures cause an aborted cross-shard transaction.

    Scenario  : single set, 9 nodes, 3 clusters (3 nodes each).
    Initial   : all accounts start at balance 10.
    Expected  : 1001=9, 1002=9, 1003=9, 1004=10 (aborted), 4001=13.
    """
    csv_path = "integration_test_input/mod_demo3_q2.csv"
    child = run_scenario(csv_path, num_nodes_per_cluster=3, num_clusters=3)

    try:
        expect_output(child, PROMPT_PATTERN, timeout=60)

        # Cluster 1 (nodes 1–3): n1 and n2 fail, so check node 3
        child.sendline("PrintBalance 1001")

        case1 = read_balance(child, node_id=1) == 9
        
        # node 1's line was already consumed by read_balance — start from node 2
        if case1:
            check_balance(child, node_id=2, expected=9)
            check_balance(child, node_id=3, expected=9)

            expect_output(child, PROMPT_PATTERN, timeout=10)

            child.sendline("PrintBalance 1002")
            check_balance(child, node_id=1, expected=10)
            check_balance(child, node_id=2, expected=10)
            check_balance(child, node_id=3, expected=10)

            expect_output(child, PROMPT_PATTERN, timeout=10)

        else:
            check_balance(child, node_id=2, expected=10)
            check_balance(child, node_id=3, expected=10)

            expect_output(child, PROMPT_PATTERN, timeout=10)

            child.sendline("PrintBalance 1002")
            check_balance(child, node_id=1, expected=9)
            check_balance(child, node_id=2, expected=9)
            check_balance(child, node_id=3, expected=9)

            expect_output(child, PROMPT_PATTERN, timeout=10)


        child.sendline("PrintBalance 1003")
        check_balance(child, node_id=1, expected=10)
        check_balance(child, node_id=2, expected=9)
        check_balance(child, node_id=3, expected=9)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        # cluster 2
        child.sendline("PrintBalance 4001")
        check_balance(child, node_id=4, expected=11)
        check_balance(child, node_id=5, expected=12)
        check_balance(child, node_id=6, expected=12)
        # check_balance(child, node_id=7, expected=10)
        
         
            
        expect_output(child, PROMPT_PATTERN, timeout=10)
        child.sendline("Continue")

        child.expect(pexpect.EOF, timeout=15)
    finally:
        if child.isalive():
            child.close(force=True)
