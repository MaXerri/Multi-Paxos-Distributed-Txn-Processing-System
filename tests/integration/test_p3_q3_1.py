"""Integration tests for p3_tests_q3_1.

Scenario: 9 nodes across 3 Paxos clusters (3 nodes each), single set.
  Cluster 3 (n7–n9) owns items 6001–9000 → all accounts in this test

Transactions and failures (in order):
  1. (6001 → 6002, 1)  — intra-shard: 6001=9, 6002=11
  2. (6003 → 6004, 1)  — intra-shard: 6003=9, 6004=11
  3. F(n9)             — cluster 3 loses n9, quorum intact (n7+n8)
  4. (6005 → 6006, 1)  — intra-shard with 2/3 quorum: 6005=9, 6006=11
  5. R(n9)             — n9 recovers, but is not caught up until there is another view change (here there is none so it doesnt catch up)
  6. (6007 → 6008, 1)  — intra-shard: 6007=9, 6008=11
  7. (6009 → 6010, 1)  — intra-shard: 6009=9, 6010=11

"""

import pexpect
import pytest

from conftest import run_scenario, expect_output, check_balance, read_balance, PROMPT_PATTERN


def test_p3_tests_q3_1_balances():
    """Verify final balances after a node failure and recovery within a single cluster.

    Scenario  : single set, 9 nodes, 3 clusters (3 nodes each).
    Initial   : all accounts start at balance 10.
    Expected  : senders (6001,6003,6005,6007,6009)=9; receivers (6002,6004,6006,6008,6010)=11.
                n9 misses txn 4 but recovers; all three nodes must be fully synced.
    """
    csv_path = "integration_test_input/p3_tests_q3_1.csv"
    child = run_scenario(csv_path, num_nodes_per_cluster=3, num_clusters=3)

    try:
        expect_output(child, PROMPT_PATTERN, timeout=60)

        # Pre-failure transactions: all three nodes should agree
        child.sendline("PrintBalance 6001")
        check_balance(child, node_id=7, expected=9)
        check_balance(child, node_id=8, expected=9)
        check_balance(child, node_id=9, expected=9)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("PrintBalance 6002")
        check_balance(child, node_id=7, expected=11)
        check_balance(child, node_id=8, expected=11)
        check_balance(child, node_id=9, expected=11)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        # Transaction during n9 failure: n7+n8 committed; n9 must have caught up after R(n9)
        child.sendline("PrintBalance 6005")
        check_balance(child, node_id=7, expected=9)
        check_balance(child, node_id=8, expected=9)
        check_balance(child, node_id=9, expected=10)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("PrintBalance 6006")
        check_balance(child, node_id=7, expected=11)
        check_balance(child, node_id=8, expected=11)
        check_balance(child, node_id=9, expected=10)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        # Post-recovery transactions: all three nodes alive, but since there has not been a 
        # view change, 9 is 1 sequence number behind and doesnt get to commit its transactions
        # For this reason, node 9 here does not record teh txns 

        child.sendline("PrintBalance 6009")
        check_balance(child, node_id=7, expected=9)
        check_balance(child, node_id=8, expected=9)
        check_balance(child, node_id=9, expected=10)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("PrintBalance 6010")
        check_balance(child, node_id=7, expected=11)
        check_balance(child, node_id=8, expected=11)
        check_balance(child, node_id=9, expected=10)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("Continue")
        child.expect(pexpect.EOF, timeout=15)
    finally:
        if child.isalive():
            child.close(force=True)
