"""Integration tests for p3_tests_q3_2.

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
  8. F(n7)             — forces a view change; n9 catches up on the missed (6005,6006) txn
  9. (6011 → 6012, 1)  — intra-shard via n8+n9: 6011=9, 6012=11
  10. (6013 → 6014, 1) — intra-shard via n8+n9: 6013=9, 6014=11

"""

import pexpect
import pytest

from conftest import run_scenario, expect_output, check_balance, read_balance, PROMPT_PATTERN


def test_p3_tests_q3_2_balances():
    """Verify balances for transactions after F(n7) triggers a view change.

    Scenario  : single set, 9 nodes, 3 clusters (3 nodes each).
    Initial   : all accounts start at balance 10.
    Expected  : 6011=9, 6012=11, 6013=9, 6014=11 on n8 and n9 (n7 is dead).
                The view change caused by F(n7) forces n9 to catch up on the
                missed (6005 → 6006) transaction before these txns are processed.
    """
    csv_path = "integration_test_input/p3_tests_q3_2.csv"
    child = run_scenario(csv_path, num_nodes_per_cluster=3, num_clusters=3)

    try:
        expect_output(child, PROMPT_PATTERN, timeout=60)

        # n7 is dead after F(n7); only check n8 and n9
        child.sendline("PrintBalance 6011")
        check_balance(child, node_id=8, expected=9)
        check_balance(child, node_id=9, expected=9)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("PrintBalance 6012")
        check_balance(child, node_id=8, expected=11)
        check_balance(child, node_id=9, expected=11)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("PrintBalance 6013")
        check_balance(child, node_id=8, expected=9)
        check_balance(child, node_id=9, expected=9)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("PrintBalance 6014")
        check_balance(child, node_id=8, expected=11)
        check_balance(child, node_id=9, expected=11)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("Continue")
        child.expect(pexpect.EOF, timeout=15)
    finally:
        if child.isalive():
            child.close(force=True)
