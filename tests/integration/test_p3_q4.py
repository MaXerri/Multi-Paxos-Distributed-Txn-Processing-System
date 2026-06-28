"""Integration tests for p3_tests_q4.

Scenario: 9 nodes across 3 Paxos clusters (3 nodes each), single set.
  Cluster 2 (n4–n6) owns items 3001–6000 → all accounts in this test

Transactions (in order, non-deterministic commit order):
  1. (3001 → 3002, 1)
  2. (3004 → 3005, 1)
  3. (3004 → 3006, 9)
  4. (3001 → 3003, 10)

All transfers are intra-shard and stay within accounts 3001–3006.
Initial balances are all 10, so the subtotal is always 60 regardless of
which transactions commit or abort.

Run from the project root:
    pip install -r requirements.txt
    pytest tests/integration/
"""

import pexpect
import pytest
import pytest_check as check

from conftest import run_scenario, expect_output, read_balance, PROMPT_PATTERN


def test_p3_q4_subtotal():
    """Verify subtotal conservation and per-account outcomes for accounts 3001–3006.

    Scenario  : single set, 9 nodes, 3 clusters (3 nodes each).
    Initial   : all accounts start at balance 10 → subtotal = 60.
    Invariant : all transfers stay within the 3001–3006 group, so the subtotal
                must be 60 on nodes 4, 5, and 6 regardless of commit order.

    Outcome checks for the two txns touching 3001 (non-deterministic):
      3001=0  → both txns committed: 3002=10, 3003=20
      3001=9  → only (3001→3002,1) committed: 3002=11, 3003=10
      3001=10 → neither committed: error

    Outcome checks for the two txns touching 3004 (non-deterministic):
      3004=0  → both txns committed: 3005=11, 3006=19
      3004=9  → only (3004→3005,1) committed: 3005=11, 3006=10
      3004=1  → only (3004→3006,9) committed: 3005=10, 3006=19
      3004=10 → neither committed: error
    """
    csv_path = "integration_test_input/p3_tests_q4.csv"
    child = run_scenario(csv_path, num_nodes_per_cluster=3, num_clusters=3)

    try:
        expect_output(child, PROMPT_PATTERN, timeout=60)

        n4_total = 0
        n5_total = 0
        n6_total = 0
        balances = {}

        for account in [3001, 3002, 3003, 3004, 3005, 3006]:
            child.sendline(f"PrintBalance {account}")
            b4 = read_balance(child, node_id=4)
            b5 = read_balance(child, node_id=5)
            b6 = read_balance(child, node_id=6)
            balances[account] = (b4, b5, b6)
            n4_total += b4
            n5_total += b5
            n6_total += b6
            expect_output(child, PROMPT_PATTERN, timeout=10)

        check.equal(n4_total, 60, msg="Node 4 subtotal for accounts 3001–3006")
        check.equal(n5_total, 60, msg="Node 5 subtotal for accounts 3001–3006")
        check.equal(n6_total, 60, msg="Node 6 subtotal for accounts 3001–3006")

        b3001 = balances[3001][0]  # node 4 is representative
        b3002_n4, b3002_n5, b3002_n6 = balances[3002]
        b3003_n4, b3003_n5, b3003_n6 = balances[3003]

        if b3001 == 0:
            # Both (3001→3002) and (3001→3003) committed
            check.equal(b3002_n4, 10, msg="Node 4: 3002 when 3001=0")
            check.equal(b3002_n5, 10, msg="Node 5: 3002 when 3001=0")
            check.equal(b3002_n6, 10, msg="Node 6: 3002 when 3001=0")
            check.equal(b3003_n4, 20, msg="Node 4: 3003 when 3001=0")
            check.equal(b3003_n5, 20, msg="Node 5: 3003 when 3001=0")
            check.equal(b3003_n6, 20, msg="Node 6: 3003 when 3001=0")
        elif b3001 == 9:
            # Exactly one of the two txns committed (XOR)
            check.is_true(
                (b3002_n4 == 11) and (b3003_n4 == 10),
                msg=f"If 3001 = 9, then 3002 must be 11 and 3003 transaction is committed but fails with insufficient balance",
            )
        elif b3001 == 10:
            pytest.fail("3001 balance is 10 — neither transaction committed, which is an error")
        else:
            check.fail(f"Unexpected 3001 balance: {b3001}")

        b3004 = balances[3004][0]  # node 4 is representative
        b3005_n4, b3005_n5, b3005_n6 = balances[3005]
        b3006_n4, b3006_n5, b3006_n6 = balances[3006]

        if b3004 == 0:
            # Both (3004→3005, 1) and (3004→3006, 9) committed
            check.equal(b3005_n4, 11, msg="Node 4: 3005 when 3004=0")
            check.equal(b3005_n5, 11, msg="Node 5: 3005 when 3004=0")
            check.equal(b3005_n6, 11, msg="Node 6: 3005 when 3004=0")
            check.equal(b3006_n4, 19, msg="Node 4: 3006 when 3004=0")
            check.equal(b3006_n5, 19, msg="Node 5: 3006 when 3004=0")
            check.equal(b3006_n6, 19, msg="Node 6: 3006 when 3004=0")
        elif b3004 == 9:
            # Only (3004→3005, 1) committed; (3004→3006, 9) aborted
            check.equal(b3005_n4, 11, msg="Node 4: 3005 when 3004=9")
            check.equal(b3005_n5, 11, msg="Node 5: 3005 when 3004=9")
            check.equal(b3005_n6, 11, msg="Node 6: 3005 when 3004=9")
            check.equal(b3006_n4, 10, msg="Node 4: 3006 when 3004=9")
            check.equal(b3006_n5, 10, msg="Node 5: 3006 when 3004=9")
            check.equal(b3006_n6, 10, msg="Node 6: 3006 when 3004=9")
        elif b3004 == 1:
            # Only (3004→3006, 9) committed; (3004→3005, 1) aborted
            check.equal(b3005_n4, 10, msg="Node 4: 3005 when 3004=1")
            check.equal(b3005_n5, 10, msg="Node 5: 3005 when 3004=1")
            check.equal(b3005_n6, 10, msg="Node 6: 3005 when 3004=1")
            check.equal(b3006_n4, 19, msg="Node 4: 3006 when 3004=1")
            check.equal(b3006_n5, 19, msg="Node 5: 3006 when 3004=1")
            check.equal(b3006_n6, 19, msg="Node 6: 3006 when 3004=1")
        elif b3004 == 10:
            pytest.fail("3004 balance is 10 — neither transaction committed, which is an error")
        else:
            check.fail(f"Unexpected 3004 balance: {b3004}")

        child.sendline("Continue")
        child.expect(pexpect.EOF, timeout=15)
    finally:
        if child.isalive():
            child.close(force=True)
