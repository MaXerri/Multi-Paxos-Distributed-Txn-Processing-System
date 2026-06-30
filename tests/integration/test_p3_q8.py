"""Integration tests for p3_tests_q8.

Scenario: 9 nodes across 3 Paxos clusters (3 nodes each), single set.
  Cluster 1 (n1–n3): accounts    1–3000
  Cluster 2 (n4–n6): accounts 3001–6000
  Cluster 3 (n7–n9): accounts 6001–9000

Live nodes at start: [n1, n3, n4, n5, n6, n7, n8, n9]  — n2 starts dead.

Transactions and failures (in order):
  1.  (1, 2, 1)       — intra-C1:      1=9,    2=11
  2.  (3, 3001, 1)    — cross C1+C2:   3=9,    3001=11
  3.  (3001, 3, 1)    — cross C2+C1:   3001-=1, 3+=1  (may abort; see below)
  4.  R(n2)           — n2 recovers and catches up
  5.  F(n1)           — C1 loses n1; n2+n3 form new quorum
  6.  (4, 3002, 1)    — cross C1+C2:   4=9,    3002=11  (n1 dead, misses this)
  7.  (3003, 3004, 1) — intra-C2:      3003=9, 3004=11
  8.  F(n4)           — C2 loses n4; n5+n6 form new quorum
  9.  (3005, 6001, 1) — cross C2+C3:   3005=9, 6001=11 (n4 dead, misses this)
  10. (6002, 6003, 1) — intra-C3:      6002=9, 6003=11

Txns 2 and 3 race on accounts 3 and 3001 via 2PC, so three outcomes are possible:
  a) txn2 commits, txn3 aborts  → 3001=11, 3=9
  b) txn3 commits, txn2 aborts  → 3001=9,  3=11
  c) both commit in order       → 3001=10, 3=10
"""

import pexpect

from conftest import run_scenario, expect_output, check_balance, read_balance, PROMPT_PATTERN


def test_p3_tests_q8_balances():
    csv_path = "integration_test_input/p3_tests_q8.csv"
    child = run_scenario(csv_path, num_nodes_per_cluster=3, num_clusters=3)

    try:
        expect_output(child, PROMPT_PATTERN, timeout=60)

        # ── Account 3001 (C2): outcome of the txn2 / txn3 race ──────────────
        child.sendline("PrintBalance 3001")
        b3001 = read_balance(child, node_id=4)

        if b3001 == 11:
            # txn2 committed, txn3 aborted  →  3001=11, 3=9
            check_balance(child, node_id=5, expected=11)
            check_balance(child, node_id=6, expected=11)
            expect_output(child, PROMPT_PATTERN, timeout=10)

            child.sendline("PrintBalance 3")
            check_balance(child, node_id=1, expected=9)
            check_balance(child, node_id=2, expected=9)
            check_balance(child, node_id=3, expected=9)

        elif b3001 == 9:
            # txn3 committed, txn2 aborted  →  3001=9, 3=11
            check_balance(child, node_id=5, expected=9)
            check_balance(child, node_id=6, expected=9)
            expect_output(child, PROMPT_PATTERN, timeout=10)

            child.sendline("PrintBalance 3")
            check_balance(child, node_id=1, expected=11)
            check_balance(child, node_id=2, expected=11)
            check_balance(child, node_id=3, expected=11)

        elif b3001 == 10: # TODO: This was hitting sometims and was not correct
            # both committed in order  →  3001=10, 3=10
            check_balance(child, node_id=5, expected=10)
            check_balance(child, node_id=6, expected=10)
            expect_output(child, PROMPT_PATTERN, timeout=10)

            child.sendline("PrintBalance 3")
            check_balance(child, node_id=1, expected=10)
            check_balance(child, node_id=2, expected=10)
            check_balance(child, node_id=3, expected=10)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Account 4 (C1): committed after F(n1) view change ───────────────
        # n1 was dead when txn (4→3002,1) committed; its last known value is 10.
        # n2 recovered before F(n1) and saw the transaction; n3 always alive.
        child.sendline("PrintBalance 4")
        check_balance(child, node_id=1, expected=10)
        check_balance(child, node_id=2, expected=9)
        check_balance(child, node_id=3, expected=9)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Account 3002 (C2): committed before F(n4); all C2 nodes saw it ──
        child.sendline("PrintBalance 3002")
        check_balance(child, node_id=4, expected=11) #  TODO: there is a problem with this case
        check_balance(child, node_id=5, expected=11)
        check_balance(child, node_id=6, expected=11)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Accounts 3003 / 3004 (C2 intra-shard): committed before F(n4) ──
        child.sendline("PrintBalance 3003")
        check_balance(child, node_id=4, expected=9)
        check_balance(child, node_id=5, expected=9)
        check_balance(child, node_id=6, expected=9)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("PrintBalance 3004")
        check_balance(child, node_id=4, expected=11)
        check_balance(child, node_id=5, expected=11)
        check_balance(child, node_id=6, expected=11)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Account 3005 (C2): committed after F(n4); n4 missed it ─────────
        child.sendline("PrintBalance 3005")
        check_balance(child, node_id=4, expected=10)  # n4 dead, last value = 10
        check_balance(child, node_id=5, expected=9)
        check_balance(child, node_id=6, expected=9)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Account 6001 (C3): received +1 as participant in txn (3005→6001) ─
        child.sendline("PrintBalance 6001")
        check_balance(child, node_id=7, expected=11)
        check_balance(child, node_id=8, expected=11)
        check_balance(child, node_id=9, expected=11)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Accounts 6002 / 6003 (C3 intra-shard) ───────────────────────────
        child.sendline("PrintBalance 6002")
        check_balance(child, node_id=7, expected=9)
        check_balance(child, node_id=8, expected=9)
        check_balance(child, node_id=9, expected=9)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("PrintBalance 6003")
        check_balance(child, node_id=7, expected=11)
        check_balance(child, node_id=8, expected=11)
        check_balance(child, node_id=9, expected=11)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("Continue")
        child.expect(pexpect.EOF, timeout=15)
    finally:
        if child.isalive():
            child.close(force=True)
