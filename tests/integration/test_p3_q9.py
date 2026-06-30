"""Integration tests for p3_tests_q9.

Scenario: 9 nodes across 3 Paxos clusters (3 nodes each), single set.
  Cluster 1 (n1–n3): accounts    1–3000   — leader n1
  Cluster 2 (n4–n6): accounts 3001–6000   — leader n4
  Cluster 3 (n7–n9): accounts 6001–9000   — leader n7 (no transactions touch C3)

All nodes start alive. n2 fails twice. Because n1 remains leader throughout
(no view change ever triggers in C1), n2 never catches up past the state it
had at its FIRST failure — verifying the "catch-up only on leader change" rule.

Transactions and failures (in order):
  T1.  (1001, 4001, 1)  — cross C1+C2:  1001-=1, 4001+=1
  T2.  (1002, 1003, 1)  — intra-C1:    1002-=1, 1003+=1
  T3.  (1004, 1005, 1)  — intra-C1:    1004-=1, 1005+=1
       F(n2)             — C1 quorum = n1+n3; n2 misses T4–T6
  T4.  (4001, 1002, 1)  — cross C2+C1: 4001-=1, 1002+=1   ← n2 MISSES
  T5.  (1003, 1004, 1)  — intra-C1:    1003-=1, 1004+=1   ← n2 MISSES
  T6.  (1001, 1005, 1)  — intra-C1:    1001-=1, 1005+=1   ← n2 MISSES
       R(n2)             — n2 recovers; no leader change → does NOT catch up
  T7.  (1003, 4001, 1)  — cross C1+C2: 1003-=1, 4001+=1   ← n2 alive but stuck
  T8.  (1005, 1002, 1)  — intra-C1:    1005-=1, 1002+=1   ← n2 alive but stuck
       F(n2)             — n2 dies again; C1 quorum = n1+n3
  T9.  (1004, 4001, 1)  — cross C1+C2: 1004-=1, 4001+=1   ← n2 MISSES
  T10. (1005, 1001, 1)  — intra-C1:    1005-=1, 1001+=1   ← n2 MISSES

Expected final balances on n1 and n3 (all transactions commit; no conflicts):
  1001: 10 -1(T1) -1(T6) +1(T10) =  9
  1002: 10 -1(T2) +1(T4) +1(T8)  = 11
  1003: 10 +1(T2) -1(T5) -1(T7)  =  9
  1004: 10 -1(T3) +1(T5) -1(T9)  =  9
  1005: 10 +1(T3) +1(T6) -1(T8) -1(T10) = 10
  4001: 10 +1(T1) -1(T4) +1(T7) +1(T9)  = 12

Expected final state on n2 (stale — last executed = C1 seq 3, only T1–T3 applied):
  1001:  9   (coincides with correct value)
  1002:  9   (diverges: correct is 11)
  1003: 11   (diverges: correct is 9)
  1004:  9   (coincides with correct value)
  1005: 11   (diverges: correct is 10)
"""

import pexpect

from conftest import run_scenario, expect_output, check_balance, PROMPT_PATTERN


def test_p3_tests_q9_balances():
    csv_path = "integration_test_input/p3_tests_q9.csv"
    child = run_scenario(csv_path, num_nodes_per_cluster=3, num_clusters=3)

    try:
        expect_output(child, PROMPT_PATTERN, timeout=60)

        # ── Account 1001 (C1) ────────────────────────────────────────────────
        # n1, n3: 9  (T1, T6, T10)
        # n2: 9  (stale — T1 only; value coincides with correct result)
        child.sendline("PrintBalance 1001")
        check_balance(child, node_id=1, expected=9)
        check_balance(child, node_id=2, expected=9)
        check_balance(child, node_id=3, expected=9)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Account 1002 (C1) ────────────────────────────────────────────────
        # n1, n3: 11  (T2, T4, T8)
        # n2: 9   (stale — T2 only; diverges from correct value)
        child.sendline("PrintBalance 1002")
        check_balance(child, node_id=1, expected=11)
        check_balance(child, node_id=2, expected=9)
        check_balance(child, node_id=3, expected=11)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Account 1003 (C1) ────────────────────────────────────────────────
        # n1, n3: 9   (T2, T5, T7)
        # n2: 11  (stale — T2 only; diverges from correct value)
        child.sendline("PrintBalance 1003")
        check_balance(child, node_id=1, expected=9)
        check_balance(child, node_id=2, expected=11)
        check_balance(child, node_id=3, expected=9)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Account 1004 (C1) ────────────────────────────────────────────────
        # n1, n3: 9   (T3, T5, T9)
        # n2: 9   (stale — T3 only; value coincides with correct result)
        child.sendline("PrintBalance 1004")
        check_balance(child, node_id=1, expected=9)
        check_balance(child, node_id=2, expected=9)
        check_balance(child, node_id=3, expected=9)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Account 1005 (C1) ────────────────────────────────────────────────
        # n1, n3: 10  (T3, T6, T8, T10)
        # n2: 11  (stale — T3 only; diverges from correct value)
        child.sendline("PrintBalance 1005")
        check_balance(child, node_id=1, expected=10)
        check_balance(child, node_id=2, expected=11)
        check_balance(child, node_id=3, expected=10)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        # ── Account 4001 (C2) ────────────────────────────────────────────────
        # n4, n5, n6: 12  (T1, T4, T7, T9)
        child.sendline("PrintBalance 4001")
        check_balance(child, node_id=4, expected=12)
        check_balance(child, node_id=5, expected=12)
        check_balance(child, node_id=6, expected=12)
        expect_output(child, PROMPT_PATTERN, timeout=10)

        child.sendline("Continue")
        child.expect(pexpect.EOF, timeout=15)
    finally:
        if child.isalive():
            child.close(force=True)
