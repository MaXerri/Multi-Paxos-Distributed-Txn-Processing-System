"""Integration tests for p3_tests_q6.

SET 1
-----
Scenario: 9 nodes across 3 Paxos clusters (3 nodes each), all nodes live at start.
  Cluster 1 (n1–n3) owns items    1–3000
  Cluster 2 (n4–n6) owns items 3001–6000
  Cluster 3 (n7–n9) owns items 6001–9000

Transactions and failures (in order):
  1.  (1, 3001, 1)    — cross-shard (C1+C2): 1=9, 3001=11
  2.  (2, 3, 1)       — intra-shard C1: 2=9, 3=11
  3.  (4, 5, 1)       — intra-shard C1: 4=9, 5=11
  4.  (6, 6001, 1)    — cross-shard (C1+C3): 6=9, 6001=11
  5.  F(n3)           — C1 loses n3; quorum intact (n1+n2)
  6.  (3002, 3003, 1) — intra-shard C2: 3002=9, 3003=11
  7.  (3004, 3005, 1) — intra-shard C2: 3004=9, 3005=11
  8.  (3006, 7, 1)    — cross-shard (C2+C1): 3006=9, 7=11 (n3 misses this)
  9.  (3007, 6002, 1) — cross-shard (C2+C3): 3007=9, 6002=11
  10. F(n6)           — C2 loses n6; quorum intact (n4+n5)
  11. (6003, 6004, 1) — intra-shard C3: 6003=9, 6004=11
  12. (6005, 6006, 1) — intra-shard C3: 6005=9, 6006=11
  13. (6007, 8, 1)    — cross-shard (C3+C1): 6007=9, 8=11
  14. F(n9)           — C3 loses n9; quorum intact (n7+n8)
  15. (6008, 3008, 1) — cross-shard (C3+C2): 6008=9, 3008=11 (n9 misses this)

Queries: (6), (7), (6008)

SET 2
-----
Scenario: 9 nodes across 3 Paxos clusters (3 nodes each).
  Live nodes at start: [n1, n2, n3, n4, n5, n7, n8] — n6 and n9 already dead.
  C2 starts with only n4+n5 (quorum = 2); C3 starts with only n7+n8 (quorum = 2).

Transactions and failures (in order):
  1.  (1, 3001, 1)     — cross-shard (C1+C2): commits → 1=9, 3001=11
  2.  (2, 3001, 1)     — cross-shard (C1+C2): commits → 2=9, 3001=12
  3.  F(n4)            — C2 loses n4; only n5 remains (no quorum)
  4.  (3, 3003, 1)     — cross-shard (C1+C2): ABORTS (C2 no quorum) → 3=10, 3003=10
  5.  (4, 6001, 1)     — cross-shard (C1+C3): commits → 4=9, 6001=11
  6.  F(n7)            — C3 loses n7; only n8 remains (no quorum)
  7.  F(n1)            — C1 loses n1; n2+n3 quorum intact
  8.  (5, 6002, 1)     — cross-shard (C1+C3): ABORTS (C3 no quorum) → 5=10, 6002=10
  9.  (3004, 6003, 1)  — cross-shard (C2+C3): ABORTS (both no quorum) → 3004=10, 6003=10

Dead nodes at end: n6, n9 (initial), n4, n7, n1.
Live nodes at end:  n2, n3 (C1) | n5 (C2) | n8 (C3)
"""

import pexpect
import time
from conftest import run_scenario, expect_output, check_balance, PROMPT_PATTERN, read_balance


def test_p3_q6_set1_balances():
    """Verify balances for accounts 6, 7, and 6008 across three cascading node failures.

    Account 6  : txn committed before F(n3) → n1=9, n2=9 (n3 dead, skip)
    Account 7  : txn committed after  F(n3) → n1=11, n2=11 (n3 dead, skip)
    Account 6008: txn committed after F(n9) → n7=9, n8=9 (n9 dead, skip)

    # --------------------------------------
    Verify balances when two clusters start with only quorum-minimum nodes and progressively lose them.

    Set 2 starts with n6 and n9 already dead, so C2 has n4+n5 and C3 has n7+n8.

    Account 3001 (C2, n5): two cross-shard txns commit before F(n4) → n5=12
    Account 6001 (C3, n8): cross-shard txn commits before F(n7)     → n8=11
    Account 3  (C1, n2+n3): txn aborts when C2 has no quorum        → n2=10, n3=10
    Account 5  (C1, n2+n3): txn aborts when C3 has no quorum        → n2=10, n3=10
    Account 6002 (C3, n8): txn aborts when C3 has no quorum         → n8=10



    """
    csv_path = "integration_test_input/p3_tests_q6.csv"
    child = run_scenario(csv_path, num_nodes_per_cluster=3, num_clusters=3)

    try:
        expect_output(child, PROMPT_PATTERN, timeout=60)

        # Account 6: (6→6001, 1) committed before F(n3); n3 dead
        child.sendline("PrintBalance 6")
        check_balance(child, node_id=1, expected=9)
        check_balance(child, node_id=2, expected=9)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        # Account 7: (3006→7, 1) committed after F(n3); n3 missed it and is dead
        child.sendline("PrintBalance 7")
        check_balance(child, node_id=1, expected=11)
        check_balance(child, node_id=2, expected=11)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        # Account 6008: (6008→3008, 1) committed after F(n9); n9 missed it and is dead
        child.sendline("PrintBalance 6008")
        check_balance(child, node_id=7, expected=9)
        check_balance(child, node_id=8, expected=9)

        expect_output(child, PROMPT_PATTERN, timeout=10)

        # Move to the next set
        # child.sendline("Continue")
        child.sendline("Continue")
        child.expect(pexpect.EOF, timeout=15)


        # ------------ start set 2 (q7) ---------------------

        # wait 10 seconds and then type "continue" then ""
        # time.sleep(6)
        # child.sendline("Continue")
        # expect_output(child, "Passed the while loop", timeout=15)
        # time.sleep(0.5)
        # child.sendline("")
        # expect_output(child, PROMPT_PATTERN, timeout=20)



        # child.sendline("Continue")
        # child.expect(pexpect.EOF, timeout=15)


    finally:
        if child.isalive():
            child.close(force=True)

