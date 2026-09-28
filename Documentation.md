# Implementation Documentation

This document explains how the system works internally: the Multi-Paxos protocol inside each cluster, the Two-Phase Commit (2PC) layer on top of it for cross-shard transfers, and the system-level design (processes, threads, gRPC, locking). For building, running and the interactive commands, see [README.md](README.md).

---

## 1. Overview

- **Data:** 9000 accounts (items `1..9000`), each starting with a balance of 10.
- **Sharding:** items are split into contiguous ranges, one range per cluster (`MakeShardMap`). With 3 clusters: items 1–3000 are in cluster 1, 3001–6000 in cluster 2, and 6001–9000 in cluster 3.
- **Replication:** each cluster is a replica group of `n` nodes that agree on an ordered log of transactions using Multi-Paxos.
- **Transactions:**
  - **Intra-shard transfer:** sender and receiver are in the same cluster. It goes through one round of Paxos in that cluster.
  - **Cross-shard transfer:** sender and receiver are in different clusters. It uses 2PC, where each 2PC step is itself replicated by Paxos inside its cluster.
  - **Read-only:** the leader answers from its in-memory state, with no consensus round.

### Process model

A single `paxos_node` invocation ([node_launch.cpp](src/node_launch.cpp)) runs the whole system on one machine:

| Process | Role | Port |
|---|---|---|
| Parent | Reads the test CSV, drives the sets, and hosts client `A` (`PaxosClient`) and the interactive prompt | client server on `6001` |
| One forked child per node (`run_node`) | Runs a `PaxosNode`. Its stdout and stderr are redirected to `build/node_<id>.log` | `5000 + node_id` |

Node IDs are global (`1..n·c`). Cluster membership comes from the ID: with `divisor_ = n / num_clusters`, node `i` belongs to cluster `(i-1)/divisor_ + 1`. The cluster's nodes are the range `intra_c_nid_range_`. Every node has a stub to every other node. Paxos messages only go to nodes in the same range, while 2PC messages go to other clusters' leaders.

### Scaling

The design is not tied to a fixed topology. The number of clusters and the number of nodes per cluster are command-line arguments. Everything else is derived from them:
- **Cluster membership** comes from `divisor_`.
- **Quorum size** (`f+1`) comes from the cluster size.
- **Shard ranges** come from `MakeShardMap`.
- **Ports** are `5000 + node_id`.

So the system can run any number of clusters with any number of nodes each. An odd cluster size is best, because an even one adds a node without increasing `f`.

Since everything runs on one machine, I keep deployments to **at most 9 nodes in total**. All benchmark results use **3 clusters of 3 nodes** (`f = 1` per cluster).

### Code map

| File | Contents |
|---|---|
| [src/paxos_node.h](src/paxos_node.h) / [.cpp](src/paxos_node.cpp) | `PaxosNode`: elections, the log, commit and execution, 2PC, the WAL, failure simulation |
| [src/call_data.h](src/call_data.h) / [.cpp](src/call_data.cpp) | Async gRPC plumbing: one server-side `CallData` class per RPC and one client-side call object per outbound RPC |
| [src/paxos_client.h](src/paxos_client.h) / [.cpp](src/paxos_client.cpp) | `PaxosClient`: sending, retrying and broadcasting requests, and receiving replies |
| [src/node_launch.cpp](src/node_launch.cpp) | `main`: forks the nodes, parses the CSV, sets up failures, the prompt, resharding and signal handlers |
| [src/benchmark.h](src/benchmark.h) | Synthetic workload generator for benchmark mode |
| [src/log.h](src/log.h) | `LOG` / `LOGERR` macros and trace modes |
| [grpc/paxos.proto](grpc/paxos.proto) | Messages and services. Regenerate the stubs with `rebuild_proto.sh` |

---

## 2. Multi-Paxos

### 2.1 Quorum and ballots

- A cluster of `n` nodes tolerates `f = (n-1)/2` crashed nodes. A **quorum is `f+1`**, and the leader counts itself. For `n = 3`, the leader plus one backup is a quorum.
- A **ballot** is a `(counter, node_id)` pair compared lexicographically, so two different nodes never have equal ballots.
- Each node tracks two ballots:
  - `current_ballot_`: the ballot of the leader it currently follows, or its own ballot while campaigning.
  - `highest_promised_ballot_`: the highest ballot it has promised. It rejects Accept messages with a lower ballot.

### 2.2 Roles and leader election

Every node starts as a `BACKUP` with `leader_id_ = -1`.

**Initial leader.** The first node of each cluster (`node_id % divisor_ == 1`) is the intended initial leader, and it gets elected quickly. When it receives its first client request while still at ballot counter 0, it queues the request and starts its election timer with a fixed **70 ms** timeout instead of the normal randomized one. The other backups don't start their timers at boot. A backup's timer starts the first time it hears from a leader, because `ResetElectionTimer` starts the timer if it isn't running. This makes the starting leaders deterministic (nodes 1, 4 and 7 with 3×3), with no race at startup.

**Election timer** (`StartElectionTimer` / `OnElectionTimeout`). This is a dedicated thread per backup that waits on a condition variable for `1000 ms + U(0, 300) ms`.
- Hearing from the leader (Accept, Commit, NEW-VIEW or heartbeat) calls `ResetElectionTimer`, which wakes the thread and restarts the wait.
- If the wait expires, the node increments its counter, promises itself, and sends `Prepare(counter+1, self)` to its cluster.
- The random jitter makes it unlikely that two backups campaign at the same moment.

**Prepare handling** (`HandlePrepare`):
- If the incoming ballot is not higher than `highest_promised_ballot_`, the node rejects it immediately.
- If it is higher, the node records it as promised. A leader receiving it steps down (`DemoteToBackup`). The node then promises in one of two ways, depending on how recently it heard from its leader:
  - **Promise immediately** if it hasn't heard from its leader within `prepare_cooldown_` (100 ms).
  - **Defer the promise** otherwise. The call is parked in `pending_promise_call_` and answered from `OnElectionTimeout` once this node's own timer expires. This stops a single backup with a flaky timer from taking over a cluster whose leader is still sending messages. Only the latest (highest) pending Prepare is kept.
- `OnElectionTimeout` also skips starting its own election if it saw a Prepare in the last 100 ms. That way two nodes don't keep outbidding each other.

**Promise.** A promise carries the node's entire `accept_log_`. The candidate collects promises until it has `f+1`, and then runs `MergeAcceptLogsFromPromises`, `FillMissingNoOps`, `BecomeLeader`, `SendNewView`, `InformClustersOfElection` and `OnElectionComplete`, in that order.

### 2.3 Becoming leader: merging logs and NEW-VIEW

**Merge** (`MergeAcceptLogsFromPromises`). For each sequence number, the new leader picks one entry from its own log and the promised logs:
1. If only one copy exists, that copy is used.
2. A **decided** 2PC entry always beats an undecided one, whatever the ballots. An entry is decided if it has a phase-2 status or a `phase2_result`. Replacing a decided entry with a higher-ballot phase-1 copy would undo a committed cross-shard transaction.
3. If both copies are equally decided, the higher ballot wins.
4. A known `phase2_result` is always copied onto the winning entry, so the final 2PC outcome is never lost.

After merging, the leader:
- sets `last_seqnum_` to the highest sequence number seen, so new proposals continue from there.
- rebuilds its duplicate-detection state from the inherited log:
  - `digest_to_seqnum_` maps a timestamp to its sequence number. It is rebuilt for every inherited entry.
  - `pending_or_completed_ts_` is the set of timestamps already handled. It gets every entry except NO-OPs and 2PC transactions still in progress, because those have to be finished by resending, not rejected as duplicates.

  Without this, a client retry of an inherited command would be proposed again under a new sequence number, and a 2PC reply arriving after a leader change would be dropped.

**Gap filling** (`FillMissingNoOps`). Every sequence number in `1..last_seqnum_` that no quorum member had is filled with a `NO-OP` entry. Execution needs a log with no gaps (see 2.5), so the new leader closes every gap before sending NEW-VIEW.

**NEW-VIEW** (`SendNewView` / `HandleNewView`). The leader sends its ballot and the **complete merged log** to its cluster, and records every entry as accepted by itself.
- A backup that receives it:
  - adopts the leader (`SetNewLeader`) and resets its timer.
  - replaces each log entry that it is missing or that has a lower ballot counter.
  - sends an **accept-ack for every entry** back to the leader.
- A leader with a lower ballot that receives a NEW-VIEW steps down. This resolves the rare case where two leaders exist at once.

`max_seqnum_in_new_view_` separates entries inherited from an earlier view from entries proposed in this one. Inherited entries don't have the client's pending `CallData`, so their replies are sent through the client's reply service instead (see 2.7).

### 2.4 Normal case: one intra-shard transaction

```
Client ──SendClientRequest──▶ Leader
Leader: try-lock accounts, assign seq = ++last_seqnum_, append to accept_log_
Leader ──Accept(ballot, seq, req)──▶ each backup in cluster
Backup: check ballot ≥ highest_promised, store in accept_log_ / pending_entries_, reset timer
Backup ──ReceiveAcceptAck(seq)──▶ Leader            (separate RPC, not the Accept reply)
Leader: count acks for (seq, ballot); at exactly f+1 → CommitEntry
Leader ──Commit(seq, req)──▶ each backup            (BroadcastCommit)
Leader + backups: SequentiallyExecuteCommittedEntries
Leader ──SendClientReply──▶ Client                  (client's own gRPC server)
```

Design notes:
- **Separate ack RPC.** A backup acknowledges an Accept with its own `ReceiveAcceptAck` RPC instead of in the Accept response. The NEW-VIEW catch-up (2.6) sends acks the same way, so the leader handles both cases in one place (`HandleAccept`).
- **Commit fires exactly once.** The quorum check is `== f+1`, not `>= f+1`, so later acks for the same sequence number don't trigger another commit.
- **Pipelining.** The leader doesn't wait for sequence number `k` to commit before proposing `k+1`. Many proposals can be in flight at once, and ordering is enforced when entries are executed.

### 2.5 Execution order

Committed entries go into `entries_to_commit_` (a `std::map` keyed by sequence number). `SequentiallyExecuteCommittedEntries` repeatedly executes `last_executed_seq_ + 1` while it is present, and stops at the first gap. So every replica applies the same commands in the same order, whatever order the Commit messages arrive in. A commit that arrives early waits in the map until the gap before it is filled.

`ExecuteTransaction` applies the balance change, releases the balance locks, and records the sequence number in `executed_entries_`. A transfer the sender can't afford is still marked as executed, with `success=false`. NO-OPs execute as nothing.

### 2.6 Catching up nodes that have fallen behind

A node falls behind when it was dead (see 4.5) or missed messages.

> **A node that falls behind catches up only when the leader changes.** There is no state-transfer or log-repair protocol. A backup that was dead, or that missed Accept or Commit messages, gets the missing entries only when a new leader is elected and sends NEW-VIEW. Until then it follows the current leader but can't execute past its first gap.

A node revived in the middle of a view:
- learns who the leader is from the next Accept with a higher ballot (`HandlePropose` → `SetNewLeader`).
- accepts new entries, stores their commits in `entries_to_commit_`, and resets its election timer on every leader message.
- executes nothing past its first missing sequence number. `SequentiallyExecuteCommittedEntries` stops at the gap, and nothing asks the leader to resend the missing entries.
- therefore stays behind while the leader is healthy. Its balances are out of date until the next election, and so are its answers to `PrintDB` and `PrintBalance`.

When the leader does change, the whole cluster catches up as follows:

1. **Promise:** the new leader merges logs from a quorum, so it learns every entry any quorum member accepted.
2. **Gaps become NO-OPs,** so the merged log has no gaps.
3. **NEW-VIEW** gives every backup the full log, including a backup that had nothing.
4. **Acks for every entry:** each backup acknowledges every entry, and the leader has already counted itself. So each sequence number reaches `f+1` again, and `CommitEntry` runs. With no pending entry for that sequence number, `CommitEntry` rebuilds the `CommitEntry` from `accept_log_`.
5. **Commits for every entry:** the leader sends a Commit for every sequence number. The behind node receives the whole prefix in order and replays it through the normal execution path. Entries it had already executed are skipped: `HandleCommit` checks `executed_entries_`, and `ExecuteRepeatedTwoPCEntry` is idempotent.

Inherited 2PC entries are the subtle part. A behind node may replay the phase-1 half of a cross-shard transaction whose outcome was decided while it was away. The outcome travels with the log entry as `phase2_result`, and the node resolves the replayed change against it using the WAL (see *Reconciling the WAL on a leader change* in section 3).

### 2.7 Client requests at the replica

`HandleClientRequest` → `ProcessClientRequest`:

- **Queuing:** while in an election, or when the node knows no leader, the request is added to `queued_client_requests_` and handled in `OnElectionComplete`.
- **Forwarding:** a backup that knows the leader forwards the request with a new async `SendClientRequest` and then finishes its own RPC.
- **Duplicate detection:**
  1. `last_reply_per_client_` caches the last reply per client. If the incoming timestamp matches it, the request is a duplicate that has already been answered.
  2. `pending_or_completed_ts_` is checked **before any balance lock is taken**. A retry of a request that has already finished would otherwise acquire locks and never release them.
  3. The same set is checked again, under `pending_mutex_`, just before the sequence number is assigned. This is the atomic claim that settles two copies of a brand-new request racing each other.
- **Locks:** each account has a mutex in `balance_locks_`. The leader only ever calls `try_lock`. If it fails, the request is rejected at once (the RPC finishes with no reply), and the client's 100 ms rebroadcast retries it. The replica never blocks while holding a lock, so there are no deadlocks between transactions.
- **Replies:** the client's `SendClientRequest` RPC stays pending (`pending_client_calls_`) until the entry executes. The result is then pushed to the client's own `ClientService.SendClientReply` endpoint, and the pending RPC is finished. Inherited entries have no pending call, so only the push is sent.
- **Read-only requests:** the leader replies straight from `accounts_`, with no Paxos round and no lock. This is fast, but a read can miss writes that were committed and not yet executed, and a deposed leader that hasn't noticed can serve a stale value.

### 2.8 Heartbeats and stepping down

- **Heartbeats:** the leader runs a `HeartbeatLoop` thread that sends `Heartbeat` to its cluster every **250 ms**. This matters most during 2PC: a leader waiting on another cluster may propose nothing for a long time, and without heartbeats its backups would time out and replace a healthy leader.
- **Stepping down:** a leader demotes itself when it sees a higher ballot in a Prepare, an Accept or a NEW-VIEW. `DemoteToBackup`:
  - stops heartbeats.
  - clears `leader_id_`.
  - clears the quorum counters, `pending_entries_` and `entries_to_commit_`; the new leader will resend them.
  - finishes all pending client RPCs, so clients retry against the new leader.
  - resets the 2PC in-flight tables.
  - reconciles the WAL (see *Reconciling the WAL on a leader change* in section 3).
- **Other clusters:** `InformClustersOfElection` sends a dummy Accept that carries only `new_leader` to every node **outside** the cluster, so other clusters' `cluster_leaders_` maps (used to route 2PC messages) stay current. A participant also learns the coordinator's leader from the `from_node` field of each PREPARE.

---

## 3. Cross-shard transactions: 2PC over Paxos

The sender's cluster is the **coordinator** and the receiver's cluster is the **participant**. The leader of each cluster drives its side, and every 2PC step is a Paxos entry in that cluster. So a 2PC decision survives the loss of a leader.

One transaction uses a **single sequence number per cluster**. Phase 1 proposes it, and phase 2 re-proposes the **same** sequence number with a phase-2 status. `digest_to_seqnum_` maps the transaction's timestamp to that sequence number.

| `two_pc_status` | Cluster | Meaning | Effect when executed |
|---|---|---|---|
| `COORD` | coordinator | phase 1, coordinator side | debit the sender, write a WAL entry, keep the sender lock |
| `P` | participant | prepared | credit the receiver, write a WAL entry, keep the receiver lock |
| `A` | participant | refused (receiver lock busy) | nothing |
| `C_COORD` / `A_COORD` | coordinator | phase 2 commit / abort | finalize, or undo from the WAL; release the lock; reply to the client |
| `C` / `A_PART_COMMIT` | participant | phase 2 commit / abort | finalize, or undo from the WAL; release the lock; tell the coordinator |

### Flow

```
Coordinator leader                                   Participant leader
------------------                                   ------------------
try-lock sender
PREPARE (SendClientRequest, two_pc_msg=PREPARE) ───▶ try-lock receiver → P or A
Paxos(COORD) → execute: debit + WAL                  Paxos(P|A)   → execute: credit + WAL
                                              ◀─── PREPARED | ABORT   (SendTwoPCMsg)
[wait until own COORD entry executed]
Paxos(C_COORD|A_COORD)                         ───▶ COMMIT | ABORT   (SendTwoPCMsg)
 → release/undo, reply to client                     Paxos(C|A_PART_COMMIT) → release/undo
                                              ◀─── COMMITTED | A_PART_COMMIT
stop tracking txn
```

- **Phases run in parallel:** the coordinator runs Paxos on its own COORD entry at the same time as the participant prepares. If PREPARED or ABORT arrives before the COORD entry has executed, it is parked in `prepare_and_aborted_queue_` and processed as soon as the COORD entry executes.
- **Commit after replication:** phase 2 commits through the same accept-ack mechanism, using a separate quorum counter (`accepted_count_two_pc_commit_`, acks with `phase=2`). `ExecuteRepeatedTwoPCEntry` applies it and writes the outcome into `phase2_result`.
- **Rollback:** each cluster's phase-1 change is recorded in the WAL so that a phase-2 abort can undo it (see below).
- **Timeouts:** a retry thread checks `in_flight_two_pc_prepares_` every 100 ms.
  - A PREPARE with no answer after **1 s** is sent once more, to the participant's current leader; right after a failover the cached leader may be dead.
  - After another second with no answer, the coordinator aborts on its own (`HandlePreparedTimeout` → Paxos `A_COORD`).
  - Phase 2 is **not** timed out. 2PC blocks once the coordinator has decided, so the transaction stays pending until that cluster is reachable again. Heartbeats keep the waiting leaders in place meanwhile.

### Write-ahead log (WAL)

A cross-shard transfer changes a balance in phase 1, before anyone knows whether it will commit. The WAL makes that change reversible.

**Structure.**
- `wal_` is a `std::map<int, WalEntry>`, keyed by sequence number and guarded by `wal_mutex_`.
- Each `WalEntry` holds `{before_value, after_value}` for the **one account** this cluster changes: the sender on the coordinator side, the receiver on the participant side.
- The WAL lives in memory only. It's an undo log, not a durable redo log.

**When entries are written.** An entry is written when a phase-1 entry **executes**. That happens on every replica (the leader and each backup), because execution is part of normal Paxos replay. The balance change and the WAL write happen together under `wal_mutex_`.

| Phase-1 entry | Balance change | WAL entry |
|---|---|---|
| `COORD` | `sender -= amt` | `{old, old − amt}` |
| `P` | `receiver += amt` | `{old, old + amt}` |
| `A` (participant refused) | none | none |
| intra-shard transfer | both accounts | none: a single log entry is already atomic |

**When entries are resolved.** Entries are resolved when the phase-2 entry for the same sequence number executes (`ExecuteRepeatedTwoPCEntry`). The WAL entry is removed in every case, and the account's balance lock is released:

| Phase-2 entry | Action |
|---|---|
| `C_COORD` / `C` (commit) | Keep the balance; the change is final |
| `A_COORD` (abort) | Restore the sender's balance to `before_value` |
| `A_PART_COMMIT` (abort) | Restore the receiver's balance to `before_value`, if there is an entry. There isn't one if the participant voted `A` |

On this normal path it's safe to restore the absolute `before_value`. The account's balance lock was held from phase 1 to phase 2, so no other transaction could change that account in between.

**Reconciling the WAL on a leader change.** Entries still in the WAL when leadership changes are resolved against the merged log's `phase2_result`. This runs in `OnElectionComplete` on the new leader and in `DemoteToBackup` on a leader that steps down:

| `phase2_result` | Action |
|---|---|
| `C_COORD` / `C` (committed) | Keep the balance and drop the WAL entry |
| `A_COORD` / `A_PART_COMMIT` (aborted) | **Undo by delta** (`balance += before − after`) and drop the entry. A node catching up replays without taking balance locks, so other changes to the same account may already have been applied after this one. Restoring the absolute `before` would wipe them out |
| empty (outcome unknown) | New leader: restore `before`, since resending the transaction will redo the change. Demoted node: keep the change and the WAL entry, and let the phase-2 commit that eventually arrives resolve it |

The new leader reconciles the WAL **before** processing its queued client requests. Otherwise the reconcile would also undo the new COORD debits those requests just made.

---

## 4. System design

### 4.1 Threads in a node process

| Thread | Count | Job |
|---|---|---|
| Server CQ pollers (`HandleRpcs`) | **2** | Share one `ServerCompletionQueue`. Each event calls `CallData::Proceed()`, which runs the handler directly on this thread |
| Client CQ poller (`PollClientCompletionQueue`) | 1 | Completes all outbound async RPCs (`OnComplete`, then `delete this`) |
| Election timer | 0–1 | Backups only. Started lazily and stopped when the node becomes leader or dies |
| Heartbeat sender | 0–1 | Leader only. Started in `BecomeLeader` and stopped in `DemoteToBackup` |
| 2PC retry loop | 1 | Checks for timed-out PREPAREs every 100 ms |

There's no separate worker pool: handlers run on the gRPC poller threads, and the **two server threads are the thread pool**. A handler must never block waiting on the network. Every outbound call is asynchronous, and anything that has to wait for another message stores state and returns. Two threads let a slow handler overlap with the next message without making lock contention much worse. The trade-offs are discussed in `PAXOS_PERF_NOTES.md`.

### 4.2 Async gRPC patterns

- **Server side:** each RPC type has a `CallData` state machine (`CREATE → PROCESS → FINISH`).
  - **3 handlers per RPC type** are posted at startup. Each handler posts its replacement as soon as it enters `PROCESS`, so there is always one waiting and a burst of messages doesn't find the service without a receiver.
- **Deferred responses:** a few handlers keep their `CallData` pointer and call `Respond()` later instead of finishing at once:
  - `Prepare`: promises that are held back (see 2.2)
  - `SendClientRequest`: waits until the entry executes
  - `NewView`
  
  `RespondAndClearNewViewCallData` takes the pointer under a mutex before responding, so the timer thread, the kill path and the CQ thread can't finish the same call twice.
- **Client side:** each outbound RPC is a heap object that owns its `ClientContext`, request and reply. It deletes itself in `OnComplete`. Calling `Finish(..., this)` must be **the last statement** in the constructor. Once the object is published as a tag, the poller thread can delete it immediately, so touching a member after that is a use-after-free.
- **Channels:** created with `GRPC_ARG_MAX_CONCURRENT_STREAMS = 10000` so that heavy pipelining isn't limited by HTTP/2 stream limits.

### 4.3 Locking

Shared state is split across many mutexes instead of one global lock:

| Mutex | Guards |
|---|---|
| `election_mutex_` | role, ballots, promise state, timer flags |
| `log_mutex_` | `accept_log_` |
| `pending_mutex_` | `pending_entries_`, `entries_to_commit_`, `pending_client_calls_`, `pending_or_completed_ts_`, sequence-number assignment |
| `quorum_mutex_` | `accepted_count_`, `accepted_count_two_pc_commit_` |
| `wal_mutex_` | `wal_` |
| `executed_entries_mutex_` | `executed_entries_`, `prepare_and_aborted_queue_` |
| `digest_to_seqnum_mutex_` | `digest_to_seqnum_`. This is a **leaf lock**: no other lock is ever taken while it is held, because it is reached from paths already holding different locks |
| `balance_locks_[i]` | per-account transaction isolation (`try_lock` only) |
| others | `client_mutex_`, `queued_mutex_`, `printlog_mutex_`, `newview_cd_mutex_`, `modified_accounts_mutex_`, `heartbeat_mutex_` |

Rules:
- Take a snapshot under the lock and do network I/O and logging outside it.
- When locks are nested, take them in this order:
  1. `election_mutex_` (outermost; e.g. log merging and demotion run under it)
  2. `pending_mutex_`
  3. `quorum_mutex_` / `log_mutex_`
  4. `digest_to_seqnum_mutex_` (always last)

### 4.4 Client (`PaxosClient`)

- `SendRequest` only enqueues. A single `TimerLoop` thread keeps a min-heap of pending requests ordered by next retry time:
  - **First send:** to the cached leader of the sender's cluster (`leader_per_cluster_`).
  - **Every 100 ms after that,** until a reply arrives: to **every node** in that cluster. Backups forward to the leader and the leader discards duplicates, so rebroadcasting is safe. It also routes around a dead leader without the client having to detect the failure.
- Replies arrive on the client's own gRPC server (1 poller thread). `HandleLeaderReply` marks the request complete (it is removed lazily from the heap), updates the cached leader from `reply.ballot().node_id`, and decrements `remaining_transactions`.
- The timestamp is the transaction's line index in the CSV, so it is unique across the whole system. It is used as the request's identity: for duplicate detection, as the 2PC digest, and as the index into the in-flight tables.

### 4.5 Test driver and simulating failures

- **CSV format:** a CSV row is `set, "(from, to, amt)" | "(acct)" | F(nX) | R(nX), "[live nodes]"`.
- **Starting a set:** each node gets an `AliveUpdate` with `reset_state` (`ResetNode` wipes all protocol and account state) and whether it's alive in that set.
- **Failure and recovery rows:** `F(nX)` and `R(nX)` wait until earlier transactions have finished, then kill or revive node X.
- **Between sets:** the driver waits until `remaining_transactions == 0`, pauses the nodes (`prompt_pause`) and opens the interactive prompt.
- **Abandoning a stuck set:** typing `Continue` during a set sends `MoveOn` to every node, which finishes pending calls, releases locks and clears 2PC tracking. The client then drops its in-flight requests.

Nodes never really crash. `kill()` clears the `alive_` flag, and every handler checks it first and drops the message. The process, its threads and its memory stay alive. A killed leader demotes itself first.

### 4.6 Storage

The account state that matters is **in memory** (`accounts_`). A per-node SQLite database (`database/node_<id>.db`, WAL journal mode) is created and reset at startup, and prepared statements exist for it. But writes are switched off (`alv_ = false`), so the database is not used for durability or recovery.

### 4.7 Logging

- `LOG` statements are compiled in only when `PAXOS_TRACE` or `PAXOS_TRACE_RING` is defined; otherwise they cost nothing.
- `LOGERR` is always compiled in.
- **Ring mode** writes each line into a lock-free in-memory ring instead of stdout, so tracing doesn't change the thread timing that races depend on. The ring is dumped on `SIGUSR1`, `SIGHUP`, `SIGTERM`, `SIGABRT` and `SIGSEGV`.

### 4.8 Benchmark and resharding

- **Benchmark mode** (`benchmark.h`) generates `N` transactions and writes them to `tests/test_data/benchmark.csv`, which is then run like any other test file. You control:
  - the fraction of read-only transactions.
  - the fraction that are cross-shard.
  - skew: a mix of uniform picks and a Gaussian around a random hotspot account.
- **`PrintReshard`** runs a greedy heuristic over the last set's transactions:
  - For each cross-shard pair, it moves the item with fewer cross-shard uses into the other item's cluster, as long as no cluster grows more than 30% above the average.
  - It then rebalances clusters that are too large.
  
  The new map is sent to every node (`AliveUpdate.reshard`).

---

## 5. Timing constants

| Constant | Value | Where |
|---|---|---|
| Election timeout | 1000 ms + U(0, 300) ms | `base_timeout_`, `jitter_` |
| Bootstrap election (initial leader) | 70 ms | `StartElectionTimer` |
| Prepare cooldown | 100 ms | `prepare_cooldown_` |
| Heartbeat interval | 250 ms | `HeartbeatLoop` |
| Client retry / rebroadcast | 100 ms | `PaxosClient::retry_interval_` |
| 2PC PREPARE timeout | 1 s, one retry, then abort | `TransactionRetryLoop` |

## 6. Known limitations

- **Catch-up only on a leader change.** A node that falls behind stays behind until the next election (see 2.6). A stable leader never repairs it, so without a failover a revived backup never catches up.
- **Fixed-size tables indexed by timestamp.** `in_flight_two_pc_prepares_` and `in_flight_two_pc_transactions_` have 5000 slots and are indexed by timestamp, so more than 5000 transactions in one run (e.g. a large benchmark) indexes past the end.
- **Balance locks are released on other threads.** `balance_locks_` are `std::mutex`es that are locked on one thread and unlocked on another. Backups also unlock mutexes they never locked. The C++ standard doesn't allow either, although it works with libc++ on macOS.
- **Leftover 3×3 assumptions:**
  - `PaxosClient::HandleLeaderReply` computes the leader's cluster as `(id-1)/num_clusters + 1`. That's only correct when the number of nodes per cluster equals the number of clusters.
  - Benchmark mode lists the live nodes as `n1..n9`.
  
  Both need a small fix before running other topologies (see Scaling in section 1).
- **Array indexing in `Reshard`.** `Reshard` indexes `cluster_loads` (size `num_clusters`) by cluster ID (`1..num_clusters`), so the last cluster indexes past the end.
- **Reads can be stale.** Read-only requests are not linearizable (see 2.7).
- **No persistence.** State is lost if the process crashes (see 4.6).
- **Benchmark write path doesn't match the read path.** The generator writes to `../test_data/benchmark.csv` ([benchmark.h](src/benchmark.h)), but the run reads `../tests/test_data/benchmark.csv`. Without a `test_data → tests/test_data` symlink at the repo root, benchmark mode runs an outdated file.
