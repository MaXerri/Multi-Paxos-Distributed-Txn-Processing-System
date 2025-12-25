# Paxos Implementation Documentation:

### Node Implementation

The PaxosNode class represents a node in a Paxos-based replicated state machine. Each node may serve as either a leader or a backup. The node is responsible for handling client requests, participating in the Paxos consensus protocol (Prepare, Accept, Commit, NewView), and running an election timer to initiate leadership changes.

Runining a node builds a gRPC server with registered RPC services (PrepareCallData, AcceptCallData, CommitCallData, NewViewCallData) and also initializes peer stubs to all other nodes for outbound RPCs

#### Multithreaded Event Loop

Each worker thread blocks on cq_ -> Next(&tag, &ok) to wait for incoming RPC events

#### Handle Client Request

```python
void PaxosNode::HandleClientRequest(
    const paxos::ClientRequest& request,
    paxos::ClientReply* reply
)
```

	•	Ensures exactly-once semantics by caching the last reply for each client (last_reply_per_client_).
	•	If the request is a duplicate, returns the cached reply.
	•	Otherwise:
	•	Builds a new ClientReply.
	•	Sets its nested Ballot (via mutable_ballot()).
	•	Fills timestamp, client_id, and a placeholder success=true.
	•	Caches the reply for future duplicates.

#### Election Timer






### Client Implementation:



### RPC Lifecycle Imlpementation



### Flow of a Transaction:

Client code calls SendRequest()
    |
    v
Async request sent
    |
    v
Server handles request -> sends ClientReply
    |
    v
CompletionQueue thread sees response -> calls OnComplete
    |
    v
OnComplete calls the callback
    |
    v
AsyncCall memory deleted






