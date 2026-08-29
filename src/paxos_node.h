#ifndef PAXOS_NODE_H
#define PAXOS_NODE_H

#include <grpcpp/grpcpp.h>
// #include <grpc/support/log.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <condition_variable>
#include "../grpc/paxos.grpc.pb.h"
#include "../grpc/paxos.pb.h"
#include "call_data.h"
#include "paxos_client.h"
#include <sqlite3.h>
#include <libpq-fe.h>

class PaxosClient;
class SendClientRequestCallData;
/**
 * @brief Represents a ballot in the Paxos consensus algorithm.
 * A ballot is identified by a counter and a node ID.
 * The comparison operators are defined to facilitate ballot comparisons.
 */
struct Ballot {
    int counter;
    int node_id;

    Ballot(int c = 0, int id = 0) : counter(c), node_id(id) {}

    bool operator>(const Ballot& other) const {
        return (counter > other.counter) || (counter == other.counter && node_id > other.node_id);
    }

    bool operator<(const Ballot& other) const {
        return (counter < other.counter) || (counter == other.counter && node_id < other.node_id);
    }

    bool operator>=(const Ballot& other) const {
        return (*this > other) || (*this == other);
    }

    bool operator==(const Ballot& other) const {
        return counter == other.counter && node_id == other.node_id;
    }

    std::string ToString() const {
        return "(" + std::to_string(counter) + "," + std::to_string(node_id) + ")";
    }
};

struct AsyncCallTransactionAck {
    std::unique_ptr<grpc::ClientAsyncResponseReader<paxos::TransactionAck>> response_reader;
    grpc::ClientContext context;
    paxos::TransactionAck reply;
    grpc::Status status;
    std::function<void(AsyncCallTransactionAck*)> callback;
    std::function<void(const paxos::TransactionAck&)> completion_callback;
};

struct QueuedClientRequest {
    paxos::ClientRequest request;
    SendClientRequestCallData* call_data;
};

struct WalEntry {
    double before_value;
    double after_value;
};

struct InFlightTransaction {
    paxos::TwoPCMsg msg;       // the commit you sent
    std::chrono::steady_clock::time_point last_send; // last time sent
    std::atomic<bool> completed{false};         // whether ACK was received
    std::atomic<bool> sent{false};              // whether the commit was sent
    int retries = 0;                            // PREPARE re-send count (retry once, then abort)
};

/** 
 * PaxosNode represents a single node in a Paxos distributed consensus system.
 * Each node can act as a leader or backup, handling client requests and coordinating with other nodes thorugh grpc 
 * 
 * Asynchronous Server: Does not block while waiting for RPCs -> allows for concurrent handling of multiple requests
 * Thus multiple threads can poll the completion queue (CQ) and process incoming RPCs in parallell
 */
class PaxosNode {
public:

    enum class Role { LEADER, BACKUP };


    PaxosNode(int id, const std::string& address, Role role, const std::unordered_map<int, std::string>& peer_addresses, std::unordered_map<std::string, std::string> client_addresses,
              std::unordered_map<int, int> shard_map, int num_clusters);
    
    // Starts the node's gRPC server and event loop
    void Run();

    // Send RPCs to other nodes
    void SendPrepare(Ballot ballot);
    void SendPromise();
    void SendAccept(Ballot ballot, int seqNum, const paxos::ClientRequest& request);
    void SendCommit(Ballot ballot, int seqNum, const paxos::ClientRequest& request);
    void SendNewView();
    void SendAcceptAckToLeader(const paxos::AcceptedEntry& entry);

    // Handles incoming RPCs
    void HandlePrepare(const paxos::PrepareRequest& request, PrepareCallData* call_data);
    void HandlePropose(const paxos::AcceptedEntry& request, paxos::Ack* reply);
    void HandleCommit(const paxos::CommitEntry& request, paxos::Ack* reply);
    
    /**Called by backups when they receive new view message
     * Updates their state to reflect the new leader and its ballot
     */
    void HandleNewView(const paxos::NewViewRequest& request, NewViewCallData* call_data); // modified

    void HandleClientRequest(const paxos::ClientRequest& request, SendClientRequestCallData* call_data); // new
    void HandlePromise(const paxos::PromiseResponse& reply); 
    void HandleAck(const paxos::Ack& reply);
    void HandleSetAlive(const paxos::AliveUpdateRequest& request, paxos::TransactionAck* reply);
    void HandleAccept(const paxos::Ack& ack); //, paxos::TransactionAck* reply
    void HandleServerReply(const paxos::ClientReply& reply);
    void HandleSendNodeInfo(const paxos::InfoRequest& request, paxos::NodeInfo* reply);
    void HandleMoveOn(const paxos::MoveOnRequest& request, paxos::TransactionAck* reply);

    void StopElectionTimer(); // need to impl
    void ResetElectionTimer(); // need to impl

    // Simulate node failure/recovery
    void kill();
    void revive();
    bool isAlive() const { return alive_.load(); }


    int GetNodeId() const { return node_id_; }
    Role GetRole() const { return role_; }

    bool IsClientCQRunning() const { return client_cq_running_.load(); }

    static std::string AcceptLogToString(const std::map<int, paxos::AcceptedEntry>& accept_log);

    void OnNewViewCallFinished(NewViewCallData* call_data);

    // new 

    void HandlePreparedAndAborted(const paxos::TwoPCMsg& msg);
    void HandleCommittedAndAborted(const paxos::TwoPCMsg& msg);

    // Backup receives a heartbeat from its leader -> just reset the election timer.
    void HandleHeartbeat(const paxos::HeartbeatRequest& request);

private:
    int node_id_;
    std::string address_;
    Role role_;
    std::atomic<bool>alive_{true}; // is node alive
    std::unordered_map<std::string, std::string> client_addresses_;

    // gRPC server
    std::unique_ptr<grpc::Server> server_;
    paxos::Paxos::AsyncService service_;
    std::unique_ptr<grpc::ServerCompletionQueue> cq_;

    // gRPC client
    std::unique_ptr<grpc::CompletionQueue> client_cq_; // for client-side
    std::thread client_cq_thread_;
    std::atomic<bool> client_cq_running_{false};
    

    // Paxos state
    Ballot current_ballot_;
    std::unordered_map<std::string, paxos::ClientReply> last_reply_per_client_;
    std::map<int, paxos::AcceptedEntry> accept_log_; // Log of accepted proposals
    Ballot highest_promised_ballot_;

    std::unordered_set<int> promises_received_;   // nodes that sent PromiseResponses, reset on election timeout 
    bool waiting_for_promises_; 
    // do i need promised ballots?

    std::unordered_map<int, std::vector<paxos::AcceptedEntry>> received_promise_logs_; 


    //leader info
    int leader_id_; 

    //peers
    std::unordered_map<int, std::shared_ptr<paxos::Paxos::Stub>> peer_stubs_; 
    std::unordered_map<int, std::string> peer_addresses_; 


    std::mutex log_mutex_; // Mutex for synchronizing access to accept_log_
    std::mutex client_mutex_; // Mutex for synchronizing access to last_reply_per_client_
    std::unordered_map<std::string, std::shared_ptr<paxos::ClientService::Stub>> client_stubs_; // map of client_id to stub to be lazily populated

    // Internal helper functions
    void HandleRpcs(); // single-threaded event loop (server side)
    void PollClientCompletionQueue(); //for client-side asynch calls


    void BecomeLeader(Ballot ballot);
    void DemoteToBackup();
    void SetNewLeader(int new_leader_id, int new_leader_counter);
    void FillMissingNoOps() ;
    void CommitEntry(const paxos::Ack entry);

    bool ExecuteTransaction(const paxos::ClientRequest& request, int seqnum, std::string two_pc_status="");
    void BroadcastCommit(const paxos::CommitEntry& entry);
    void SendClientReply(SendClientRequestCallData* call_data, int seq_num, bool success);
    void SequentiallyExecuteCommittedEntries();

    // Timer for leader election
    void StartElectionTimer();
    void OnElectionTimeout();
    std::chrono::milliseconds GetRandomizedTimeout();

    //Election Timer Fields:
    std::thread election_thread_; // bg thread for timer
    std::mutex election_mutex_; // protects state of timer
    std::condition_variable election_cv_; // to wake up timer thread
    
    //elec state 
    bool timer_running_ = false; // tell timer to keep looping
    bool election_reset_ = false; // timer shoudl restart

    // Parameters
    std::chrono::milliseconds base_timeout_{2000}; //3000
    std::chrono::milliseconds jitter_{500}; 

    // Cooldown timer to prevent rapid successive Prepare requests
    std::chrono::steady_clock::time_point last_prepare_received_;
    std::chrono::steady_clock::time_point last_leader_msg_received_;
    std::chrono::milliseconds prepare_cooldown_{100}; // t_p in instructions

    // new_view fields
    std::mutex newview_ack_mutex_;
    std::unordered_set<int> newview_acks_;
    bool newview_quorum_reached_ = false;


    // pending promise 
    PrepareCallData* pending_promise_call_ = nullptr; // to store promise reply if timer not expired
    


    // accept quorum tracking
    std::unordered_map<int, std::unordered_map<int, std::unordered_set<int>>> accepted_count_; // Track accepted message counts from backups for sequence numbers
    // Store pending entries so they can be committed once quorum is reached
    std::unordered_map<int, paxos::AcceptedEntry> pending_entries_;
    std::mutex quorum_mutex_; // recheck
    
    std::unordered_set<int> executed_entries_; // all executed seq nums
    std::unordered_set<int> executed_two_pc_;  // seqnums whose 2PC phase-2 (commit/abort) was already applied -- idempotency guard (executed_entries_mutex_)
    std::map<int, paxos::CommitEntry> entries_to_commit_; // committed entries
    int last_executed_seq_ = 0;
    int f_; // max faulty nodes
    // std::mutex commit_mutex_;  // protect sequential commit - removed 


    std::atomic<int> last_seqnum_{0};

    std::vector<double> accounts_ =  std::vector<double>(9001); ; 

    // Map of sequence numbers to client RPC call objects, to respond once committed
    std::unordered_map<int, SendClientRequestCallData*> pending_client_calls_;
    std::mutex pending_mutex_; // protects pending_client_calls_

    // service side
    paxos::ClientService::AsyncService client_service_;
    std::unique_ptr<grpc::ServerCompletionQueue> client_service_cq_;
    std::atomic<bool> client_service_cq_running_{false};
    std::thread client_service_cq_thread_;

    bool in_election_ = false;
    std::vector<QueuedClientRequest> queued_client_requests_;
    void ProcessClientRequest(const paxos::ClientRequest& request, SendClientRequestCallData* call_data);
    void OnElectionComplete();

    std::mutex printlog_mutex_; // TODO - later remove this and switch to using log_mutex_
    std::string print_log_;
    std::vector<paxos::NewViewRequest> new_view_logs_;

    void MergeAcceptLogsFromPromises();  
    
    NewViewCallData* newview_call_data_ = nullptr;
    std::mutex newview_cd_mutex_; // guards newview_call_data_ against concurrent Respond/clear across threads
    bool in_new_view_ = false;
    int max_seqnum_in_new_view_ = 0;

    // Atomically take ownership of newview_call_data_ (nulling the member) and
    // Respond exactly once, preventing double-Finish/double-free of the call data.
    void RespondAndClearNewViewCallData();
    
    bool alive_before_prompt_ = true; // to track if node was alive before prompt since its shut to dead during prompt


    // new 
    std::set<int> modified_accounts_;
    std::vector<std::mutex> balance_locks_{9000 + 1};
    std::unordered_map<int, int> shard_map_; // map of account_id to cluster_id
    int divisor_;
    std::pair<int, int> intra_c_nid_range_;
    int cluster_id_;
    std::map<int, WalEntry> wal_;
    std::mutex wal_mutex_;
    std::map<int, int> cluster_leaders_; // map of cluster_id to leader node_id

    void SendReplyToCoordLeader(SendClientRequestCallData* call_data, int seq_num, std::string two_pc_status);
    std::unordered_map<int, int> digest_to_seqnum_; // map of digest to seqnum for 2pc

    void ExecuteRepeatedTwoPCEntry(const paxos::CommitEntry& entry);
    
    std::vector<InFlightTransaction> in_flight_two_pc_transactions_{5000}; // key = ????
    std::vector<InFlightTransaction> in_flight_two_pc_prepares_{5000}; 
    
    void TransactionRetryLoop();
    std::thread LaunchTransactionRetryThread();
    std::thread retry_thread_;

    // Leader-only heartbeat sender. Started on becoming leader, stopped on demotion.
    // The loop runs while heartbeat_running_ (i.e. while leader) but only *sends* when
    // alive_ -- so a paused leader (prompt-pause) holds the thread and resumes after.
    void StartHeartbeat();
    void StopHeartbeat();
    void HeartbeatLoop();
    std::thread heartbeat_thread_;
    std::atomic<bool> heartbeat_running_{false};
    std::condition_variable heartbeat_cv_;
    std::mutex heartbeat_mutex_;

    std::unordered_map<int, std::unordered_map<int, std::unordered_set<int>>> accepted_count_two_pc_commit_;

    void SendClientReplyTwoPC(const paxos::CommitEntry& entry, int seq_num, bool success);

    void ResetNode();
    int num_clusters_;



    void HandlePreparedTimeout(int timestamp);

    void InitializeDatabase();
    void UpdateBalance(int item_id, int amount, bool sender);
    bool alv_ = false;
    std::unordered_map<int, paxos::TwoPCMsg> prepare_and_aborted_queue_; // map of timestamp to msg
    void StartSecondPhase(paxos::TwoPCMsg msg);

    std::mutex executed_entries_mutex_;

    std::unordered_set<int> handled_ro_requests_; // by ts
    std::atomic<bool> first_txn_received_{true};

    std::mutex queued_mutex_;

    std::unordered_set<int> pending_or_completed_ts_; // to avoid reprocessing
    
    void InformClustersOfElection();
    
    void InitDB();
    void SetBalance(int item_id, double balance);
    sqlite3* db_ = nullptr;
    sqlite3_stmt* update_stmt_ = nullptr;
    int GetBalance(int item_id);

};


#endif 
