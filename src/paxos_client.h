#ifndef PAXOS_CLIENT_H
#define PAXOS_CLIENT_H

#include "call_data.h"
#include <grpcpp/grpcpp.h>
#include "../grpc/paxos.grpc.pb.h"
#include <memory>
#include <thread>
#include <functional>
#include <atomic>


/** 
 * Class for paxos client 
 * Asynchronous clients: Do not block while waiting for RPCs to complete -> allows multiple concurrent requests
 */
class PaxosClient {
public:
    explicit PaxosClient(const std::string& server_address, const std::string& client_id, const int initial_leader_id, std::unordered_map<int, std::shared_ptr<paxos::Paxos::Stub>> stubs, std::unordered_map<int, std::string> addresses, int num_clusters, std::unordered_map<int, int> initial_ranges);
    ~PaxosClient();

    // Sends a ClientRequest asynchronously
    void SendRequest(const paxos::ClientRequest& request,
                     std::function<void(const paxos::TransactionAck&)> callback);
    
    void StopTimerThread();
    
    
    void StartPolling();

    void StopPolling();

    void HandleLeaderReply(const paxos::ClientReply& reply);

    bool SendMoveOn(const std::string& client_id, int target_node_id);

    void HandleMoveOn();

    //new 
    std::unordered_map<int, int> sharding_map;

    void reset_leaders_per_cluster();

private:


    std::string client_id_; 
    std::shared_ptr<paxos::Paxos::Stub> stub_; // local handle to the remote service
    std::mutex stub_mutex_;  // Protects stub_ in case of leader switching
    std::unordered_map<int, std::shared_ptr<paxos::Paxos::Stub>> node_stubs_;
    std::unordered_map<int, std::string> node_addresses_; 
    

    std::unique_ptr<grpc::CompletionQueue> cq_; // changed from ServerCompletionQueue

    std::vector<std::thread> polling_threads_;
    std::vector<std::thread> server_threads_;
    std::atomic<bool> running_{false};
    void PollCompletionQueue();

    // For managing pending requests and retries, holds request, callback and expiry time
    struct PendingRequest {
        paxos::ClientRequest request;
        std::chrono::steady_clock::time_point expiry;
        std::atomic<bool> active{false}; // new
        std::atomic<bool> completed{false}; /// new

        PendingRequest(const paxos::ClientRequest& req)
            : request(req) {}
    };

    std::unordered_map<int, std::shared_ptr<PendingRequest>> inflight_;
    std::mutex pending_mutex_;
    std::condition_variable pending_cv_;
    std::atomic<bool> stop_timer_{false};
    std::chrono::milliseconds retry_interval_{1000}; //cchange this 2000
    std::thread timer_thread_;

    // Helper functions
    void TimerLoop();
    void AddPendingRequest(const paxos::ClientRequest& request,
                           std::function<void(const paxos::TransactionAck&)> callback);
    void SendToLeader(const paxos::ClientRequest& request,
                      std::function<void(const paxos::TransactionAck&)> callback);
    void BroadcastRequest(const paxos::ClientRequest& request,
                          std::function<void(const paxos::TransactionAck&)> callback);

    void SetRequestTimestamp(paxos::ClientRequest& request); // new

    //server side:
    paxos::ClientService::AsyncService client_service_;
    std::unique_ptr<grpc::ServerCompletionQueue> server_cq_;
    std::unique_ptr<grpc::Server> server_;

    void StartServer(const std::string& address);
    void StopServer();
    void PollServerQueue();

    AsyncCall* ongoing_call_ = nullptr; // to allow cancellation if needed

    
    // new
    int num_clusters_;
    std::unordered_map<int, int> leader_per_cluster_; // cluster_id -> leader_node

    struct CompareExpiry {
        bool operator()(const std::shared_ptr<PendingRequest>& a,
                        const std::shared_ptr<PendingRequest>& b) const {
            return a->expiry > b->expiry; // min-heap
        }
    };

    std::priority_queue<std::shared_ptr<PendingRequest>,
                        std::vector<std::shared_ptr<PendingRequest>>,
                        CompareExpiry> timer_heap;

};

#endif 