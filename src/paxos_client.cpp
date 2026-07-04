#include "paxos_client.h"
#include "call_data.h"
#include "log.h"
#include <iostream>

PaxosClient::PaxosClient(const std::string& server_address, 
    const std::string& client_id, 
    const int initial_leader_id, 
    std::unordered_map<int, std::shared_ptr<paxos::Paxos::Stub>> stubs, 
    std::unordered_map<int, std::string> addresses, 
    int num_clusters,
    std::unordered_map<int, int> initial_ranges) 
    {
        cq_ = std::make_unique<grpc::CompletionQueue>();
        stub_ = stubs[initial_leader_id];
        node_stubs_ = stubs;
        node_addresses_ = addresses;
        client_id_ = client_id;
        num_clusters_ = num_clusters;
        leader_per_cluster_ = std::unordered_map<int, int>{};
        for (int c = 0; c < num_clusters_; c++) {
            leader_per_cluster_[c + 1] = (c* addresses.size() / num_clusters_) + 1;
        }
        sharding_map = initial_ranges;
        
        StartPolling();
        timer_thread_ = std::thread([this]() { TimerLoop(); });
        
        LOG << "[Client " << client_id_ << "] Starting server at " << server_address << std::endl;
        StartServer(server_address); 
        
    }

PaxosClient::~PaxosClient() {

    StopTimerThread();
    StopPolling();
    StopServer();
}


void PaxosClient::StartPolling() {
    running_ = true;
    int num_threads = 1; 
    for (int i = 0; i < num_threads; ++i) {
        polling_threads_.emplace_back([this]() { PollCompletionQueue(); });
    }
}

void PaxosClient::StopPolling() {
    running_ = false;
    cq_->Shutdown();
    for (auto& t : polling_threads_) {
        if (t.joinable()) t.join();
    }
    polling_threads_.clear();
}


void PaxosClient::StopTimerThread() {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        stop_timer_ = true;
    }
    pending_cv_.notify_one();
    if (timer_thread_.joinable()) timer_thread_.join();
}


void PaxosClient::SetRequestTimestamp(paxos::ClientRequest& request) {
    // set timestamp for send 
    auto now = std::chrono::steady_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    request.set_timestamp(timestamp); 
}

// ----------------- Timer loop -----------------
void PaxosClient::TimerLoop() {
while (true) {
    std::unique_lock<std::mutex> lock(pending_mutex_);
    
    if (stop_timer_) break;

    while (!timer_heap.empty()) {
        auto req = timer_heap.top();

        // Lazy deletion: completed requests
        if (req->completed.load()) {
            timer_heap.pop();  // just remove it
            continue;
        }

        auto now = std::chrono::steady_clock::now();
        if (req->expiry > now) {
            // Wait until next request expiry
            pending_cv_.wait_until(lock, req->expiry);
            break;  // check again after waking
        }

        // Timer expired, send/broadcast
        timer_heap.pop();  // remove it before retry
        if (!req->active.load()) {
            req->active.store(true);
            req->expiry = now + retry_interval_;
            LOG << "[Client " << client_id_ << "] Sending request (ts=" 
                      << req->request.timestamp() << ") from " << req->request.from_account() << std::endl;
            SendToLeader(req->request, nullptr);
        } else {
            req->expiry = now + retry_interval_;
            BroadcastRequest(req->request, nullptr);
        }

        // push it back with updated expiry
        timer_heap.push(req);
    }

    // If heap is empty, just wait for new requests
    if (timer_heap.empty()) {
        pending_cv_.wait(lock);
    }
}
}

// handles a sendrequest. Moreso queues the request and lets the timer thread handle sending/retries
void PaxosClient::SendRequest(const paxos::ClientRequest& request,
                             std::function<void(const paxos::TransactionAck&)> callback) {

    LOG << "[Client " << client_id_ << "] Queuing transaction request (client_id=" 
              << request.client_id() << ", from_account=" 
              << request.from_account() << ", to_account=" 
              << request.to_account() << ", amount=" 
              << request.amount() << ", ts=" 
              << request.timestamp() << ")\n";
                                
    auto req_ptr = std::make_shared<PendingRequest>(request);

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        inflight_[request.timestamp()] = req_ptr;

        // Set initial expiry
        req_ptr->expiry = std::chrono::steady_clock::now();

        timer_heap.push(req_ptr);
    }

    pending_cv_.notify_one();
}


void PaxosClient::SendToLeader(
    const paxos::ClientRequest& request,
    std::function<void(const paxos::TransactionAck&)> callback) 
{
    int leader_id;
    std::shared_ptr<paxos::Paxos::Stub> leader_stub;
    LOG << "from account: " << request.from_account() << std::endl;
    int cluster = sharding_map[std::stoi(request.from_account())];
    LOG << "[Client] determined cluster " << cluster << " for from_account " 
              << request.from_account() << std::endl;
    {
        std::lock_guard<std::mutex> lock(stub_mutex_);
        
        leader_id = leader_per_cluster_[cluster];
        auto it = node_stubs_.find(leader_id);
        if (it != node_stubs_.end()) {
            leader_stub = it->second;
        } else {
            std::cerr << "[Client] No stub found for leader node " << leader_id << std::endl;
            return;
        }
    }

    LOG << "[Client] making SendToLeader RPC to node " << leader_id << "of cluster " << cluster
              << " (ts=" << request.timestamp() << ")\n";


    if (request.timestamp() == 0) {
        extern std::chrono::steady_clock::time_point start_time;
        start_time = std::chrono::steady_clock::now();
    }
    
    new AsyncCall(leader_stub, cq_.get(), request, callback); // will delete itself
}

// Broadcasts request to all nodes 
void PaxosClient::BroadcastRequest(
    const paxos::ClientRequest& request,
    std::function<void(const paxos::TransactionAck&)> callback) 
{
    int from_account = std::stoi(request.from_account());
    int cluster = sharding_map[from_account];

    int start_node = (cluster - 1) * (node_stubs_.size() / num_clusters_) + 1;;
    int end_node = cluster * (node_stubs_.size() / num_clusters_);

    for (auto& [node_id, stub_ptr] : node_stubs_) {

        if (node_id >= start_node && node_id <=end_node){
            auto call = new AsyncCall(stub_ptr, cq_.get(), request, callback);

            LOG << "[Client] Broadcast sent to node " << node_id
                    << " (ts=" << request.timestamp() << ")" << std::endl;
        }
    }

}

// client side polling loop
void PaxosClient::PollCompletionQueue() {
    void* tag;
    bool ok;
    while (running_) {
        if (cq_->Next(&tag, &ok)) {
            if (tag) {
                auto* call = static_cast<AsyncCall*>(tag);
                call->OnComplete(ok);
            }
        }
    }
}


void PaxosClient::StartServer(const std::string& address) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&client_service_);
    server_cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();

    // Start accepting calls
    int init_handlers = 20;
    for (int i = 0; i < init_handlers; ++i){
        new AsyncServerCall(&client_service_, server_cq_.get(), this);
    }


    int num_server_threads = 1; // or 1/2 of available cores for async server
    LOG << "[Client " << client_id_ << "] Starting " << num_server_threads 
              << " server threads for handling incoming requests." << std::endl;
    for (int i = 0; i < num_server_threads; ++i) {
        server_threads_.emplace_back([this]() { PollServerQueue(); });
    }

    
}

void PaxosClient::StopServer() {
    if (server_) server_->Shutdown();
    if (server_cq_) server_cq_->Shutdown();
    for (auto& t : server_threads_) {
        if (t.joinable()) t.join();
    }
    server_threads_.clear();
}

// server side polling loop
void PaxosClient::PollServerQueue() {
    void* tag;
    bool ok;
    while (running_) {
        if (server_cq_->Next(&tag, &ok) && tag != nullptr && ok) {
            static_cast<AsyncServerCall*>(tag)->Proceed();
        }
    }
}



void PaxosClient::HandleLeaderReply(const paxos::ClientReply& reply) {
    LOG << "[CLIENT " << client_id_ << "] Received reply from leader (ts="
              << reply.timestamp() << ")\n";

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);

        // Find the request matching this reply
        auto it = inflight_.find(reply.timestamp());

        if (it != inflight_.end()) {
            //LOG << "[CLIENT " << client_id_ << "] Matching pending request found (ts="
            //          << reply.timestamp() << "), removing from pending list.\n";
            it->second->completed.store(true); // shared ptr could still hold a reference in snapshot
            inflight_.erase(it);
            pending_cv_.notify_one();
        }
    }


    //LOG << "[CLIENT " << client_id_ << "] Transaction committed successfully (ts="
    //            << reply.timestamp() << ")\n";

    int new_leader_id = reply.ballot().node_id();

    {
        std::lock_guard<std::mutex> lock(stub_mutex_);
        leader_per_cluster_[((new_leader_id -1)/ num_clusters_) + 1] = new_leader_id;
    }
    


    // Decrement remaining transactions
    if (reply.from_this_term()) {
        extern std::atomic<int> remaining_transactions;  
        --remaining_transactions;
        LOG << "[CLIENT " << client_id_ << "] decrementing total transactions. Remaining transactions: " 
                << remaining_transactions.load() << std::endl;
        if (remaining_transactions.load() == 0) {
            extern std::chrono::steady_clock::time_point end_time;
            end_time = std::chrono::steady_clock::now();
        }
    }
    else {
        LOG << "[CLIENT " << client_id_ << "] Reply not from this term, part of NV, not decrementing remaining transactions.\n";
    }

}

bool PaxosClient::SendMoveOn(const std::string& client_id, int target_node_id) {
    paxos::MoveOnRequest request;
    request.set_client_id(client_id);

    paxos::TransactionAck reply;
    grpc::ClientContext context;

    auto it = node_stubs_.find(target_node_id);

    LOG << "[Client " << client_id_ << "] Sending MoveOn to node " << target_node_id << std::endl;
    grpc::Status status = it->second->SendMoveOn(&context, request, &reply);

    if (!status.ok()) {
        std::cerr << "[Client] MoveOn RPC failed: " << status.error_message() << std::endl;
        return false;
    } else{
        LOG << "[Client] MoveOn RPC to node " << target_node_id 
                  << " succeeded: " <<  std::endl;
    }

    return reply.accepted();
}

void PaxosClient::HandleMoveOn() {
    // cleaer all tranactions queued for the current set and move on 
    
    for (auto& [ts, req] : inflight_) {
        req->completed = true; 
        req.reset();
    }

    int size_pending = inflight_.size();
    inflight_.clear();

    // Hard-reset timer heap by popping each element
    while (!timer_heap.empty()) {
        auto req = timer_heap.top();
        timer_heap.pop();

        // kill shared_ptr
        if (req) {
            req->completed.store(true);
            req.reset(); // explicit destruction
        }
    }

    extern std::atomic<int> remaining_transactions;  
    remaining_transactions -= size_pending;
    LOG << "[Client " << client_id_ << "] Received MoveOn, clearing " << size_pending 
              << " pending requests. Remaining transactions: " 
              << remaining_transactions.load() << std::endl;
    pending_cv_.notify_one();

}

void PaxosClient::reset_leaders_per_cluster() {
    std::lock_guard<std::mutex> lock(stub_mutex_);
    for (int c = 0; c < num_clusters_; c++) {
        leader_per_cluster_[c + 1] = (c* node_stubs_.size() / num_clusters_) + 1;
    }
}

