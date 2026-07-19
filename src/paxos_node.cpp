#include "paxos_node.h"
#include "call_data.h"
#include "log.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <random>
#include <sqlite3.h>
#include <sys/stat.h>

PaxosNode::PaxosNode(int id, const std::string& address, Role role, const std::unordered_map<int, std::string>& node_addresses, std::unordered_map<std::string, std::string> client_addresses, 
                     std::unordered_map<int, int> shard_map, int num_clusters):
    node_id_(id),
    address_(address),
    role_(role),
    current_ballot_(0, id),
    peer_addresses_(node_addresses), // contains all nodes including self
    waiting_for_promises_(false),
    leader_id_(-1), // everyone starts as backup and doesn't know who leader is
    alive_(true),
    last_prepare_received_(std::chrono::steady_clock::now() - std::chrono::seconds(1)), 
    last_leader_msg_received_(std::chrono::steady_clock::now() - std::chrono::seconds(1)), 
    client_addresses_(client_addresses),
    in_election_(true),
    shard_map_(shard_map),
    num_clusters_(num_clusters)
    {
        
        for (int i = 1; i <= 9000; ++i) {
            accounts_[i] = 10.0;
        }
        

        for (const auto& [id, address] : client_addresses_) {
            grpc::ChannelArguments args;
            args.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, 10000); // or higher if needed
            auto channel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
            client_stubs_[id] = paxos::ClientService::NewStub(channel);
        }
        LOG << "Post cli stub gen" << std::endl;
        
        int n = peer_addresses_.size();
        divisor_ = n / num_clusters_;
        int low =static_cast<int>((node_id_-1) / divisor_)* divisor_ + 1;
        int high = low + divisor_ -1 ;
        intra_c_nid_range_ = {low, high};
        cluster_id_ = (node_id_ -1) / divisor_ + 1;
        f_ =  (divisor_ - 1) / 2 ;

        LOG << "Node " << node_id_ << " is within the cluster of nodes: " << intra_c_nid_range_.first << " to " << intra_c_nid_range_.second << std::endl;
        
        for (int i = 1; i <= num_clusters_ ; ++i) {
            cluster_leaders_[i] = divisor_ * (i - 1) + 1;
            LOG << "Cluster " << i << " leader is Node " << cluster_leaders_[i] << std::endl;
        }
        InitializeDatabase();
        InitDB();
        
    }

void PaxosNode::Run() {
    // Build gRPC server
    grpc::ServerBuilder builder;
    LOG << address_ << std::endl;
    LOG << "Binding to: [" << address_ << "]" << std::endl;
    int port = 0;
    builder.AddListeningPort(address_, grpc::InsecureServerCredentials(), &port);
    LOG << "AddListeningPort returned: " << port << std::endl;
    LOG << std::flush;
    LOG << "11" << std::endl;
    builder.RegisterService(&service_);
    cq_ = builder.AddCompletionQueue(); //server side cq
    LOG << "22" << std::endl;
    server_ = builder.BuildAndStart();


    LOG << "Node " << node_id_ << " running at " << address_ << std::endl;

    // initialize peer_stubs_ and do include yourself
    for (const auto& [id, address] : peer_addresses_) {
        peer_stubs_[id] = paxos::Paxos::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    }
    LOG << "Node " << node_id_ << " initialized stubs for all peers." << std::endl;


    // Start election timer (if backup)
    //if (role_ == Role::BACKUP) {
    //    StartElectionTimer();
    //}

    // Create and start the initial server side CallData objects, start multiple avoid not having one ready and messages being lost
    int init_handlers = 3;
    for (int i = 0; i < init_handlers; ++i) {
        new PrepareCallData(&service_, cq_.get(), this);
        new AcceptCallData(&service_, cq_.get(), this);
        new CommitCallData(&service_, cq_.get(), this);
        new NewViewCallData(&service_, cq_.get(), this);
        new SendClientRequestCallData(&service_, cq_.get(), this);
        new ReceiveAcceptAckCallData(&service_, cq_.get(), this); 
        new AliveUpdateCallData(&service_, cq_.get(), this);
        new GetNodeInfoCallData(&service_, cq_.get(), this);
        new MoveOnCallData(&service_, cq_.get(), this);
        new TwoPCCallData(&service_, cq_.get(), this);
    }

    LOG<<"All Handlers initialized" << std::endl;
    
    // start server side event loop threads
    int numThreads = 3 ; // pool 
    std::vector<std::thread> threads;
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this] {  
            this->HandleRpcs(); 
        });
    }

    // Start a separate thread to poll the completion queue for client-side calls   
    client_cq_ = std::make_unique<grpc::CompletionQueue>();
    client_cq_running_.store(true);
    client_cq_thread_ = std::thread([this]() { this->PollClientCompletionQueue(); });
    client_cq_thread_.detach();  // independent, runs forever

    // start the thread that checks for commit rety by the coordinator leader
    // retry_thread_ = LaunchTransactionRetryThread();

    // Optionally join threads if you want the main thread to wait
    for (auto& t : threads) {
        t.join();
    }

    // stop on node shutdown
    StopElectionTimer();  
    client_cq_running_.store(false); // stop client CQ polling
    server_->Shutdown(); 
    client_cq_->Shutdown(); 
    LOG << "Node " << node_id_ << " shutting down." << std::endl;
}

// Server Side PaxosNode Polling Queue
void PaxosNode::HandleRpcs() {
    void* tag;
    bool ok;
    while (cq_->Next(&tag, &ok)) { //blocks the calling thread until event occurs
        if (ok) {
            static_cast<CallData*>(tag)->Proceed();
        }
    }
    LOG << "Node " << node_id_ << " completion queue polling thread exiting." << std::endl;
}

// Poll function for client-side asyc calls
void PaxosNode::PollClientCompletionQueue() {
    void* tag;
    bool ok;
    while (client_cq_running_.load()) {
        if (client_cq_->Next(&tag, &ok)) {
            if (!tag) continue;
            auto* call = static_cast<AsyncClientCallBase*>(tag);
            call->OnComplete(ok);
        }
    }
    LOG << "Node " << node_id_ << " client completion queue polling thread exiting." << std::endl;
}


void PaxosNode::HandleClientRequest(const paxos::ClientRequest& request, SendClientRequestCallData* call_data) {
    if (!alive_.load()) {
        LOG << "Node " << node_id_ << " is dead. Ignoring client request." << std::endl;

        paxos::TransactionAck reply;
        call_data->Respond(reply); // call object needs to be finished
        return;
    }

    {
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        print_log_ += "<REQUEST, (" + request.from_account() + ", " + request.to_account() + ", " + std::to_string(request.amount()) + "), " + std::to_string(request.timestamp()) + ", " + request.from_account() + "> \n";
    }

    //new - force election on first node for each
    if (current_ballot_.node_id % divisor_ == 1 && current_ballot_.counter == 0 && first_txn_received_.load()) {
        LOG << "Node " << node_id_ << " forcing election on first request. this was ts=" << request.timestamp() << std::endl;
        first_txn_received_ = false;
        {
            std::lock_guard<std::mutex> lock(queued_mutex_);
            queued_client_requests_.push_back({request, call_data});
        }
        StartElectionTimer();
        return;
    }
    else {
        LOG << "Node " << node_id_ << " " << current_ballot_.counter << " " << first_txn_received_.load() << std::endl;
    }

    if (in_election_ || (leader_id_ == -1 && role_ == Role::BACKUP)) {
        LOG << "Node " << node_id_ << " is in election or no leader known. Queuing client request." << std::endl;
        {
            std::lock_guard<std::mutex> lock(queued_mutex_);
            queued_client_requests_.push_back({request, call_data});
        }
        return;
    }
    
    ProcessClientRequest(request, call_data);
}

void PaxosNode::ProcessClientRequest(const paxos::ClientRequest& request, SendClientRequestCallData* call_data) {
    LOG << "Node " << node_id_ << " received ClientRequest from client "
              << request.from_account() << " for amount " << request.amount()
              << " from account " << request.from_account()
              << " to account " << request.to_account() << std::endl;

    if (request.from_account()==""){
        LOG << "[ERROR] Invalid client request with empty from_account!" << std::endl;
        paxos::TransactionAck reply;
        call_data->Respond(reply); // call object needs to be finished
        return;
    }

    // if not the leader, forward asynchronously
    if (role_ != Role::LEADER) {
        if (leader_id_ == -1) {
            std::cerr << "[ERROR]Problem: No leader known, cannot forward request!" << std::endl;
            return;
        }

        LOG << "Node " << node_id_ << " not leader - forwarding request to leader node "
                  << leader_id_ << std::endl;

        if (leader_id_==node_id_){
            std::cerr << "Error: leader_id_ equals node_id_ but role is not LEADER!" << std::endl;
            std::cerr << "Rare case when the initial election timer has 2 nodes which time out exactly at the same time to the millisecond" << std::endl;
            std::abort();
        }
        new AsyncCall(peer_stubs_[leader_id_], client_cq_.get(), request, nullptr);

        paxos::TransactionAck reply;
        call_data->Respond(reply);

        return;
    }

    // duplicate completed request check: Todo - process for sending back needs to be through RPC not reply
    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        auto it = last_reply_per_client_.find(request.from_account()); //TODO
        if (it != last_reply_per_client_.end() && it->second.timestamp() == request.timestamp()) { // new
            
            LOG << "timestamp match for client " << request.from_account() << ": " << request.timestamp() << std::endl;
            // kill call data
            std::string from_acct = request.from_account(); // capture before Respond() may free call_data
            paxos::TransactionAck acks; // does not need to be filled out
            call_data->Respond(acks);

            // auto* call = new AsyncClientReplyCallData(client_stubs_[request.from_account()], client_cq_.get(), it->second);

            LOG << "Node " << node_id_ << " detected duplicate client request from client "
                      << from_acct << " - responding with cached reply." << std::endl;
            //*reply = it->second;
            return;
        }
    }

    // check if we have intra-shard txn or not
    bool read_only = request.read_only();
    bool participant = (request.two_pc_msg() == "PREPARE");
    bool intra_shard = false;

    if (read_only) {
        
        // send read only reply
        paxos::ClientReply reply;
        reply.mutable_ballot()->set_counter(current_ballot_.counter);
        reply.mutable_ballot()->set_node_id(current_ballot_.node_id);
        reply.set_client_id(request.from_account());
        reply.set_timestamp(request.timestamp());
        reply.set_success(true);
        reply.set_from_this_term(true);
        reply.set_read_only_balance(accounts_[std::stoi(request.from_account())]);
        
        LOG << "Node " << node_id_ << " processing read-only request for account "
                  << request.from_account() << " with balance " << accounts_[std::stoi(request.from_account())] << std::endl;

                  
        {
            std::lock_guard<std::mutex> lock(client_mutex_);

            if (handled_ro_requests_.find(request.timestamp()) == handled_ro_requests_.end()) {
                LOG << "Node " << node_id_ << " recording read-only request from client "
                          << request.from_account() << std::endl;
                last_reply_per_client_[request.from_account()] = reply;
                handled_ro_requests_.insert(request.timestamp());
            }
            else {
                LOG << "Node " << node_id_ << " detected duplicate read-only request from client "
                          << request.from_account() << " - ignoring." << std::endl;
                paxos::TransactionAck ack;
                call_data->Respond(ack); // clean up the call data
                return;
            }

        }

        auto* call = new AsyncClientReplyCallData(client_stubs_["A"], client_cq_.get(), reply);

        paxos::TransactionAck ack;
        call_data->Respond(ack); // clean up the call data
        return;
    }

    int s = std::stoi(request.from_account());
    int r = std::stoi(request.to_account());
    bool participant_abort = false;
    if (!participant) {

        if (shard_map_[s] == shard_map_[r]) {
            intra_shard = true;
        }


        // try to aquire locks
        if (balance_locks_[std::stoi(request.from_account())].try_lock()) {
            LOG << "Node " << node_id_ << " acquired lock for from_account "
                    << request.from_account() << std::endl;
        }
        else{
            LOG << "Node " << node_id_ << " LEADER could not acquire lock for from_account "
                    << request.from_account() << " - rejecting request." << std::endl;
            paxos::TransactionAck reply;
            call_data->Respond(reply);
            
            // do I reply to client, or wait for a broadcast?

            return;
        }

        if (intra_shard) {
            if (balance_locks_[std::stoi(request.to_account())].try_lock()) {
                LOG << "Node " << node_id_ << " acquired lock for to_account "
                        << request.to_account() << std::endl;
            }
            else{
                LOG << "Node " << node_id_ << " INTRA-SHARD could not acquire lock for to_account "
                        << request.to_account() << " - rejecting request and releasing prior lock." << std::endl;
                
                balance_locks_[std::stoi(request.from_account())].unlock();
                paxos::TransactionAck reply;
                call_data->Respond(reply);
                return;
            }
        }


        // send to leader of partcipant cluster:
        if (!intra_shard && participant == false) {
            LOG << "Node " << node_id_ << " sending 2pc prepare to participant cluster leader." << std::endl;

            int participant_cluster_id_ = shard_map_[r];
            LOG << "Node " << node_id_ << " identified part cluster as " << participant_cluster_id_ << " for to_account " << request.to_account() << std::endl;
            paxos::ClientRequest twopc_request;
            twopc_request.CopyFrom(request);
            twopc_request.set_two_pc_msg("PREPARE");
            twopc_request.set_from_node(node_id_);

            paxos::TwoPCMsg msg_2pc;
            msg_2pc.mutable_m()->CopyFrom(twopc_request);

            in_flight_two_pc_prepares_[request.timestamp()].msg = msg_2pc;
            in_flight_two_pc_prepares_[request.timestamp()].last_send = std::chrono::steady_clock::now();
            in_flight_two_pc_prepares_[request.timestamp()].sent = true;

            LOG << "Node " << node_id_ << " created TwoPCMsg for timestamp " << request.timestamp() << " to account "  << msg_2pc.m().to_account() << std::endl;
            new AsyncCall(peer_stubs_[cluster_leaders_[participant_cluster_id_]], client_cq_.get(), twopc_request, nullptr);
        }
    }
    else {
        // set new leader
        if (request.two_pc_msg() == "PREPARE" && request.from_node() != cluster_leaders_[shard_map_[s]]) {
            cluster_leaders_[shard_map_[s]] = request.from_node();
            LOG << "Node " << node_id_ << " updated cluster leader for cluster "
                      << shard_map_[s] << " to Node " << request.from_node() << std::endl;
        }
        if (balance_locks_[r].try_lock()) {
            LOG << "Node " << node_id_ << " acquired lock for to_account "
                    << request.to_account() << std::endl;
        }
        else{
            LOG << "Node " << node_id_ << " PARTICIPANT could not acquire lock for to_account "
                    << request.to_account() << " - rejecting request." << std::endl;
            
            participant_abort = true;
        }
    }




    // create accept message
    paxos::AcceptedEntry accept_msg;
    accept_msg.mutable_request()->CopyFrom(request);
    accept_msg.mutable_ballot()->set_counter(current_ballot_.counter);
    accept_msg.mutable_ballot()->set_node_id(current_ballot_.node_id);
    accept_msg.set_node_id(node_id_);

    if (participant && participant_abort) {
        accept_msg.set_two_pc_status("A");
    }
    else if (participant && !participant_abort) {
        accept_msg.set_two_pc_status("P");
        accept_msg.mutable_m()->CopyFrom(request); // look for this !!!!!!!
    }
    else if (!participant && intra_shard == false) {
        accept_msg.set_two_pc_status("COORD");
        accept_msg.mutable_m()->CopyFrom(request);
    }

    // check if request was received already and is pending 
    // case of a client broadcasting to multiple nodes and backups forwarding 
    int seqnum;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);

        if (pending_or_completed_ts_.find(request.timestamp()) != pending_or_completed_ts_.end()) {
            LOG << "Node " << node_id_ << " found existing pending request with timestamp "
                      << request.timestamp() << " - ignoring duplicate." << std::endl;
            paxos::TransactionAck reply;
            call_data->Respond(reply); // clean up the call data
            return;
        }
      
        seqnum = ++last_seqnum_;
        accept_msg.set_seqnum(seqnum);

        // log locally as pending and save call pointer
        std::lock_guard<std::mutex> lock2(quorum_mutex_);
        pending_or_completed_ts_.insert(request.timestamp());
        pending_entries_[seqnum] = accept_msg; // for future commit on leader end
        pending_client_calls_[seqnum] = call_data; // save call pointer to respond later TODO// FIX!!!!
        accepted_count_[seqnum][current_ballot_.counter].insert(node_id_);
    }


    //Log As Acceted and Broadcast Accept messages
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        accept_log_[accept_msg.seqnum()] = accept_msg;
    }
    {
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        print_log_ += "<ACCEPTED, (" + std::to_string(accept_msg.ballot().counter()) + ", " + std::to_string(accept_msg.ballot().node_id()) + "), " + std::to_string(seqnum) + ", (" + accept_msg.request().from_account() + ", " + accept_msg.request().to_account() + ", " + std::to_string(accept_msg.request().amount()) + "), " + std::to_string(node_id_) + "> \n";
    }

    // save digest (timestampt atm) to seqnum map. Only used for 2pc
    digest_to_seqnum_[request.timestamp()] = seqnum;

    LOG << "Node " << node_id_ << " broadcasting Accept for seqnum "
              << seqnum << " with ballot " << current_ballot_.ToString() << std::endl;

    for (auto& [peer_id, stub] : peer_stubs_) {
        if (peer_id == node_id_) continue; 
        if (peer_id < intra_c_nid_range_.first || peer_id > intra_c_nid_range_.second) {
            continue; 
        }
        new AcceptClientCallData(stub, client_cq_.get(), this, accept_msg);
    }

}


void PaxosNode::HandlePrepare(const paxos::PrepareRequest& request, PrepareCallData* call_data) {

    if (!alive_.load()) {return;} 

    {
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        print_log_ += "<PREPARE, (" + std::to_string(request.ballot().counter()) + "," + std::to_string(request.ballot().node_id()) + ")> \n";
    }

    Ballot incoming = {request.ballot().counter(), request.ballot().node_id()};
    bool should_demote = false;
    {
        std::lock_guard<std::mutex> lock(election_mutex_);

        LOG << "Node " << node_id_ << " received Prepare from Node " 
                  << incoming.node_id << " with ballot (" << incoming.counter 
                  << "," << incoming.node_id << ")" << std::endl;

        auto now = std::chrono::steady_clock::now();
        last_prepare_received_ = now;  // update timestamp
        bool timer_expired = (now - last_leader_msg_received_) >= prepare_cooldown_;
        
        // TODO - fix the logic with highest_promised_ballot_ and current_ballot_
        // Accept only if incoming ballot is higher than any seen (look at both counter and node_id for tie-break)
        if (incoming > highest_promised_ballot_){
                
            in_election_ = true; 

            if (role_ == Role::LEADER) {
                LOG << "Node " << node_id_ << " stepping down from LEADER to BACKUP due to Prepare from Node " 
                        << incoming.node_id << " with higher ballot " << incoming.ToString() << std::endl;
                should_demote = true;
                DemoteToBackup();
            }

            highest_promised_ballot_ = incoming; // update highest seen (Fix was made here)
            pending_promise_call_ = call_data; // save call pointer with highest ballot
            if (timer_expired) {

                LOG << "Node " << node_id_ << " will promise to Node " 
                        << incoming.node_id << " with ballot " << incoming.ToString()
                        << " (timer expired)" << std::endl;
                SendPromise();
            }
            else{
                LOG << "Node " << node_id_ << " has to wait to promise to Node " 
                        << incoming.node_id << " with ballot " << incoming.ToString()
                        << " (timer has not expired)" << std::endl;
                
            }
            
        } else {

            // Immefiately reject, no ened to wait for timeout
            paxos::PromiseResponse reply;
            reply.mutable_ballot()->set_counter(current_ballot_.counter);
            reply.mutable_ballot()->set_node_id(current_ballot_.node_id);
            reply.set_node_id(node_id_);
            reply.set_promised(false);

            call_data->Respond(reply); // immediate rejection
            pending_promise_call_ = nullptr; // clear any pending promise to avoid danglin p
            
            LOG << "Node " << node_id_ << " rejecting Prepare from Node " 
                    << incoming.node_id << " with ballot " << incoming.ToString()
                    << " (current ballot is " << current_ballot_.ToString() << ")" << std::endl;

        }
    } 
    if (should_demote) {
        StartElectionTimer();
    }

}

void PaxosNode::SendPromise() {

    if (!alive_.load()) {return;}

    //lock held from caller
    if (pending_promise_call_ == nullptr) {
        std::cerr << "Error: No pending promise reply to send!" << std::endl;
        return;
    }; 
    paxos::PromiseResponse promise_reply;
    promise_reply.mutable_ballot()->set_counter(highest_promised_ballot_.counter);
    promise_reply.mutable_ballot()->set_node_id(highest_promised_ballot_.node_id);
    promise_reply.set_promised(true);
    promise_reply.set_node_id(node_id_);
    
    for (const auto& [seq, entry] : accept_log_) {
        *promise_reply.add_accept_log() = entry;  // send back all accepted entries //TODO check this
    }

    // lock is held from parent call so setting to null ptr is safe here
    pending_promise_call_->Respond(promise_reply); //calls finish sequence in call_data
    pending_promise_call_ = nullptr; // clear after sending

    LOG << "Node " << node_id_ << " promising ballot " 
            << highest_promised_ballot_.ToString() << std::endl;
}

void PaxosNode::HandlePromise(const paxos::PromiseResponse& reply) {
    if (!alive_.load()) return; 

    {
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        print_log_ += "<ACK, (" + std::to_string(reply.ballot().counter()) + ", " + std::to_string(reply.ballot().node_id()) + "), " + PaxosNode::AcceptLogToString(accept_log_) + "> \n";
    }

    if (!reply.promised()) {
        LOG << "Node " << node_id_ << " received rejected Promise from Node " << reply.node_id()
                  << " with ballot (" << reply.ballot().counter() << "," << reply.ballot().node_id() << ")" << std::endl;
        return; // ignore rejections
    }
    LOG << "Node " << node_id_ << " received Promise from Node " << reply.node_id()
              << " with ballot (" << reply.ballot().counter() << "," << reply.ballot().node_id() << ")" << std::endl;

    bool quorum_reached = false;
    Ballot quorum_ballot;
    {
        std::lock_guard<std::mutex> lock(election_mutex_);

        // Ignore replies if not in election
        if (role_ == Role::LEADER || !waiting_for_promises_) {
            LOG << "Node " << node_id_ << " ignoring Promise (not in election or is leader)." << std::endl;
            return;
        }

        int sender_id = reply.node_id();
        if (promises_received_.count(sender_id)) return; // already counted

        // Track the promise
        promises_received_.insert(sender_id);
        received_promise_logs_[sender_id].clear();
        for (const auto& entry : reply.accept_log()) {
            received_promise_logs_[sender_id].push_back(entry);
        }

        // Check if quorum reached
        int quorum_size = f_ + 1;
        if (promises_received_.size() >= quorum_size) {
            quorum_reached = true;
            quorum_ballot = current_ballot_; // ballot used for this PREPARE
            LOG << "Node " << node_id_ 
                    << " received quorum of promises, becoming leader with ballot "
                    << quorum_ballot.ToString() << std::endl;

            waiting_for_promises_ = false;
        }
    }
    if (quorum_reached) {
        LOG << "node " << node_id_ << " Starts sequence of fillmissingnoop -> become_leader -> sendNV" << std::endl;
        MergeAcceptLogsFromPromises(); 
        FillMissingNoOps();  
        BecomeLeader(quorum_ballot);  
        SendNewView();  
        InformClustersOfElection();
        OnElectionComplete(); // process queued client requests
       
    }
    LOG << "Node " << node_id_ << " finished no-op processing, becomeing leader and sending new view" << std::endl;
}

void PaxosNode::MergeAcceptLogsFromPromises() {
    std::lock_guard<std::mutex> lock(election_mutex_);

    LOG << "Node " << node_id_ << " merging accept logs from promises..." << std::endl;

    int max_seqnum = accept_log_.empty() ? 0 : accept_log_.rbegin()->first;


    // A 2PC entry is "decided" once its outcome is known -- phase2_result set, or a phase-2
    // two_pc_status. A decided entry must never be replaced by a phase-1 (COORD/P) copy, even a
    // higher-ballot one: the C_COORD phase-2 is a value chosen at its ballot, so regressing it to
    // COORD un-decides a committed txn and its balance change is later dropped on demotion (the
    // run191 divergence -- node 2 reads 10 vs 9). Decidedness therefore dominates ballot here.
    auto is_decided = [](const paxos::AcceptedEntry& e) {
        const std::string& s = e.two_pc_status();
        return !e.phase2_result().empty() ||
               s == "C_COORD" || s == "C" || s == "A_COORD" || s == "A_PART_COMMIT";
    };

    for (const auto& [node_id, log_entries] : received_promise_logs_) {
        for (const auto& accepted : log_entries) {
            int seq = accepted.seqnum();
            auto it = accept_log_.find(seq);

            bool higher_ballot =
                (it != accept_log_.end()) &&
                (accepted.ballot().counter() > it->second.ballot().counter() ||
                 (accepted.ballot().counter() == it->second.ballot().counter() &&
                  accepted.ballot().node_id() > it->second.ballot().node_id()));
            bool cur_decided = (it != accept_log_.end()) && is_decided(it->second);
            bool inc_decided = is_decided(accepted);

            if (it == accept_log_.end()) {
                accept_log_[seq] = accepted;
            } else if (inc_decided && !cur_decided) {
                accept_log_[seq] = accepted;        // decided outcome wins over phase-1, any ballot
            } else if (cur_decided && !inc_decided) {
                // keep the decided entry -- do NOT let a phase-1 copy regress it
            } else if (higher_ballot) {
                accept_log_[seq] = accepted;        // same decidedness -> higher ballot wins
            }

            // Carry a known outcome onto the winner even if the ballot-winner lacked it, and
            // mirror a phase-2 two_pc_status into phase2_result (the demotion reconcile reads
            // phase2_result, not two_pc_status).
            auto& winner = accept_log_[seq];
            if (winner.phase2_result().empty()) {
                if (!accepted.phase2_result().empty()) {
                    winner.set_phase2_result(accepted.phase2_result());
                } else {
                    const std::string& ws = winner.two_pc_status();
                    if (ws == "C_COORD" || ws == "C" || ws == "A_COORD" || ws == "A_PART_COMMIT")
                        winner.set_phase2_result(ws);
                }
            }

            // Track the highest sequence number seen
            if (seq > max_seqnum) {
                max_seqnum = seq;
            }
        }
    }


    // update last_seqnum_ so the new leader knows next seqnum to use
    last_seqnum_ = max_seqnum;
    received_promise_logs_.clear();
    LOG << "Node " << node_id_ << " updated last_seqnum_ to " << last_seqnum_ << std::endl;

    // Seed the dedup set from the inherited log: pending_or_completed_ts_ is only filled on the
    // propose path, so a new leader that inherited a command (never proposed it) re-proposes it
    // under a fresh seqnum on client retry -- the double-commit. Skip NO-OPs, and skip in-flight
    // 2PCs (COORD/P, no phase2_result) since those are completed by re-driving the retry (deduping
    // them would block completion -- the q8 regression).
    {
        std::lock_guard<std::mutex> plock(pending_mutex_);
        for (const auto& [seq, entry] : accept_log_) {
            const auto& req = entry.request();
            if (req.client_id() == "NO-OP") continue;

            // Rebuild the ts->seqnum map for inherited commands. Like pending_or_completed_ts_,
            // digest_to_seqnum_ is otherwise only populated on the propose path (line ~454), so a
            // leader that inherited an in-flight 2PC via merge has no ts->seqnum entry. Then
            // HandlePreparedAndAborted's `executed_entries_.find(digest_to_seqnum_[ts])` looks up 0,
            // misses, and drops the participant's PREPARED -> the cross-shard 2PC hangs after a
            // coordinator leader change. Seeding it lets the new leader recognize phase-1 as done
            // and drive phase-2.
            digest_to_seqnum_[req.timestamp()] = seq;

            const std::string& p1 = entry.two_pc_status();
            if ((p1 == "COORD" || p1 == "P") && entry.phase2_result().empty()) continue; // in-flight 2PC
            pending_or_completed_ts_.insert(req.timestamp());
        }
    }

}

void PaxosNode::FillMissingNoOps() {
    std::lock_guard<std::mutex> lock(log_mutex_);

    int max_seq = last_seqnum_.load(); // highes sequence number seen so far (from MergeAcceptLogsFromPromises)

    // Fill in missing sequence numbers with No-Op entries
    for (int seq = 1; seq <= max_seq; ++seq) {
        if (accept_log_.find(seq) == accept_log_.end()) {
            paxos::AcceptedEntry noop_entry;
            noop_entry.mutable_ballot()->set_counter(current_ballot_.counter);
            noop_entry.mutable_ballot()->set_node_id(current_ballot_.node_id);
            noop_entry.set_seqnum(seq);
            noop_entry.mutable_request()->set_client_id("NO-OP");
            noop_entry.mutable_request()->set_timestamp(0);
            noop_entry.mutable_request()->set_from_account("");
            noop_entry.mutable_request()->set_to_account("");
            noop_entry.mutable_request()->set_amount(0.0);
            noop_entry.set_node_id(node_id_);

            accept_log_[seq] = noop_entry;
            LOG << "Node " << node_id_ << " filled missing seqnum " << seq << " with No-Op entry." << std::endl;
        }
    }
    LOG << "Node " << node_id_ << " filled missing sequence numbers up to " << max_seq << " with No-Op entries if needed" << std::endl;
}

void PaxosNode::InformClustersOfElection() {
    paxos::AcceptedEntry entry;
    entry.set_new_leader(node_id_);

    for (auto& [peer_id, stub] : peer_stubs_) {
        if (peer_id == node_id_) continue; // skip self
        if (!(peer_id < intra_c_nid_range_.first || peer_id > intra_c_nid_range_.second)) {
            continue; // skip nodes inside cluster
        }

        new AcceptClientCallData(stub, client_cq_.get(), this, entry); // dummy
        
    }}

void PaxosNode::HandlePropose(const paxos::AcceptedEntry& request, paxos::Ack* reply) {
    if (!alive_.load()) return; 

    if (request.new_leader() != 0) {
        int leader_cluster_id = (request.new_leader()-1) / divisor_ + 1;
        cluster_leaders_[leader_cluster_id] = request.new_leader();
        LOG << "Node " << node_id_ << " updated cluster " << leader_cluster_id << " leader to Node " << request.new_leader() << std::endl;
        return;
    }


    {
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        print_log_ += "<ACCEPT, (" + std::to_string(request.ballot().counter()) + "," + std::to_string(request.ballot().node_id()) + "), " + std::to_string(request.seqnum()) + " (" + request.request().from_account() + ", " + request.request().to_account() + ", " + std::to_string(request.request().amount()) + ")> \n";
    }


    reply->mutable_ballot()->set_counter(request.ballot().counter());
    reply->mutable_ballot()->set_node_id(request.ballot().node_id());
    

    if (role_ != Role::BACKUP) {
        if (request.ballot().counter() > current_ballot_.counter ||
            (request.ballot().counter() == current_ballot_.counter &&
             request.ballot().node_id() > current_ballot_.node_id)) {
            LOG << "[Leader] Node " << node_id_ << " stepping down from LEADER to BACKUP due to Accept from Node " 
                      << request.node_id() << " with higher ballot (" 
                      << request.ballot().counter() << "," << request.ballot().node_id() << ")" << std::endl;
            
            {
                std::lock_guard<std::mutex> lock(election_mutex_);
                DemoteToBackup();
                SetNewLeader(request.ballot().node_id(), request.ballot().counter());
            }

            StartElectionTimer();
        }
    }

    LOG << "[Backup] Node " << node_id_ 
              << " received Accept for seqnum " << request.seqnum()
              << " with ballot (" << request.ballot().counter()
              << "," << request.ballot().node_id() << ")" << std::endl;

    // Lock while checking/updating promised ballot and storing the request
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);

        // Reject if ballot is less than the highest promised
        if (request.ballot().counter() < highest_promised_ballot_.counter ||
            (request.ballot().counter() == highest_promised_ballot_.counter &&
             request.ballot().node_id() < highest_promised_ballot_.node_id)) {
            LOG << "[Backup] Node " << node_id_
                      << " rejecting Accept for seqnum " << request.seqnum()
                      << " due to lower ballot than highest promised ("
                      << highest_promised_ballot_.counter << ", "
                      << highest_promised_ballot_.node_id << ")" << std::endl;
            return; // do not store or ack
        }

        // Update highest promised ballot if this ballot is greater
        if (request.ballot().counter() > highest_promised_ballot_.counter ||
            (request.ballot().counter() == highest_promised_ballot_.counter &&
             request.ballot().node_id() > highest_promised_ballot_.node_id)) {

            highest_promised_ballot_.counter = request.ballot().counter();
            highest_promised_ballot_.node_id = request.ballot().node_id();

            // a node which has been awoken and made a backup may not know the leader if it joins in within running view
            SetNewLeader(request.ballot().node_id(), request.ballot().counter()); 
            
        }
        {
            std::lock_guard<std::mutex> lock(log_mutex_);
            if (accept_log_.find(request.seqnum()) == accept_log_.end()) {
                accept_log_[request.seqnum()] = request; 
            } 
            
        }
        
        // Store for future commit on backup end
        pending_entries_[request.seqnum()] = request;
    }

    ResetElectionTimer();

    SendAcceptAckToLeader(request);


}

void PaxosNode::SendAcceptAckToLeader(const paxos::AcceptedEntry& entry) {
    // only gets called for new entries of the view

    paxos::Ack ack;
    ack.mutable_ballot()->set_counter(entry.ballot().counter());
    ack.mutable_ballot()->set_node_id(entry.ballot().node_id());
    ack.set_seqnum(entry.seqnum());
    ack.set_node_id(node_id_); // sender ID

    std::string status = entry.two_pc_status();
    if (status == "C_COORD" || status == "C" || status == "A_COORD" || status == "A_PART_COMMIT") {
        ack.set_phase(2);
        ack.set_status(status);
    }
    else if (status == "COORD" || status == "P" || status == "A") {

        ack.set_status(status);
    }


    auto call = new AsyncAcceptAckCall(peer_stubs_[entry.node_id()], ack, client_cq_.get());

    LOG << "[backup Node " << node_id_ << " sent Accept Ack for seqnum "
              << entry.seqnum() << " to leader " << leader_id_ << std::endl;
}



void PaxosNode::CommitEntry(const paxos::Ack ack) {
    
    int seqnum = ack.seqnum();
    paxos::CommitEntry entry;

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);

        // Check if entry exists in pending_entries_
        auto it = pending_entries_.find(seqnum);
        if (it == pending_entries_.end()) {
            LOG << "[Leader] Node " << node_id_ << " commiting off of new-view accepts or just repeated commit for seqnum (like for 2pc commit) " << seqnum << std::endl;
            
            paxos::AcceptedEntry entry_to_copy_from;
            entry_to_copy_from = accept_log_[seqnum]; // should be present in accept log


            entry.mutable_ballot()->set_counter(entry_to_copy_from.ballot().counter());
            entry.mutable_ballot()->set_node_id(entry_to_copy_from.ballot().node_id());
            entry.set_seqnum(entry_to_copy_from.seqnum());
            entry.mutable_request()->CopyFrom(entry_to_copy_from.request());

            entry.set_two_pc_status(ack.status());
            entry.mutable_m()->CopyFrom(entry_to_copy_from.m()); // TODO

        }
        else{
            // Copy the entry to the committed map
            entry.mutable_ballot()->set_counter(it->second.ballot().counter());
            entry.mutable_ballot()->set_node_id(it->second.ballot().node_id());
            entry.set_seqnum(it->second.seqnum());
            entry.mutable_request()->CopyFrom(it->second.request());

            entry.set_two_pc_status(ack.status());
            LOG << "entry two pc status is " << entry.two_pc_status() << "for seqnum " << entry.seqnum() << std::endl;
        }



        // Insert if not already committed
        if (entries_to_commit_.count(seqnum) == 0) {
            entries_to_commit_[seqnum] = entry;
            LOG << "[Leader] Node " << node_id_ 
                      << " committed entry for seqnum " << seqnum << std::endl;
        }
    }

    LOG << "[Leader] Node " << node_id_ 
              << " entering into broadcasting Commits for seqnum " << seqnum << std::endl;
    BroadcastCommit(entry);

    if (last_executed_seq_ + 1 == seqnum) {
        LOG << "[Leader] Node " << node_id_ 
                  << " entering sequentially ... for seqnum " << seqnum << std::endl;
        SequentiallyExecuteCommittedEntries();
    }
    else if (ack.phase() == 2) {
        ExecuteRepeatedTwoPCEntry(entry);
    }
    else {
        LOG << "[Leader] Node " << node_id_ 
                  << " not executing committed entry for seqnum " << seqnum 
                  << " yet (last executed is " << last_executed_seq_ << ")" << std::endl;
    }
}

void PaxosNode::SequentiallyExecuteCommittedEntries() {

    // process in strict seq_num order and also isnt busy waiting
    while (true) {
        paxos::CommitEntry entry;
        std::string two_pc_Status;
        {
            LOG << "Node " << node_id_ 
                      << " top of while" << std::endl;
            std::lock_guard<std::mutex> lock(pending_mutex_);
            int next_seq = last_executed_seq_ + 1;
            auto it = entries_to_commit_.find(next_seq);
            if (it == entries_to_commit_.end()) {
                LOG << "Node " << node_id_ 
                          << " broke. Next seq to be executed would be seq.  " << next_seq << std::endl;

                break;
            }; // no next in-order entry

            entry.mutable_ballot()->set_counter(it->second.ballot().counter());
            entry.mutable_ballot()->set_node_id(it->second.ballot().node_id());
            entry.set_seqnum(it->second.seqnum());
            entry.mutable_request()->CopyFrom(it->second.request());
            two_pc_Status = it->second.two_pc_status();
            last_executed_seq_ = next_seq;
            LOG << "Node " << node_id_ 
                      << " updated last_executed_seq_ to " << last_executed_seq_ << std::endl;
        }

        // outside lock
        bool success = ExecuteTransaction(entry.request(), entry.seqnum(), two_pc_Status);

        if (role_ == Role::LEADER){
            
            // Handle client reply if present
            SendClientRequestCallData* call_data = nullptr;
            
            // Should be before executing - then if reply fails to get to client, retry is performed (idempotent)
            // Also if the execution is off of the new-view accept log, the call_data will not be present and this is handled
            
            if (max_seqnum_in_new_view_ < entry.seqnum()){
                {
                    std::lock_guard<std::mutex> lock(pending_mutex_);
                    auto it = pending_client_calls_.find(entry.seqnum());
                    if (it != pending_client_calls_.end()) {
                        call_data = it->second;
                        pending_client_calls_.erase(it);
                        pending_entries_.erase(entry.seqnum()); 
                    }
                }
            }
            else {
                LOG << "[Leader] Node " << node_id_ << " not looking for call data for seqnum " << entry.seqnum() << " as it is from new-view accept log" << std::endl;
            
            }

            if (call_data && two_pc_Status != "A" && two_pc_Status != "P" && two_pc_Status != "COORD") {
                SendClientReply(call_data, entry.seqnum(), success);
            } 
            else if (two_pc_Status == "A" || two_pc_Status == "P") {
                LOG << "[Leader] Node " << node_id_ << " sending reply for seqnum " 
                        << entry.seqnum() << " due to 2PC status " << two_pc_Status << std::endl;

                SendReplyToCoordLeader(call_data, entry.seqnum(), two_pc_Status);
            }
            else if (two_pc_Status == "COORD") {
                LOG << "[Leader] Node " << node_id_ << " executed " 
                        << entry.seqnum() << " as coordinator of 2PC" << std::endl;

                paxos::TwoPCMsg cached_msg;       // local copy (outside lock)
                bool has_msg = false;      // flag to track if we found something

                if (call_data != nullptr) {
                    paxos::TransactionAck ack;
                    call_data->Respond(ack);
                }

                {
                    // LOCK SCOPE — only container operations
                    std::lock_guard<std::mutex> lock(executed_entries_mutex_);

                    auto it = prepare_and_aborted_queue_.find(entry.request().timestamp());
                    if (it != prepare_and_aborted_queue_.end()) {
                        LOG << "Node " << node_id_ 
                                << ": found queued PrepareAndAbort req, ts=" 
                                << entry.request().timestamp() << std::endl;

                        cached_msg = it->second;  // copy message OUT of the map
                        has_msg = true;

                        prepare_and_aborted_queue_.erase(it); // erase while locked
                    }
                } // lock is released here

                // Call heavy logic OUTSIDE the lock
                if (has_msg) {
                    LOG << "Node " << node_id_ 
                            << ": calling StartSecondPhase for queued ts="
                            << entry.request().timestamp() << std::endl;

                    StartSecondPhase(cached_msg);
                }
            }

            else if (entry.request().client_id() != "NO-OP") {
                //SendClientReply(nullptr, entry.seqnum(), success); // no call data, but still send reply
                std::cerr << "[Leader] WARNING: No CallData for seqnum " 
                        << entry.seqnum() << " client " 
                        << entry.request().client_id() << std::endl;
            }
            else if (call_data != nullptr) {
                LOG << "intra-shard normal sendreply" << std::endl;
                SendClientReply(call_data, entry.seqnum(), success);
            }
        }
        else { // backup

            if (in_new_view_ && max_seqnum_in_new_view_ == entry.seqnum() && newview_call_data_!= nullptr) {
                LOG << "[Backup] Node " << node_id_ << " about to kill new-view cd. just executed seq: " << entry.seqnum() << std::endl;
                paxos::Ack ack;
                //newview_call_data_->Respond(ack); // set to null ptr in calld data
                //in_new_view_ = false; // done processing new-view entries
                LOG << "[Backup] Node " << node_id_ << " completed executing all new-view committed entries." << std::endl;
            } else{
                LOG << "[Backup] Node " << node_id_ << " is newview_call_data_ nullptr for seq: " << entry.seqnum()<< " =" << (newview_call_data_==nullptr) << std::endl;
            }
            {
                // clear pending 
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_entries_.erase(entry.seqnum()); 
            }
        }
    }
}

// Execute the client transaction
bool PaxosNode::ExecuteTransaction(const paxos::ClientRequest& request, int seqnum, std::string two_pc_status) { // last is default
    // Example: transaction = transfer from_account -> to_account, amount
    int from = std::stoi(request.from_account());
    int to = std::stoi(request.to_account());
    double amt = request.amount();

    bool intra_shard = false;
    int r = std::stoi(request.to_account());
    int s = std::stoi(request.from_account());
    

    if (shard_map_[r] == shard_map_[s]) {
        intra_shard = true;
    }


    // Check for NO-OP request
    if (request.client_id() == "NO-OP") return true;

    // Check sufficient balance
    if (accounts_[from] < amt) {
        LOG << "Node " << node_id_ << ": insufficient balance for " 
                  << from << " -> " << to << " amount " << amt << ", client" << from
                  << " only has " << accounts_[from] << std::endl;

        std::lock_guard<std::mutex> lock(executed_entries_mutex_);
        executed_entries_.insert(seqnum); // still executed even though failed
        if (intra_shard) {
            balance_locks_[from].unlock();
            balance_locks_[to].unlock();
        }
        return false;
    }

    if (two_pc_status == "A") {
        LOG << "Node " << node_id_ << ": executing aborted due to 2PC abort for seqnum " << seqnum << std::endl;
        //wal_[seqnum] = {accounts_[to], accounts_[to]}; // no change
        std::lock_guard<std::mutex> lock(executed_entries_mutex_);
        executed_entries_.insert(seqnum); // still executed even though aborted
        return false;
    }
 

    if (intra_shard) {
        accounts_[from] -= amt;
        accounts_[to] += amt;
        modified_accounts_.insert(from);
        modified_accounts_.insert(to);
        //UpdateBalance(from, amt, true);
        //UpdateBalance(to, amt, false);
        balance_locks_[from].unlock();
        balance_locks_[to].unlock();
    }
    else if (two_pc_status =="P") {
        {
            std::lock_guard<std::mutex> lock(wal_mutex_);
            wal_[seqnum] = {accounts_[to], accounts_[to] + amt};
            accounts_[to] += amt;
        }
        
        
        //UpdateBalance(to, amt, false);
        modified_accounts_.insert(to);
    }
    else if (two_pc_status == "COORD") {
        int og = accounts_[from];
        {
            std::lock_guard<std::mutex> lock(wal_mutex_);
            wal_[seqnum] = {accounts_[from], accounts_[from] - amt};

            accounts_[from] -= amt;
        }
        
        //UpdateBalance(from, amt, true);
        modified_accounts_.insert(from);
        LOG << "Node " << node_id_ << ": executed 2PC COORD transaction, seqnum:" << seqnum
                  << " " << from << " -> " << to << " amount " << amt 
                  << " original balance was " << og << ", new balance is " << accounts_[from] << std::endl;
    }
    else {
        LOG << "two_pc_status: " << two_pc_status << std::endl;
    }

    {
        std::lock_guard<std::mutex> lock(executed_entries_mutex_);
        executed_entries_.insert(seqnum); // TODO lock here?
    }


    LOG << "Node " << node_id_ << ": executed transaction, seqnum:" << seqnum
              << " " << from << " -> " << to << " amount " << amt << std::endl;

    return true;
}

// Broadcast a COMMIT message to all backup nodes
void PaxosNode::BroadcastCommit(const paxos::CommitEntry& entry) {
    for (auto& [peer_id, stub] : peer_stubs_) {
        if (peer_id == node_id_) continue; 
        if (peer_id < intra_c_nid_range_.first || peer_id > intra_c_nid_range_.second) {
            continue; // skip nodes outside cluster
        }
        LOG << "[Leader] Node " << node_id_ << " broadcasting Commit for seqnum "
                  << entry.seqnum() << " to Node " << peer_id << std::endl;
        CommitClientCallData* call = new CommitClientCallData(stub, client_cq_.get(), this, entry);
    }

    LOG << "Node " << node_id_ << ": broadcasted commit for seqnum " 
              << entry.seqnum() << std::endl;
}

// Send reply back to the original client
void PaxosNode::SendClientReply(SendClientRequestCallData* call_data, int seq_num, bool success) {
    
    if (!alive_.load()) return;
    
    const paxos::ClientRequest& request = call_data->GetRequest();

    if (request.client_id() == "NO-OP") return;

    paxos::ClientReply reply;
    reply.mutable_ballot()->set_counter(current_ballot_.counter);
    reply.mutable_ballot()->set_node_id(current_ballot_.node_id);
    reply.set_client_id(request.from_account());
    reply.set_timestamp(request.timestamp());
    reply.set_success(success);
    reply.set_from_this_term(seq_num > max_seqnum_in_new_view_);
    LOG << "set from this term to " << (seq_num > max_seqnum_in_new_view_) << std::endl;


    // Cache reply for exactly-once semantics
    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        last_reply_per_client_[request.from_account()] = reply;
    }


    LOG << "[Leader] Node " << node_id_ 
              << ": sending reply to client " << request.from_account()
              << " for seqnum=" << seq_num
              << " success=" << success << std::endl;
    

    new AsyncClientReplyCallData(client_stubs_["A"], client_cq_.get(), reply); // changed

    // kill teh call_data after responding to active request, if this cas eisnt hit it is a request from the newview
    if (seq_num > max_seqnum_in_new_view_ && call_data != nullptr) {
        paxos::TransactionAck ack;
        call_data->Respond(ack); // clean up the call data
        LOG << "[Leader] Node " << node_id_ << ": cleaned up new-view call data " << std::endl;
    } else {
        LOG << "[Leader] Node " << node_id_ << ": not cleaning up call data for seqnum " << seq_num << " as it is from new-view accept log" << std::endl;
    }
}

/**
 * How a follower handles a commit message from the leader
 */
void PaxosNode::HandleCommit(const paxos::CommitEntry& request, paxos::Ack* reply) {
    if (!alive_.load()) return;

    LOG << "[Backup] Node " << node_id_ 
              << " received Commit for seqnum " << request.seqnum()
              << " with ballot (" << request.ballot().counter()
              << "," << request.ballot().node_id() << ")" << std::endl;
    {
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        print_log_ += "<COMMIT, (" + std::to_string(request.ballot().counter()) + "," + std::to_string(request.ballot().node_id()) + "), " +  std::to_string(request.seqnum()) + " (" + request.request().from_account() + ", " + request.request().to_account() + ", " + std::to_string(request.request().amount()) + ")> \n";
    }

    // heard from leader
    if (role_ == Role::BACKUP) {
        ResetElectionTimer();
    }

    // Mark entry committed + execute if not already done
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        if (!entries_to_commit_.count(request.seqnum())) {

            entries_to_commit_[request.seqnum()] = request;
        }
    }

    if ((request.two_pc_status() == "C_COORD" || request.two_pc_status() == "C" || request.two_pc_status() == "A_COORD" || request.two_pc_status() == "A_PART_COMMIT")) {
        ExecuteRepeatedTwoPCEntry(request);
    }
    else if (last_executed_seq_ + 1 == request.seqnum() && executed_entries_.count(request.seqnum()) == 0) {
        LOG << "[Backup] Node " << node_id_ 
                  << " enterring sequentially ... for seqnum " << request.seqnum() << std::endl;
        SequentiallyExecuteCommittedEntries();
    } else {
        LOG << "[Backup] Node " << node_id_ 
                  << " cannot execute Commit for seqnum " << request.seqnum()
                  << " yet (last executed seqnum is " << last_executed_seq_ << ")" << std::endl;
    }
}

void PaxosNode::OnNewViewCallFinished(NewViewCallData* call_data) {
    std::lock_guard<std::mutex> lock(newview_cd_mutex_);
    if (newview_call_data_ == call_data) {
        newview_call_data_ = nullptr;
    }
}

// Atomically claim newview_call_data_ and Respond once. Multiple threads
// (election-timer thread, HandleSetAlive/kill, the CQ FINISH handler) can race;
// guarantees only one caller ever gets the live pointer, so Finish()/delete happens exactly once. 
// Respond() is done outside the lock so the CQ thread that deletes the object never contends on it.
void PaxosNode::RespondAndClearNewViewCallData() {
    NewViewCallData* cd = nullptr;
    {
        std::lock_guard<std::mutex> lock(newview_cd_mutex_);
        cd = newview_call_data_;
        newview_call_data_ = nullptr;
    }
    if (cd != nullptr) {
        cd->Respond(paxos::Ack());
    }
}


/** HHow receiving node handles new view */
void PaxosNode::HandleNewView(const paxos::NewViewRequest& request, NewViewCallData* call_data) {
    if (!alive_.load()) return;

    {
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        print_log_ += "<NEW-VIEW, (" + std::to_string(request.ballot().counter()) + "," + std::to_string(request.ballot().node_id()) + ")," + PaxosNode::AcceptLogToString(accept_log_) + "> \n";
        new_view_logs_.push_back(request);
    }

    Ballot leader_ballot = {request.ballot().counter(), request.ballot().node_id()};

    // heard from leader
    if (role_ == Role::BACKUP && alive_.load()) {
        ResetElectionTimer();
    }
        
    bool demote = false;
    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        // Sanity check: only backups should process NEW-VIEW
        if (role_ == Role::LEADER) {
            LOG << "[Check] Node " << node_id_ << " is LEADER but received a NEW-VIEW with ballot number " << leader_ballot.counter << " from node " << leader_ballot.node_id << std::endl;

            // If the incoming ballot is higher, step down to BACKUP
            // there is a possibility that 2 leaders can be elected (however unlikely), and this should resolve it
            if (leader_ballot > current_ballot_) { 

                LOG << "Node " << node_id_ << " stepping down from LEADER to BACKUP due to higher ballot in NEW-VIEW: " << leader_ballot.ToString() << " > " << current_ballot_.ToString() << std::endl;
                demote = true;

                // it is okay to call this while holding the election_mutex_ lock
                DemoteToBackup();
                SetNewLeader(request.ballot().node_id(), request.ballot().counter());
            }
            else{
                LOG << "Node " << node_id_ << " ignoring NEW-VIEW with lower ballot." << std::endl;
                
                
                call_data->Respond(paxos::Ack()); 
                return; // ignore if not higher
            }
        }
    }
    // restart electiontimer if demoted
    if (demote) {StartElectionTimer();}
    
    LOG << "Hit HandleNewView on Node " << node_id_ << " with NEW-VIEW from Node " << request.ballot().node_id() << " with ballot " << leader_ballot.ToString() << std::endl;
    // Adopt the leader's ballot for this backup
    if (role_ == Role::BACKUP) {
        // track leader -> NEeds to be fixed
        {
            std::lock_guard<std::mutex> lock(election_mutex_);

            SetNewLeader(request.ballot().node_id(), request.ballot().counter());
            if (leader_ballot < current_ballot_){
                LOG << "Warning: Node " << node_id_ << " received NEW-VIEW with lower ballot than its current ballot. This should not happen!" << std::endl;
            }

            waiting_for_promises_ = false; // no longer in election - TODO look for better palcement maybe in helper

        }

        OnElectionComplete();

        LOG << "Node " << node_id_ 
                    << " updated current_ballot_ and highest_promised_ballot_ to leader's ballot: "
                    << current_ballot_.ToString() << std::endl;
    }




    // Step 3: Process NEW-VIEW entries and replay to leader
    std::vector<paxos::AcceptedEntry> entries_to_send;
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        for (const auto& entry : request.accept_log()) {
            int seq = entry.seqnum();
            auto it = accept_log_.find(seq);
            if (it == accept_log_.end() || entry.ballot().counter() > it->second.ballot().counter()) {
                LOG << "Node " << node_id_ << " updating accept_log_ for seq " << seq << " from NEW-VIEW" << std::endl;
                accept_log_[seq] = entry;
            }

            // Collect entries to send outside the lock
            entries_to_send.push_back(entry);
        }
    }

    if (entries_to_send.empty()) {
        LOG << "Node " << node_id_ << " has no acc entries in new-view. Calling respond!" << std::endl;
        call_data->Respond(paxos::Ack()); 
    } else {
        {
            std::lock_guard<std::mutex> lock(newview_cd_mutex_);
            newview_call_data_ = call_data; // save to respond after sending acks
        }
        in_new_view_ = true; // indicate we are in new-view processing
        max_seqnum_in_new_view_ = entries_to_send.back().seqnum();
        LOG << "Node " << node_id_ << " processing NEW-VIEW with " << entries_to_send.size() << " entries, max seqnum " << max_seqnum_in_new_view_ << std::endl;
    }

    for (auto& entry : entries_to_send) {
        paxos::Ack ack; 
        ack.mutable_ballot()->set_counter(current_ballot_.counter); // TODO nade chabnge
        ack.mutable_ballot()->set_node_id(current_ballot_.node_id); // changed this
        ack.set_seqnum(entry.seqnum());
        ack.set_node_id(node_id_); // sender ID

        std::string status = entry.two_pc_status();
        if (status == "C_COORD" || status == "C" || status == "A_COORD" || status == "A_PART_COMMIT") {
            ack.set_phase(2);
            ack.set_status(status);
        }
        else if (status == "COORD" || status == "P" || status == "A") {

            ack.set_status(status);
        }

        LOG << "in HandleNewView, Node " << node_id_ << " just about to send accept to leader " << leader_id_ << " for seq " << entry.seqnum() << std::endl;

        new AsyncAcceptAckCall(peer_stubs_[leader_id_], ack, client_cq_.get()); 
    }


}


void PaxosNode::HandleAccept(const paxos::Ack& ack) { //, paxos::TransactionAck* reply
    
    if (!alive_.load()) return; 

    if (role_ != Role::LEADER) { // for accept acks send by demoted leader 
        LOG << "[IRGNORING] Node " << node_id_ << " not LEADER but received an Accept Ack from Node " << ack.node_id() << std::endl;
        return; // only leader processes accept acks
    }

    {
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        std::string from_acc = accept_log_[ack.seqnum()].request().from_account();
        std::string to_acc = accept_log_[ack.seqnum()].request().to_account();
        float amount = accept_log_[ack.seqnum()].request().amount();
        print_log_ += "<ACCEPTED, (" + std::to_string(ack.ballot().counter()) + ", " + std::to_string(ack.ballot().node_id()) + "), " + std::to_string(ack.seqnum()) + ", (" + from_acc + ", " + to_acc + ", " + std::to_string(amount) + "), " + std::to_string(ack.node_id()) + "> \n";
    }



    bool should_commit = false;
    LOG << "[Leader] Node " << node_id_ 
              << " received Accept from Node " << ack.node_id()
              << " for seqnum " << ack.seqnum()
              << " with ballot (" << ack.ballot().counter()
              << "," << ack.ballot().node_id() << ")"
              << " accepted=" << ack.accepted() << std::endl;
    {
        std::lock_guard<std::mutex> lock(quorum_mutex_);
        
        // 2pc commit case
        if (accept_log_.find(ack.seqnum()) != accept_log_.end() && (ack.phase() == 2) ) {
            
            accepted_count_two_pc_commit_[ack.seqnum()][ack.ballot().counter()].insert(ack.node_id());

            bool condition_q = (accepted_count_two_pc_commit_[ack.seqnum()][ack.ballot().counter()].size() == f_ + 1 ); // force only 1 commit
            if ((condition_q) || 
                (ack.seqnum() <= max_seqnum_in_new_view_ && condition_q)) {
                
                should_commit = true;
                LOG << "[Leader] Node " << node_id_ << " has enough acks (2pc) (" << accepted_count_two_pc_commit_[ack.seqnum()][ack.ballot().counter()].size() << " > " << f_ << ") to commit seqnum " << ack.seqnum() << " Ack from " << ack.node_id() << std::endl;

            }
            else {
                LOG << "[Leader] Node " << node_id_ << " does not have enough acks yet (2pc) to commit seqnum " << ack.seqnum() 
                        << " (have " << accepted_count_two_pc_commit_[ack.seqnum()][ack.ballot().counter()].size() 
                        << ", need " << (f_ + 1) << ")" << ". Ack from " << ack.node_id() << std::endl;
            }

        }
        else if (ack.phase() != 2) { // normal case
            accepted_count_[ack.seqnum()][ack.ballot().counter()].insert(ack.node_id());

            bool condition_q = (accepted_count_[ack.seqnum()][ack.ballot().counter()].size() == f_ + 1 ); // changed from. >=
            if ((condition_q && entries_to_commit_.count(ack.seqnum()) == 0) || 
                (ack.seqnum() <= max_seqnum_in_new_view_ && condition_q)) {
                
                should_commit = true;
                LOG << "[Leader] Node " << node_id_ << " has enough acks (" << accepted_count_[ack.seqnum()][ack.ballot().counter()].size() << " > " << f_ << ") to commit seqnum " << ack.seqnum() << std::endl;

            }
            else {
                LOG << "[Leader] Node " << node_id_ << " does not have enough acks yet to commit seqnum " << ack.seqnum() 
                        << " (have " << accepted_count_[ack.seqnum()][ack.ballot().counter()].size() 
                        << ", need " << (f_ + 1) << ")" << std::endl;
            }
        }
        

    }

    if (should_commit) {
        LOG << "[Leader] Node " << node_id_ << " calling commitentry w seqnum " << ack.seqnum() << " and 2pc status " << ack.status() << std::endl;
        CommitEntry(ack);
    }



}

void PaxosNode::SendNewView() {
    if (role_ != Role::LEADER || !alive_.load()) return;



    // Send asynchronously to all peers
    for (auto& [peer_id, stub] : peer_stubs_) {

        if (peer_id < intra_c_nid_range_.first || peer_id > intra_c_nid_range_.second) {
            continue; // skip nodes outside cluster
        }

        paxos::NewViewRequest request;
        request.mutable_ballot()->set_counter(current_ballot_.counter);
        request.mutable_ballot()->set_node_id(current_ballot_.node_id);

        std::vector<int> self_accept_seqs;
        {
            std::lock_guard<std::mutex> lock(log_mutex_);
            // Add all accept_log entries in sequence order
            for (int seq = 1; seq <= last_seqnum_.load(); ++seq) {
                auto it = accept_log_.find(seq);
                if (it != accept_log_.end()) {
                    *request.add_accept_log() = it->second;
                    self_accept_seqs.push_back(seq);
                }
            }
        }
        {
            // accepted_count_ is guarded by quorum_mutex_ everywhere else (e.g. HandleAccept).
            // Updating it under log_mutex_ here races HandleAccept's concurrent inserts during
            // the view change -> concurrent std::map mutation -> heap corruption. Use the right lock.
            std::lock_guard<std::mutex> lock(quorum_mutex_);
            for (int seq : self_accept_seqs) {
                accepted_count_[seq][current_ballot_.counter].insert(node_id_); // self accepts
            }
        }

        if (peer_id == node_id_) {
            {
                std::lock_guard<std::mutex> lock(printlog_mutex_);
                print_log_ += "<NEW-VIEW, (" + std::to_string(request.ballot().counter()) + "," + std::to_string(request.ballot().node_id()) + ")," + PaxosNode::AcceptLogToString(accept_log_) + "> \n"; // log its own new view
            }
            {
                std::lock_guard<std::mutex> lock(log_mutex_);
                new_view_logs_.push_back(request); // technically sent to self as well (just for log)
            }
            continue; // skip self
        }

        LOG<<"Sending to peer" << peer_id << " with stub " << stub << std::endl;
        max_seqnum_in_new_view_ = last_seqnum_.load();
        auto call = new NewViewClientCallData(stub, client_cq_.get(), this, request);
    }

    LOG << "Node " << node_id_ << " sent NEW-VIEW with ballot "
              << current_ballot_.ToString() << " to all peers." << std::endl;
}

void PaxosNode::SendPrepare(Ballot new_ballot) {

    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        current_ballot_ = new_ballot;
        last_prepare_received_ = std::chrono::steady_clock::now(); //recevies its own prepare
        in_election_ = true;
    }

    LOG << "Node " << node_id_ << " sending PREPARE with ballot " << new_ballot.ToString() << std::endl;

    { // log your own prepare
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        print_log_ += "<ACK, (" + std::to_string(new_ballot.counter) + "," + std::to_string(new_ballot.node_id) + "), "+ AcceptLogToString(accept_log_) + "> \n";
    }

    // Send PrepareRequest to all peers (not including self)
    for (auto& [peer_id, stub] : peer_stubs_) {
        if (peer_id == node_id_) continue; // skip self
        if (peer_id < intra_c_nid_range_.first || peer_id > intra_c_nid_range_.second) {
            continue; // skip nodes outside cluster
        }
        paxos::PrepareRequest request;
        request.mutable_ballot()->set_counter(new_ballot.counter);
        request.mutable_ballot()->set_node_id(new_ballot.node_id);
        request.set_node_id(node_id_); // proposer's id

        
        auto call = new PrepareClientCallData(stub, client_cq_.get(), this, request); // deleted in OnComplete() in CallData
        LOG << "Node " << node_id_ << " sent PREPARE to Node " << peer_id << std::endl;
    }
}

// Example: sending Accept message
void PaxosNode::SendAccept(Ballot ballot, int seqNum, const paxos::ClientRequest& request) {
    // TODO: create stub for each peer and send async Accept RPC
}


/** This is better than what I prev has becasue -> 
 * Thread local RNG and avoids rand() problems 
 */
std::chrono::milliseconds PaxosNode::GetRandomizedTimeout() {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(0, jitter_.count());
    return base_timeout_ + std::chrono::milliseconds(dist(rng));
}

void PaxosNode::StartElectionTimer() {
    if (role_ != Role::BACKUP || !alive_.load()) return; // only backups run timers

    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        if (timer_running_) return; // already running
        timer_running_ = true;
        election_reset_ = false; // no immediate reset
    }

    election_thread_ = std::thread([this]() {
        while (timer_running_ && alive_.load()) { // run as long as the node is alive (timer_running_ can be a condition if you dont want loop runnign when not in election)
            
            std::chrono::milliseconds timeout;
            if (current_ballot_.node_id % divisor_ == 1 && current_ballot_.counter == 0) {
                timeout = std::chrono::milliseconds(70);
            }
            else {
                timeout = GetRandomizedTimeout();
            }
            bool timed_out = false;
            bool woke_by_reset = false;
            LOG << "New timer loop for node " << node_id_ << " with timeout " << timeout.count() << " ms" << std::endl;
            // Wait with a mutex just for the condition variable
            {
                std::unique_lock<std::mutex> lock(election_mutex_);
                timed_out = !election_cv_.wait_for(lock, timeout, [this]() { return election_reset_; });
                
                if (election_reset_) {
                    woke_by_reset = true;
                    if (current_ballot_.node_id % divisor_ == 1 && current_ballot_.counter == 0) {
                        timed_out = true;
                    }
                }
                election_reset_ = false; // reset the flag
            }

            if (woke_by_reset) {
                LOG << "Node " << node_id_ << " woke up due to RESET" << std::endl;
            } else if (timed_out) {
                LOG << "Node " << node_id_ << " woke up due to TIMEOUT" << std::endl;
            }

            // Call timeout handler outside of the lock
            if (timed_out && alive_.load()) {
                LOG << " Calling OnElectionTimeout for node " << node_id_ << std::endl;

                RespondAndClearNewViewCallData(); // safe: nulls member before Respond, so no double-Finish
                OnElectionTimeout();
            }
        }
    });
}

/**
 * Node hasn't received commm from leader within the timeout period so this function is triggered
 * 
 */
void PaxosNode::OnElectionTimeout() {
    Ballot new_ballot;
    bool should_send_prepare = false;
    

    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        if (role_ == Role::LEADER || !alive_.load()) return; // only alive backups care

        auto now = std::chrono::steady_clock::now();
        auto since_last_leader_msg = now - last_leader_msg_received_;
        auto since_last_prepare = now - last_prepare_received_;

        LOG << "Time since last prepare for node " << node_id_
                  << ": " << std::chrono::duration_cast<std::chrono::milliseconds>(since_last_prepare).count()
                  << " ms" << std::endl;


        if (since_last_prepare >= prepare_cooldown_) {
            new_ballot = Ballot(current_ballot_.counter + 1, node_id_);

            LOG << "Node " << node_id_
                      << " election timeout, starting new election with ballot "
                      << new_ballot.ToString() << std::endl;

            // Reset election state
            waiting_for_promises_ = true;
            promises_received_.clear();
            promises_received_.insert(node_id_); // self-promise

            should_send_prepare = true;
        } else {
            // Skip this round, wait for next timer tick
            LOG << "Node " << node_id_
                      << " election timeout, but recently saw prepare. Skipping." << std::endl;
        }
        if (pending_promise_call_!= nullptr) {SendPromise();} // if there is a pending promise to send, send it now that weve timed out
    }

    // Unlock before doing network or timer reset operations
    if (should_send_prepare) {
        SendPrepare(new_ballot);  // network I/O outside lock
    }
}

void PaxosNode::OnElectionComplete() {
    std::vector<QueuedClientRequest> queued;

    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        in_election_ = false;

    }

    // Reconcile the INHERITED WAL first, BEFORE processing queued client requests. The reconcile
    // reverts in-flight coordinator debits; if it runs after the queued loop it also reverts the
    // fresh in-flight entries that loop just created (the queued requests execute new COORD txns),
    // which the later C_COORD commit does not re-apply -> a lost debit (the run602 divergence,
    // node 1 read 10 vs 9). At this point accept_log_ is already the merged log, so phase2_result /
    // request lookups are valid, and inherited WAL is all there is to reconcile here.
    {
        std::lock_guard<std::mutex> lock(wal_mutex_);
        for (auto it = wal_.begin(); it != wal_.end(); ) {
            int seqnum = it->first;
            auto& wal_entry = it->second;

            // Committed 2PC (phase2_result C_COORD/C): catch-up replay artifact whose
            // balance is final -- finalize (drop WAL), never revert (Mode B).
            const std::string& p2 = accept_log_[seqnum].phase2_result();
            if (p2 == "C_COORD" || p2 == "C") {
                it = wal_.erase(it);
                continue;
            }

            paxos::ClientRequest req = accept_log_[seqnum].request();
            int from_account = std::stoi(req.from_account());
            bool is_coordinator = (cluster_id_ == shard_map_[from_account]);
            int acct = is_coordinator ? from_account : std::stoi(req.to_account());

            if (p2 == "A_COORD" || p2 == "A_PART_COMMIT") {
                // Aborted: undo via DELTA (reverse this op) rather than setting the
                // account to WAL.before. Absolute revert is wrong when catch-up applied
                // other ops on this account afterward -- it discards them (the q8 ==9
                // acct-3 off-by-one: node 2 replayed an aborted debit, then a committed
                // credit; absolute revert to 10 dropped the credit; delta gives 11).
                accounts_[acct] += (wal_entry.before_value - wal_entry.after_value);
                balance_locks_[acct].unlock();
                it = wal_.erase(it);
                continue;
            }

            // In-flight (undecided outcome): revert as before.
            accounts_[acct] = wal_entry.before_value;
            balance_locks_[acct].unlock();
            if (is_coordinator) {
                LOG << "Reverted account " << acct << " to " << wal_entry.before_value
                        << " as part of demotion." << std::endl;
            }
            it = wal_.erase(it);
        }
    }

    if (leader_id_ != -1 )  { // otherwise forwarding blindly - TODO: maybe add condition to prevent 1 broadcast by client before handling

        {
            std::lock_guard<std::mutex> lock(queued_mutex_);
            queued.swap(queued_client_requests_);
        }

        LOG << "Node " << node_id_ << " election complete, processing "
                << queued.size() << " queued client requests." << std::endl;
        for (auto& req : queued) {
            ProcessClientRequest(req.request, req.call_data);
        }
    }
    else {
        LOG << "Node " << node_id_ << " election complete, but leader is Node 1, not processing queued requests since it does know who leader is" << std::endl;
    }

}

void PaxosNode::StopElectionTimer() {
    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        //if (!timer_running_) return; // already stopped
        
        if (timer_running_ == false) {
            LOG << "Node " << node_id_ << " election timer already stopped." << std::endl;
            return;
        }
        timer_running_ = false;      // signal the thread to stop
        election_reset_ = true;         // wake up the thread if it's waiting
    }
    LOG << "In Stop Election Timer, about to notify one" << std::endl;
    election_cv_.notify_one();          // notify the waiting thread and there is only 1 election timer thread per node
    if (election_thread_.joinable()) {
        election_thread_.join();        // wait for thread to exit
    }
    LOG << "Node " << node_id_ << " election timer stopped." << std::endl;

}

/**Wakes the timer thread which then re-evaluates its waiting condition and recalculates new timeout */
void PaxosNode::ResetElectionTimer() {
    last_leader_msg_received_ = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        //if (!timer_running_) return;   // timer not running, nothing to reset
        election_reset_ = true;           // signal the thread to reset its wait
    }

    if (timer_running_ == false) {
        StartElectionTimer();
    }
    else {
        election_cv_.notify_one();            // wake up the waiting thread
        LOG << "Node " << node_id_ << " election timer reset." << std::endl;
        LOG << "Election running for node " << node_id_ << ": " << timer_running_ << std::endl;
        LOG << "Election Reset for node " << node_id_ << ": " << election_reset_ << std::endl;
    }

}


void PaxosNode::BecomeLeader(Ballot ballot) {
    
    StopElectionTimer();

    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        role_ = Role::LEADER;
        current_ballot_ = ballot;
        leader_id_ = node_id_;
    }

    LOG << "Node " << node_id_ << " officially becomes leader with ballot " << ballot.ToString() << std::endl;

}

// hello
void PaxosNode::DemoteToBackup() {
    {
        role_ = Role::BACKUP;
        // Clear our own leadership claim. BecomeLeader sets leader_id_ = node_id_,
        // so a node that was briefly leader must reset it here -- otherwise it stays
        // a BACKUP with leader_id_ == node_id_, and forwarding a client request then
        // hits the leader_id_ == node_id_ std::abort() guard (crash -> cluster stall).
        leader_id_ = -1;
        LOG << "Node " << node_id_
                  << " demoted to backup. Current ballot: "
                  << current_ballot_.ToString() << std::endl;

        waiting_for_promises_ = false; // no longer in election
        // reset new view states:
        {
            std::lock_guard<std::mutex> newview_lock(newview_ack_mutex_);
            newview_acks_.clear();
            newview_quorum_reached_ = false;
        }
        {
            std::lock_guard<std::mutex> lock(quorum_mutex_);
            accepted_count_.clear();
        }

        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            entries_to_commit_.clear(); // clear out any entries to commit - they will be resent by leader
            pending_entries_.clear(); // clear out any pending entries - they will be resent by leader
            // Clean up pending client calls - they will retry
            for (auto& [seqnum, call_data] : pending_client_calls_) {
                paxos::TransactionAck ack; // does not need to be filled out just need to be killed
                call_data->Respond(ack);
            }
            pending_client_calls_.clear();
        }  

                //reset in_flights
        for (auto& entry : in_flight_two_pc_prepares_) {
            entry.completed = false;
            entry.sent = false;
            entry.last_send = std::chrono::steady_clock::time_point{};
            entry.msg = paxos::TwoPCMsg();
        }
        in_flight_two_pc_transactions_.clear();


        {
            std::lock_guard<std::mutex> lock(wal_mutex_);
            for (auto it = wal_.begin(); it != wal_.end(); ) {
                int seqnum = it->first;
                auto& wal_entry = it->second;

                // Committed 2PC (phase2_result C_COORD/C): catch-up replay artifact,
                // balance is final -- finalize (drop WAL), never revert.
                const std::string& p2 = accept_log_[seqnum].phase2_result();
                if (p2 == "C_COORD" || p2 == "C") {
                    it = wal_.erase(it);
                    continue;
                }

                paxos::ClientRequest req = accept_log_[seqnum].request();
                int from_account = std::stoi(req.from_account());
                bool is_coordinator = (cluster_id_ == shard_map_[from_account]);
                int acct = is_coordinator ? from_account : std::stoi(req.to_account());

                if (p2 == "A_COORD" || p2 == "A_PART_COMMIT") {
                    // Aborted: undo via DELTA (reverse this op), correct even if catch-up
                    // applied other ops on this account afterward (q8 ==9 acct-3 fix).
                    accounts_[acct] += (wal_entry.before_value - wal_entry.after_value);
                    balance_locks_[acct].unlock();
                    it = wal_.erase(it);
                    continue;
                }

                // In-flight (undecided outcome): KEEP the change + WAL so phase-2 resolves
                // it (C/C_COORD finalizes; an abort is handled by the delta-undo above once
                // the A_* outcome is known). Reverting here would drop a change that will
                // commit and catch-up won't re-apply (last_executed_seq_ passed it): the
                // demo3/q9 4001 + q8 3005 off-by-ones. Release the lock (a backup that
                // holds this in-flight change doesn't hold the balance lock).
                balance_locks_[acct].unlock();
                ++it;
                continue;
            }
        }
    } 
}

void PaxosNode::SetNewLeader(int new_leader_id, int new_leader_counter) {

    //called with election_mutex_ lock held
    leader_id_ = new_leader_id;
    cluster_leaders_[shard_map_[node_id_]] = new_leader_id;
    current_ballot_.counter = new_leader_counter;
    current_ballot_.node_id = new_leader_id;
    highest_promised_ballot_ = current_ballot_; 

    LOG << "Node " << node_id_ << " recognizes Node " << leader_id_ << " as new leader with ballot counter" << new_leader_counter << std::endl;
}


void PaxosNode::kill() {
    alive_.store(false);
    if (role_ == Role::BACKUP) {
        StopElectionTimer(); 
    }
    else {
        // if leader, demote to backup
        DemoteToBackup();
    }
    
    LOG << "Node " << node_id_ << " has been killed." << std::endl;
}

void PaxosNode::revive() {
    if (!alive_.exchange(true)) { // only act if previously dead
        alive_.store(true);
        LOG << "Node " << node_id_ << " revived." << std::endl;
        //if (role_ == Role::BACKUP) {
        //    StartElectionTimer(); 
        //}
    }
}

void PaxosNode::HandleSetAlive(const paxos::AliveUpdateRequest& request, paxos::TransactionAck* reply) {
    bool was_alive = alive_.load();
    reply->set_accepted(true);

    if (request.prompt_pause()) {
        alive_before_prompt_ = alive_.load();
    }

    if (request.reshard()) {
        for (const auto& entry : request.shard_map().entry()) {
              shard_map_[entry.item_id()] = entry.cluster();
        }
        return;
    }

    LOG << "[Node " << node_id_ << "] Hit HandleSetAlive with is_alive=" 
              << request.is_alive() << " kill_leader=" << request.kill_leader() << std::endl;

    if (request.reset_state()) {
        LOG<<"[Node " << node_id_ << "] Resetting state as per request." << std::endl;
        ResetNode();
    }

    if (request.kill_leader()) {
        bool was_leader = (role_ == Role::LEADER);
        kill();
        if (was_leader) {
            leader_id_ = -1;
        }
        alive_before_prompt_ = false;
        LOG << "[Node " << node_id_ << "] Killed as per kill request." << std::endl;
        return;
    }
    else if (request.recover()) {
        LOG << "[Node " << node_id_ << "] revived as per revive request." << std::endl;
        revive();
        return;
    }

    if (!request.prompt_pause() && request.is_alive() && !alive_before_prompt_) { // actual alive message 
        LOG << "[Node " << node_id_ << "] revive + demotion" << std::endl;
        DemoteToBackup(); 
        leader_id_ = -1; // ?
        revive();
    }
    else if (request.is_alive() && !was_alive) {
        revive();
    } 
    else if (!request.is_alive() && was_alive) {
        RespondAndClearNewViewCallData(); // safe: nulls member before Respond, so no double-Finish
        kill();
    }
    
    LOG << "[Node " << node_id_ << "] Executed alive update of is_alive="
                << request.is_alive() << std::endl;

}

std::string PaxosNode::AcceptLogToString(std::map<int, paxos::AcceptedEntry> accept_log) {
    std::string result;
    result += "{";
    for (const auto& [seq, entry] : accept_log) {
        int counter = entry.ballot().counter();
        int node_id = entry.ballot().node_id();
        int accept_seq = seq;
        const auto& req = entry.request();
        std::string from_client = req.from_account();
        std::string to_client = req.to_account();
        double amount = req.amount();
        result += "<ACCEPT, (" + std::to_string(counter) + ", " + std::to_string(node_id) + "), " +
                  std::to_string(accept_seq) + ", (" +
                  from_client + ", " + to_client + ", " +
                  std::to_string(amount) + ")>; ";
    }
    result += "}";
    return result;
}

void PaxosNode::HandleSendNodeInfo(const paxos::InfoRequest& request, paxos::NodeInfo* reply) {
    
    if (request.print_log()) {
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        reply->set_print_log(print_log_);
    }
    else if (request.print_db()){
        for (const auto& account : modified_accounts_) {
            
            float balance = accounts_[account];
            auto* entry = reply->add_database();
            entry->set_client(std::to_string(account));
            entry->set_balance(balance);
        }
    }
    else if (request.print_status()){
        if (executed_entries_.find(request.seqnum())!= executed_entries_.end()){
            reply->set_status("E");
        }
        else if (entries_to_commit_.count(request.seqnum())>0){
            reply->set_status("C");
        }
        else if (accept_log_.count(request.seqnum())>0){
            reply->set_status("A");
        }
        else{
            reply->set_status("X");
        }     

    }
    else if (request.print_newview()){
        for (const auto& nv : new_view_logs_){
            // copy nv into add_newviews
            reply->add_newviews()->CopyFrom(nv);
        }
    }
}


void PaxosNode::HandleMoveOn(const paxos::MoveOnRequest& request,
                             paxos::TransactionAck* reply) 
{
    std::lock_guard<std::mutex> lock(pending_mutex_);

    LOG << "[Node " << node_id_ << "] Received MoveOn for client " 
              << request.client_id() << std::endl;
    
    if (role_ == Role::LEADER) {
        LOG << "[Node " << node_id_ << "] I am the leader." << std::endl;
    }

    // Iterate over pending client calls and finish them
    LOG << "Size of pending calls before removal: " << pending_client_calls_.size() << std::endl;
    std::vector<int> to_erase; // store seqnums to remove from map
    for (const auto& [seqnum, call_data] : pending_client_calls_) {
        const auto& client_req = call_data->GetRequest(); // assuming getter exists

        LOG << "[Node " << node_id_ << "] Finishing pending request seqnum " 
                    << seqnum << " for client " << request.client_id() << std::endl;

        // Send empty TransactionAck to finish the RPC
        paxos::TransactionAck empty_ack;
        call_data->Respond(empty_ack);

        to_erase.push_back(seqnum);
        
    }

    // Remove finished calls from the map
    for (int seqnum : to_erase) {
        paxos::ClientRequest req = pending_entries_[seqnum].request();
        if (shard_map_[std::stoi(req.from_account())] == shard_map_[std::stoi(req.to_account())]){
            balance_locks_[std::stoi(req.from_account())].unlock();
            balance_locks_[std::stoi(req.to_account())].unlock();
            LOG << "[Node " << node_id_ << "] Released locks for client "
                      << req.from_account() << " and " << req.to_account() << std::endl;
        }
        pending_client_calls_.erase(seqnum);
    }

    // Remove queued client requests
    LOG<< "size of queued requests before removal: " << queued_client_requests_.size() << std::endl;
    auto it = queued_client_requests_.begin();
    while (it != queued_client_requests_.end()) {

        LOG << "[Node " << node_id_ << "] Removing queued request for client "
                    << request.client_id() << std::endl;
        paxos::TransactionAck empty_ack;
        it->call_data->Respond(empty_ack);
        it = queued_client_requests_.erase(it);

    }

    // handle the in flight 2pc stuff
    //reset in_flights
    for (auto& entry : in_flight_two_pc_prepares_) {
        entry.completed = false;
        entry.sent = false;
        entry.last_send = std::chrono::steady_clock::time_point{};
        entry.msg = paxos::TwoPCMsg();
    }
    in_flight_two_pc_transactions_.clear();



    reply->set_accepted(true);

}





void PaxosNode::SendReplyToCoordLeader( SendClientRequestCallData* call_data, int seq_num, std::string two_pc_status) {


    if (!alive_.load()) return;
    
    const paxos::ClientRequest& request = call_data ? call_data->GetRequest() : accept_log_[seq_num].m();


    if (request.client_id() == "NO-OP") return;

    paxos::TwoPCMsg reply;
    if (two_pc_status == "A") {
        reply.set_type("ABORT");
    }
    else if (two_pc_status == "P") {
        reply.set_type("PREPARED");
    }
    else if (two_pc_status == "C"){
        reply.set_type("COMMITTED");
    }
    else if (two_pc_status == "A_PART_COMMIT") {
        reply.set_type("A_PART_COMMIT");
    }

    reply.set_s(request.from_account());
    reply.set_r(request.to_account());
    reply.set_amt(request.amount());

    reply.mutable_m()->CopyFrom(request);


    LOG << "[Leader] Node " << node_id_ 
              << ": sending reply to Coordinator node " << request.from_account()
              << " for seqnum=" << seq_num
              << " status=" << two_pc_status << std::endl;
    
    int s = std::stoi(request.from_account());
    int coord_cluster_id_ = shard_map_[s];

    LOG << "coord cluster id is " << coord_cluster_id_ << std::endl;
    LOG << "leader for coord cluster id is " << cluster_leaders_[coord_cluster_id_] << std::endl;
    new TwoPCClientCallData(peer_stubs_[cluster_leaders_[coord_cluster_id_]], client_cq_.get(), this, reply); // changed

    if (call_data!= nullptr){
        // kill teh call_data 
        paxos::TransactionAck ack;
        call_data->Respond(ack); // clean up the call data
        LOG << "[Leader] Node " << node_id_ << ": cleaned up new-view call data " << std::endl;
    }


}

void PaxosNode::HandlePreparedAndAborted(const paxos::TwoPCMsg& msg) {

    LOG << "[Node " << node_id_ << "] Received PrepareAndAbort message of type " 
              << msg.type() << " for (" << msg.s() << ", " << msg.r() << ", " 
              << msg.amt() << ", " << msg.m().timestamp() << ")" << std::endl;
    

    // if you are the coord: check if ive finished executing corresponding 1st phase on coord end
    int s = std::stoi(msg.s());
    bool is_coord = shard_map_[s] == cluster_id_;
    LOG << "[Node " << node_id_ << "] is_coord = " << is_coord << std::endl;
    int ts = msg.m().timestamp();
    if (is_coord) {
        {
            std::lock_guard<std::mutex> lock(executed_entries_mutex_);
            if (executed_entries_.find(digest_to_seqnum_[ts]) != executed_entries_.end()) {
                LOG << "[Node " << node_id_ << "] Has executed 1st phase for timestamp " 
                        << ts << ", can start 2nf phase" << std::endl;
                StartSecondPhase(msg);
            }
            else {
                LOG << "[Node " << node_id_ << "] Has NOT executed 1st phase for timestamp " 
                        << ts << ", cannot start 2nf phase yet" << std::endl;

                prepare_and_aborted_queue_[ts] = msg; // store for later processing
            }
        }

    }
    else {
        // participant just start second phase
        LOG << "[Node " << node_id_ << "] I am participant, starting 2nd phase for timestamp " 
                  << ts << std::endl;
        StartSecondPhase(msg);
    }

}

void PaxosNode::StartSecondPhase(paxos::TwoPCMsg msg) {
    // initiate consensus in this cluster
    
    int r = std::stoi(msg.m().to_account());
    bool participant = shard_map_[r] == cluster_id_;
    bool abort = msg.type() == "ABORT";


    if (!participant) {

        //remove from in_flight_prepares_
        in_flight_two_pc_prepares_[msg.m().timestamp()].completed = true;

        int participant_cluster_id_ = shard_map_[r];

        paxos::TwoPCMsg twopc_request;
        twopc_request.CopyFrom(msg);

        if (abort) {
            twopc_request.set_type("ABORT");
        }
        else {
            twopc_request.set_type("COMMIT");
        }
        
        in_flight_two_pc_transactions_[msg.m().timestamp()].last_send = std::chrono::steady_clock::now();
        in_flight_two_pc_transactions_[msg.m().timestamp()].msg = twopc_request;
        // send 2pc commit to participant backup and set a timer to broadcast if it doesnt receive response
        new TwoPCClientCallData(peer_stubs_[cluster_leaders_[participant_cluster_id_]], client_cq_.get(), this, twopc_request); // changed
        
    }



    paxos::AcceptedEntry old_accept = accept_log_[digest_to_seqnum_[msg.m().timestamp()]];

    // create accept message
    paxos::AcceptedEntry accept_msg;
    accept_msg.mutable_request()->CopyFrom(msg.m());
    accept_msg.mutable_ballot()->set_counter(old_accept.ballot().counter());
    accept_msg.mutable_ballot()->set_node_id(old_accept.ballot().node_id());
    accept_msg.set_node_id(node_id_);
    accept_msg.mutable_m()->CopyFrom(msg.m());
    accept_msg.set_seqnum(old_accept.seqnum());

    if (abort  && ! participant) {
        accept_msg.set_two_pc_status("A_COORD");
    }
    else if (!abort && !participant) {
        accept_msg.set_two_pc_status("C_COORD");
    }
    else if (abort && participant) {
        accept_msg.set_two_pc_status("A_PART_COMMIT");
    }
    else if (!abort && participant) {
        accept_msg.set_two_pc_status("C");
    }
    else {
        LOG << "Problem - case unhandled" << std::endl;
    }


    //Log As Acceted 
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        accept_log_[accept_msg.seqnum()] = accept_msg;
    }

    {
        std::lock_guard<std::mutex> lock(quorum_mutex_);
        accepted_count_two_pc_commit_[old_accept.seqnum()][old_accept.ballot().counter()].insert(node_id_);
    }

    for (auto& [peer_id, stub] : peer_stubs_) {
        if (peer_id == node_id_) continue; // skip self
        if (peer_id < intra_c_nid_range_.first || peer_id > intra_c_nid_range_.second) {
            continue; // skip nodes outside cluster
        }
        new AcceptClientCallData(stub, client_cq_.get(), this, accept_msg);
    }


    return;
}

void PaxosNode::ExecuteRepeatedTwoPCEntry(paxos::CommitEntry entry) {


    LOG << "[Node " << node_id_ << "] Enter Re-executing TwoPC CommitEntry for seqnum " 
              << entry.seqnum() << " with 2PC status " << entry.two_pc_status() << std::endl;

    std::string two_pc_status = entry.two_pc_status();
    int seqnum = entry.seqnum();
    LOG << "[Node " << node_id_ << (entry.m().from_account()) << (entry.m().to_account())  << std::endl;
    int from = std::stoi((entry.m().from_account()));
    int to = std::stoi(entry.m().to_account());

    // Record the FINAL 2PC outcome (commit or abort) onto the log entry, once, so it
    // ships in NEW-VIEW -> a recovering node learns commit-vs-abort and its revert
    // loops can KEEP committed catch-up WAL (Mode B) and correctly UNDO aborted ones
    // (the q8 ==9 acct-3 off-by-one, where node 2 replayed an aborted debit but never
    // received the A_COORD to undo it).
    if (two_pc_status == "C_COORD" || two_pc_status == "C" ||
        two_pc_status == "A_COORD" || two_pc_status == "A_PART_COMMIT") {
        std::lock_guard<std::mutex> lock(log_mutex_);
        auto ait = accept_log_.find(seqnum);
        if (ait != accept_log_.end() && ait->second.phase2_result().empty()) {
            ait->second.set_phase2_result(two_pc_status);
        }
    }

   if (two_pc_status == "A_COORD") {
        // use WAL to undo executions and then release locks

        LOG << "[Node " << node_id_ << "] Undoing effects of aborted transaction for seqnum " 
                  << entry.seqnum() << " from " << from << " to " << to << std::endl;
        WalEntry x;
        {
            std::lock_guard<std::mutex> lock(wal_mutex_);
            x = wal_[seqnum];
            wal_.erase(seqnum); // remove wal entry
        }

        
        LOG << "[Node " << node_id_ << "] wal entry is (" << x.before_value << ", " << x.after_value << ")" << std::endl;
        accounts_[from] = x.before_value;
        //SetBalance(from, false);
        balance_locks_[from].unlock();

        

        LOG << "[Node " << node_id_ << "] new balance of account " << from << " is " << accounts_[from] << std::endl;

    }
    else if (two_pc_status == "C_COORD") {
        LOG << "[Node " << node_id_ << "] Committing transaction for seqnum " 
                  << entry.seqnum() << " from " << from << " to " << to << std::endl;
        // release locks
        balance_locks_[from].unlock();
        {
            std::lock_guard<std::mutex> lock(wal_mutex_);
            wal_.erase(seqnum); // remove wal entry
        }
        

    }
    else if (two_pc_status == "C") {
        LOG << "[Node " << node_id_ << "] Committing participant transaction for seqnum " 
                  << entry.seqnum() << " from " << from << " to " << to << std::endl;
        // release locks 
        balance_locks_[to].unlock();
        {
            std::lock_guard<std::mutex> lock(wal_mutex_);
            wal_.erase(seqnum); // remove wal entry
        }

    }
    else if (two_pc_status == "A_PART_COMMIT") {  
        LOG << "[Node " << node_id_ << "] Undoing effects of aborted participant transaction for seqnum " 
                  << entry.seqnum() << " from " << from << " to " << to << std::endl; 

        {
            std::lock_guard<std::mutex> lock(wal_mutex_);
            auto it = wal_.find(seqnum);

            // if participant aborts, I do not make a WAL entry, but in the case that the participant prepared, 
            // but the coord sends a 2nd phase abort, there will be a WAL entry
            if (it != wal_.end()) {
                LOG << "[Node " << node_id_ << "] wal entry is (" << it->second.before_value << ", " << it->second.after_value << ")" << std::endl;
                accounts_[to] = it->second.before_value;
                //SetBalance(to, false);
                wal_.erase(seqnum); // remove wal entry
            }
        }
        balance_locks_[to].unlock();
        LOG << "[Node " << node_id_ << "] new balance of account " << to << " is " << accounts_[to] << std::endl;
    }

    // send replies
    if (role_ == Role::LEADER) {

        if (two_pc_status == "C_COORD") {
            // send reply to client
            SendClientReplyTwoPC(entry, entry.seqnum(), true);
            
        }
        else if (two_pc_status == "C") {
            // send reply to coord
            SendReplyToCoordLeader(nullptr, entry.seqnum(), "C");

        }
        else if (two_pc_status == "A_COORD") {
            SendClientReplyTwoPC(entry, entry.seqnum(), false);

        }
        else if (two_pc_status == "A_PART_COMMIT") {
            SendReplyToCoordLeader(nullptr, entry.seqnum(), "A_PART_COMMIT");
        }
    }

}

void PaxosNode::HandleCommittedAndAborted(const paxos::TwoPCMsg& msg) {

    LOG << "[Node " << node_id_ << "] Received CommittedAndAborted message of type " 
              << msg.type() << " for (" << msg.r() << ", " << msg.s() << ", " 
              << msg.amt() << ", " << msg.m().timestamp() << ")" << std::endl;
    

    // stop retries from occurring
    auto& entry = in_flight_two_pc_transactions_[msg.m().timestamp()];
    entry.completed = true;

}

void PaxosNode::TransactionRetryLoop() {
    std::chrono::milliseconds timeout = std::chrono::milliseconds(10000);
    while (true) {
        auto now = std::chrono::steady_clock::now();
        

        for (auto& entry: in_flight_two_pc_transactions_) {
            if (entry.last_send != std::chrono::steady_clock::time_point{} && 
                !entry.completed.load() 
                && now - entry.last_send >= timeout) {
                
                entry.last_send = now;
                LOG << "[Node " << node_id_ << "] retrying 2PC commit/abort message for timestamp " 
                          << entry.msg.m().timestamp() << " of type " << entry.msg.type() << std::endl;
            }
        }

        for (auto& entry: in_flight_two_pc_prepares_) {
            if (entry.sent.load() && entry.last_send != std::chrono::steady_clock::time_point{} && 
                !entry.completed.load() 
                && now - entry.last_send >= timeout) {
                
                LOG << "[Node " << node_id_ << "] coord aborting txn due to timeout for prepare " << std::endl;
                entry.completed = true; // stop retrying
                HandlePreparedTimeout(entry.msg.m().timestamp());
            }
        }


        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}


std::thread PaxosNode::LaunchTransactionRetryThread() {
    return std::thread([this]() { this->TransactionRetryLoop(); });
}


void PaxosNode::SendClientReplyTwoPC(const paxos::CommitEntry entry, int seq_num, bool success) {

    if (!alive_.load()) return;
    
    const paxos::ClientRequest& request = entry.m();

    if (request.client_id() == "NO-OP") return;

    paxos::ClientReply reply;
    reply.mutable_ballot()->set_counter(-1); // dont think this matters
    reply.mutable_ballot()->set_node_id(current_ballot_.node_id);
    reply.set_client_id(request.from_account());
    reply.set_timestamp(request.timestamp());
    reply.set_success(success);
    reply.set_from_this_term(seq_num > max_seqnum_in_new_view_);
    LOG << "set from this term to " << (seq_num > max_seqnum_in_new_view_) << std::endl;


    // Cache reply for exactly-once semantics
    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        last_reply_per_client_[request.from_account()] = reply;
    }


    LOG << "[Leader] Node " << node_id_ 
              << ": sending reply (2pc) to client " << request.from_account()
              << " for seqnum=" << seq_num
              << " success=" << success << std::endl;
    

    new AsyncClientReplyCallData(client_stubs_["A"], client_cq_.get(), reply); // changed

}

void PaxosNode::HandlePreparedTimeout(int timestamp) {

    LOG << "[Node " << node_id_ << "] hasnt received PREPARED/ABORTED from backup node, aborting for Coord end " 
               << std::endl;


    paxos::AcceptedEntry acc_entry;
    acc_entry.CopyFrom(accept_log_[digest_to_seqnum_[timestamp]]);
    acc_entry.mutable_m()->CopyFrom(in_flight_two_pc_prepares_[timestamp].msg.m());
    acc_entry.set_two_pc_status("A_COORD");
    acc_entry.set_seqnum(digest_to_seqnum_[timestamp]);

    //Log As Acceted 
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        accept_log_[acc_entry.seqnum()] = acc_entry;
    }

    // send to backups
    for (auto& [peer_id, stub] : peer_stubs_) {
        if (peer_id == node_id_) continue; // skip self
        if (peer_id < intra_c_nid_range_.first || peer_id > intra_c_nid_range_.second) {
            continue; // skip nodes outside cluster
        }
        new AcceptClientCallData(stub, client_cq_.get(), this, acc_entry);
    }


}

void PaxosNode::ResetNode() {

    // reset attributes
    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        role_ = Role::BACKUP;
        current_ballot_ = Ballot(0, node_id_);
        highest_promised_ballot_ = Ballot(0, 0);
        leader_id_ = -1;
        waiting_for_promises_ = false;
        promises_received_.clear();
        new_view_logs_.clear();
        newview_acks_.clear();
        in_election_ = true;
    }

    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        accept_log_.clear();
        last_seqnum_.store(0);
        last_executed_seq_ = 0;
        max_seqnum_in_new_view_ = 0;
    }

    {
        std::lock_guard<std::mutex> lock(quorum_mutex_);
        accepted_count_.clear();
    }

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        entries_to_commit_.clear();
        pending_entries_.clear();
        for (auto& [seqnum, call_data] : pending_client_calls_) {
            paxos::TransactionAck ack; // does not need to be filled out just
            call_data->Respond(ack);
        }
        pending_client_calls_.clear();
    }

    {
        std::lock_guard<std::mutex> lock(printlog_mutex_);
        print_log_.clear();
    }

    handled_ro_requests_.clear();
    first_txn_received_ = true;


    executed_entries_.clear();
    modified_accounts_.clear();
    accepted_count_two_pc_commit_.clear();
    digest_to_seqnum_.clear();
    wal_.clear();
    executed_entries_.clear();
    max_seqnum_in_new_view_ = 0;

    //reset in_flights
    for (auto& entry : in_flight_two_pc_prepares_) {
        entry.completed = false;
        entry.sent = false;
        entry.last_send = std::chrono::steady_clock::time_point{};
        entry.msg = paxos::TwoPCMsg();
    }
    in_flight_two_pc_transactions_.clear();

    for (int i = 1; i <= num_clusters_ ; ++i) {
        cluster_leaders_[i] = divisor_ * (i - 1) + 1;
        LOG << "Cluster " << i << " leader is Node " << cluster_leaders_[i] << std::endl;
    }
    for (int i = 1; i <= 9000; ++i) {
        accounts_[i] = 10.0;
    }

    InitializeDatabase();
    InitDB();

}


void PaxosNode::InitializeDatabase() {
    std::string db_dir = "../database";
    struct stat st = {0};
    if (stat(db_dir.c_str(), &st) == -1) {
        mkdir(db_dir.c_str(), 0700);
    }

    std::string db_path = db_dir + "/node_" + std::to_string(node_id_) + ".db";
    if (sqlite3_open(db_path.c_str(), &db_) != SQLITE_OK) {
        std::cerr << "Failed to open database: " << sqlite3_errmsg(db_) << std::endl;
        return;
    }

    // Enable WAL mode and normal sync
    sqlite3_exec(db_, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);
    sqlite3_exec(db_, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);

    // Create table
    const char* create_sql =
        "CREATE TABLE IF NOT EXISTS accounts ("
        " item_id INTEGER PRIMARY KEY,"
        " balance INTEGER NOT NULL"
        ");";
    char* errmsg = nullptr;
    if (sqlite3_exec(db_, create_sql, nullptr, nullptr, &errmsg) != SQLITE_OK) {
        std::cerr << "SQL error: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        return;
    }

    // Check row count
    const char* count_sql = "SELECT COUNT(*) FROM accounts;";
    sqlite3_stmt* stmt = nullptr;
    int row_count = 0;
    if (sqlite3_prepare_v2(db_, count_sql, -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            row_count = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    // Begin transaction for initialization
    sqlite3_exec(db_, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);

    if (row_count >= 9000) {
        // Reset all balances to 10
        const char* reset_sql = "UPDATE accounts SET balance = 10;";
        if (sqlite3_exec(db_, reset_sql, nullptr, nullptr, &errmsg) != SQLITE_OK) {
            std::cerr << "SQL error: " << errmsg << std::endl;
            sqlite3_free(errmsg);
        } else {
            LOG << "Node " << node_id_ << ": All balances reset to 10." << std::endl;
        }
    } else {
        // Insert missing rows with balance = 10
        sqlite3_stmt* insert_stmt = nullptr;
        const char* insert_sql = "INSERT OR IGNORE INTO accounts (item_id, balance) VALUES (?, 10);";
        if (sqlite3_prepare_v2(db_, insert_sql, -1, &insert_stmt, nullptr) == SQLITE_OK) {
            for (int item = 1; item <= 9000; ++item) {
                sqlite3_bind_int(insert_stmt, 1, item);
                if (sqlite3_step(insert_stmt) != SQLITE_DONE) {
                    std::cerr << "Insert error at item " << item << ": " << sqlite3_errmsg(db_) << std::endl;
                }
                sqlite3_reset(insert_stmt);
            }
            sqlite3_finalize(insert_stmt);
            LOG << "Node " << node_id_ << ": Database initialized with 9000 items." << std::endl;
        } else {
            std::cerr << "Failed to prepare insert statement: " << sqlite3_errmsg(db_) << std::endl;
        }
    }

    sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);

    // Prepare update statement for high-throughput updates
    const char* update_sql = "UPDATE accounts SET balance = balance + ? WHERE item_id = ?;";
    if (sqlite3_prepare_v2(db_, update_sql, -1, &update_stmt_, nullptr) != SQLITE_OK) {
        std::cerr << "Failed to prepare update statement: " << sqlite3_errmsg(db_) << std::endl;
    }
}


void PaxosNode::InitDB() {
    std::string db_path = "../database/node_" + std::to_string(node_id_) + ".db";
    if (sqlite3_open(db_path.c_str(), &db_) != SQLITE_OK) {
        std::cerr << "Failed to open database: " << sqlite3_errmsg(db_) << std::endl;
        return;
    }

    sqlite3_exec(db_, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);
    sqlite3_exec(db_, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);

    const char* update_sql = "UPDATE accounts SET balance = balance + ? WHERE item_id = ?;";
    if (sqlite3_prepare_v2(db_, update_sql, -1, &update_stmt_, nullptr) != SQLITE_OK) {
        std::cerr << "Failed to prepare update stmt: " << sqlite3_errmsg(db_) << std::endl;
    }
}


void PaxosNode::UpdateBalance(int item_id, int amount, bool is_sender) {
    if (!update_stmt_ || !alv_) return;

    int signed_amount = is_sender ? -amount : amount;
    sqlite3_reset(update_stmt_);
    sqlite3_bind_int(update_stmt_, 1, signed_amount);
    sqlite3_bind_int(update_stmt_, 2, item_id);

    if (sqlite3_step(update_stmt_) != SQLITE_DONE) {
        std::cerr << "Failed to update balance for item " << item_id << ": " << sqlite3_errmsg(db_) << std::endl;
    }
}

void PaxosNode::SetBalance(int item_id, double new_balance) {
    if (!db_ || !alv_) {
        return;
    }

    const char* sql = "UPDATE accounts SET balance = ? WHERE item_id = ?;";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        std::cerr << "Failed to prepare set balance statement: " << sqlite3_errmsg(db_) << std::endl;
        return;
    }

    sqlite3_bind_int(stmt, 1, new_balance);
    sqlite3_bind_int(stmt, 2, item_id);

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        std::cerr << "Failed to set balance for item " << item_id
                  << ": " << sqlite3_errmsg(db_) << std::endl;
    }

    sqlite3_finalize(stmt);
}

int PaxosNode::GetBalance(int item_id) {
    if (!db_ || !alv_) {
        return -1;
    }

    const char* sql = "SELECT balance FROM accounts WHERE item_id = ?;";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        std::cerr << "Failed to prepare get balance statement: " << sqlite3_errmsg(db_) << std::endl;
        return -1;
    }

    sqlite3_bind_int(stmt, 1, item_id);

    int balance = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        balance = sqlite3_column_int(stmt, 0);
    } else {
        std::cerr << "Item " << item_id << " not found or failed to read balance." << std::endl;
    }

    sqlite3_finalize(stmt);
    return balance;
}