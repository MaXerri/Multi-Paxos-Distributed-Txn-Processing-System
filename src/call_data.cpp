#include "call_data.h"
#include "paxos_node.h"
#include <assert.h>
#include "paxos_client.h"
#include "log.h"


PrepareCallData::PrepareCallData(paxos::Paxos::AsyncService* service, ServerCompletionQueue* cq, PaxosNode* paxos_node)
    : responder_(&ctx_), paxos_node_(paxos_node) {
    service_ = service;
    cq_ = cq;
    status_ = CREATE;
    Proceed();
}

void PrepareCallData::Proceed() {
    if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestPrepare(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
        new PrepareCallData(service_, cq_, paxos_node_); 
        paxos_node_->HandlePrepare(request_, this); // modified for later response
    } else {
        assert(status_ == FINISH);
        delete this;
    }
}

void PrepareCallData::Respond(paxos::PromiseResponse reply) {
    reply_ = reply;
    status_ = FINISH;
    responder_.Finish(reply_, grpc::Status::OK, this);
}



AcceptCallData::AcceptCallData(paxos::Paxos::AsyncService* service, ServerCompletionQueue* cq, PaxosNode* paxos_node)
    : responder_(&ctx_), paxos_node_(paxos_node) {
    service_ = service;
    cq_ = cq;
    status_ = CREATE;
    Proceed();
}

void AcceptCallData::Proceed() {
    if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestAccept(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
        new AcceptCallData(service_, cq_, paxos_node_);
        paxos_node_->HandlePropose(request_, &reply_);
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
    } else if (status_ == FINISH) {
        delete this;
    }
}


CommitCallData::CommitCallData(paxos::Paxos::AsyncService* service, ServerCompletionQueue* cq, PaxosNode* paxos_node)
    : responder_(&ctx_), paxos_node_(paxos_node) {
    service_ = service;
    cq_ = cq;
    status_ = CREATE;
    Proceed();
}

void CommitCallData::Proceed() {
    if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestCommit(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
        new CommitCallData(service_, cq_, paxos_node_);
        paxos_node_->HandleCommit(request_, &reply_);
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
    } else if (status_ == FINISH) {
        LOG << "[Node " << paxos_node_->GetNodeId() << "] Commit call finished" << std::endl;
        delete this;
    }
}


NewViewCallData::NewViewCallData(paxos::Paxos::AsyncService* service, ServerCompletionQueue* cq, PaxosNode* paxos_node)
    : responder_(&ctx_), paxos_node_(paxos_node) {
    service_ = service;
    cq_ = cq;
    status_ = CREATE;
    Proceed();
}

void NewViewCallData::Proceed() {
    
    if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestNewView(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
        new NewViewCallData(service_, cq_, paxos_node_);
        paxos_node_->HandleNewView(request_, this);


    } else {
        assert(status_ == FINISH);
        paxos_node_->OnNewViewCallFinished(this); // avoid complications
        LOG << "[DEBUG] NewViewCallData finished for node " << paxos_node_->GetNodeId() << std::endl;
        delete this;
    }
}

void NewViewCallData::Respond(const paxos::Ack& reply) {
    LOG << "[DEBUG] NewViewCallData hit respond for node " << paxos_node_->GetNodeId() << std::endl;
    reply_ = reply;
    status_ = FINISH;
    responder_.Finish(reply_, grpc::Status::OK, this);
}

SendClientRequestCallData::SendClientRequestCallData(
    paxos::Paxos::AsyncService* service,
    grpc::ServerCompletionQueue* cq,
    PaxosNode* node)
    : paxos_node_(node),
      responder_(&ctx_)
{
    service_ = service;
    cq_ = cq;
    status_ = CREATE;

    // Request that gRPC notify us when a new ClientRequest arrives
    service_->RequestSendClientRequest(&ctx_, &request_, &responder_, cq_, cq_, this);
}

void SendClientRequestCallData::Proceed() {
    if (status_ == CREATE) {
        status_ = PROCESS;

        // Spawn another CallData for the next request
        new SendClientRequestCallData(service_, cq_, paxos_node_);

        // Log the received request
        LOG << "[DEBUG] Node " << paxos_node_->GetNodeId() 
                  << " received ClientRequest to client " << request_.to_account() 
                  << " from client " << request_.from_account() << std::endl;

        // Instead of finishing here, register this CallData with the node
        //paxos_node_->HandleClientRequest(request_, this);
        paxos_node_->HandleClientRequest(request_, this); //modified for ...

    } else if (status_ == FINISH) {
        LOG << "[DEBUG] SendClientRequestCallData finished for node " << paxos_node_->GetNodeId() << std::endl;
        delete this; // cleanup
    }
}

// Getter for the request
const paxos::ClientRequest& SendClientRequestCallData::GetRequest() const {
    return request_;
}

// Getter for the responder
grpc::ServerAsyncResponseWriter<paxos::TransactionAck>& SendClientRequestCallData::GetResponder() {
    return responder_;
}

void SendClientRequestCallData::Respond(const paxos::TransactionAck& reply) {
    reply_ = reply;
    status_ = FINISH;
    responder_.Finish(reply_, grpc::Status::OK, this);
}

SubmitTransactionCallData::SubmitTransactionCallData(
        paxos::ClientService::AsyncService* service,
        ServerCompletionQueue* cq)
    : responder_(&ctx_), service_(service) {
    cq_ = cq;
    status_ = CREATE;
    Proceed();
}

void SubmitTransactionCallData::Proceed() {
    if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestSubmitTransaction(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
        
        new SubmitTransactionCallData(service_, cq_);

        // Process the transaction (placeholder)
        LOG << "Received transaction from client " 
                  << request_.client_id() 
                  << " " << request_.from_account() 
                  << " -> " << request_.to_account()
                  << " amount=" << request_.amount() << std::endl;

        reply_.set_accepted(true); // just ack for now

        status_ = FINISH;
        responder_.Finish(reply_, grpc::Status::OK, this);
    } else {
        assert(status_ == FINISH);
        LOG << "[DEBUG] SubmitTransactionCallData finished "
                  << "for client " << request_.client_id() << std::endl;
        delete this;
    }
}


ReceiveAcceptAckCallData::ReceiveAcceptAckCallData(
    paxos::Paxos::AsyncService* service,
    grpc::ServerCompletionQueue* cq,
    PaxosNode* node)
    : paxos_node_(node),
      responder_(&ctx_) 
{
    service_ = service;
    cq_ = cq;
    status_ = CREATE;
    Proceed();
}

void ReceiveAcceptAckCallData::Proceed() {
    if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestReceiveAcceptAck(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
        
        new ReceiveAcceptAckCallData(service_, cq_, paxos_node_);

        paxos_node_->HandleAccept(request_);

        status_ = FINISH;
        responder_.Finish(reply_, grpc::Status::OK, this);
    } else {
        assert(status_ == FINISH);
        LOG << "[DEBUG] ReceiveAcceptAckCallData finished for node " << paxos_node_->GetNodeId() << " and seq: " << request_.seqnum() << std::endl;
        delete this;
    }
}

HeartbeatCallData::HeartbeatCallData(
    paxos::Paxos::AsyncService* service,
    grpc::ServerCompletionQueue* cq,
    PaxosNode* node)
    : paxos_node_(node),
      responder_(&ctx_)
{
    service_ = service;
    cq_ = cq;
    status_ = CREATE;
    Proceed();
}

void HeartbeatCallData::Proceed() {
    if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestHeartbeat(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
        new HeartbeatCallData(service_, cq_, paxos_node_);
        paxos_node_->HandleHeartbeat(request_);   // backup just resets its timer
        status_ = FINISH;
        responder_.Finish(reply_, grpc::Status::OK, this);
    } else {
        assert(status_ == FINISH);
        delete this;
    }
}

AliveUpdateCallData::AliveUpdateCallData(paxos::Paxos::AsyncService* service,
                                         grpc::ServerCompletionQueue* cq,
                                         PaxosNode* paxos_node)
    : service_(service),
      cq_(cq),
      paxos_node_(paxos_node),
      responder_(&ctx_)  // bind responder to context
{
    status_ = CREATE;
    Proceed();
}

void AliveUpdateCallData::Proceed() {
    if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestSendAliveUpdate(&ctx_, &request_, &responder_, cq_, cq_, this);

    } else if (status_ == PROCESS) {
        
        new AliveUpdateCallData(service_, cq_, paxos_node_);

        paxos_node_->HandleSetAlive(request_, &reply_);

        status_ = FINISH;
        responder_.Finish(reply_, grpc::Status::OK, this);

    } else {
        assert(status_ == FINISH);
        LOG << "[DEBUG] AliveUpdateCallData finished for node " << paxos_node_->GetNodeId() << std::endl;
        delete this;
    }
}

GetNodeInfoCallData::GetNodeInfoCallData(paxos::Paxos::AsyncService* service,
                                         grpc::ServerCompletionQueue* cq,
                                         PaxosNode* paxos_node)
    : service_(service), cq_(cq), responder_(&ctx_), paxos_node_(paxos_node) {

    status_ = CREATE;
    Proceed();
}

void GetNodeInfoCallData::Proceed() {
    
    if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestGetNodeInfo(&ctx_, &request_, &responder_, cq_, cq_, this);
    } 
    else if (status_ == PROCESS) {
        
        new GetNodeInfoCallData(service_, cq_, paxos_node_);

        paxos_node_->HandleSendNodeInfo(request_, &reply_);

        status_ = FINISH;
        responder_.Finish(reply_, grpc::Status::OK, this);
    } 
    else {
        assert(status_ == FINISH);
        delete this;
    }
}

MoveOnCallData::MoveOnCallData(paxos::Paxos::AsyncService* service,
                               ServerCompletionQueue* cq,
                               PaxosNode* paxos_node)
    : service_(service), cq_(cq), responder_(&ctx_), paxos_node_(paxos_node) 
{
    // Request gRPC to notify us when a new MoveOn RPC arrives
    service->RequestSendMoveOn(&ctx_, &request_, &responder_, cq, cq, this);
    status_ = CREATE;
}

void MoveOnCallData::Proceed() {
    if (status_ == CREATE) {
        status_ = PROCESS;

        // Spawn a new handler for the next incoming MoveOn RPC
        new MoveOnCallData(static_cast<paxos::Paxos::AsyncService*>(service_), cq_, paxos_node_);

        LOG << "[Node " << paxos_node_->GetNodeId() 
                  << "] Received MoveOn request for client " << request_.client_id() << std::endl;

        // Handle the MoveOn logic
        paxos_node_->HandleMoveOn(request_, &reply_);

        // Respond to the client
        responder_.Finish(reply_, grpc::Status::OK, this);

        status_ = FINISH;
    } else {
        delete this; // cleanup
    }
}



// ---------------- PaxosNode Client Side CallData ----------------

PrepareClientCallData::PrepareClientCallData(std::shared_ptr<paxos::Paxos::Stub> stub,
                                             grpc::CompletionQueue* cq,
                                             PaxosNode* node,
                                             paxos::PrepareRequest request)
    : stub_(stub), cq_(cq), node_(node), request_(request) 
{
    response_reader_ = stub_->AsyncPrepare(&context_, request_, cq_);
    response_reader_->Finish(&reply_, &status_, this);
}

void PrepareClientCallData::OnComplete(bool ok) {
    if (ok && status_.ok()) {
        node_->HandlePromise(reply_);
    } else {
        std::cerr << "Prepare RPC failed (network/timeout or error)" << std::endl;
    }
    LOG << "[DEBUG] PrepareClientCallData finished for node " << node_->GetNodeId() << std::endl;
    delete this;
}



NewViewClientCallData::NewViewClientCallData(std::shared_ptr<paxos::Paxos::Stub> stub,
                                             grpc::CompletionQueue* cq,
                                             PaxosNode* node,
                                             const paxos::NewViewRequest& request)
    : stub_(stub), cq_(cq), node_(node), request_(request) 
{
    LOG << "[DEBUG] NewViewClientCallData constructed for node "
              << node_->GetNodeId() << std::endl;
    response_reader_ = stub_->AsyncNewView(&context_, request_, cq_);
    // Log BEFORE Finish(): after Finish() publishes `this` as the CQ tag, a polling
    // thread may `delete this` before the next line runs -> touching node_ here is a
    // use-after-free (same defect as AsyncCall). Finish() must be the last statement.
    LOG << "[DEBUG] finish called for newview RPC sent by " << node_->GetNodeId() << std::endl;
    response_reader_->Finish(&reply_, &status_, this);
}

void NewViewClientCallData::OnComplete(bool ok) {
    if (ok && status_.ok()) {
        LOG << "NewView RPC to node " << request_.ballot().node_id() 
                  << " succeeded." << std::endl;

    } else {
        std::cerr << "NewView RPC failed for node " << node_->GetNodeId() << ": " << status_.error_message() << std::endl;
    }
    LOG << "[DEBUG] NewViewClientCallData deleted for node " << node_->GetNodeId() << std::endl;
    delete this;
}




AliveAsyncCall::AliveAsyncCall(std::shared_ptr<paxos::Paxos::Stub> stub,
                               grpc::CompletionQueue* cq,
                               int node_id,
                               bool is_alive,
                               bool kill_leader,
                               bool recover,
                               bool prompt_pause, 
                               bool reset_state,
                               bool reshard,
                               paxos::ShardMap new_shard_map
                               //std::function<void(bool)> callback
                            )
    : stub_(std::move(stub)),
      cq_(cq),
      node_id_(node_id)
      //callback_(std::move(callback))
{
    // Set alive status in the request
    request_.set_is_alive(is_alive); 
    request_.set_kill_leader(kill_leader); 
    request_.set_prompt_pause(prompt_pause);
    request_.set_recover(recover);
    request_.set_reset_state(reset_state);
    if (reshard) {
        request_.mutable_shard_map()->CopyFrom(new_shard_map);
        request_.set_reshard(true);
    }

    // Start RPC - changed to synchronous since it needs to be processed before starting next call
    response_reader_ = stub_->AsyncSendAliveUpdate(&context_, request_, cq_);
    response_reader_->Finish(&reply_, &status_, this);
}

void AliveAsyncCall::OnComplete(bool ok) {
    if (!ok || !status_.ok()) {
        std::cerr << "[AliveAsyncCall] Node " << node_id_
                  << " alive update failed: " << status_.error_message() << "\n";
    }
    else if (ok && status_.ok()) {
        LOG << "[AliveAsyncCall] Node " << node_id_
                  << " alive update succeeded.\n";
        //callback_(true); // notify success
    }
    delete this;
}


AcceptClientCallData::AcceptClientCallData(std::shared_ptr<paxos::Paxos::Stub> stub,
                                           grpc::CompletionQueue* cq,
                                           PaxosNode* node,
                                           paxos::AcceptedEntry request)
    : stub_(stub), cq_(cq), node_(node), request_(request) {
    
    // Start the asynchronous RPC
    reply_ = paxos::Ack();
    response_reader_ = stub_->AsyncAccept(&context_, request_, cq_);
    response_reader_->Finish(&reply_, &status_, this);
}

void AcceptClientCallData::OnComplete(bool ok) {
    if (ok && status_.ok()) {
        // Successfully sent Accept to leader
        // You could log or update any state if needed
        // Example: node_->HandleAcceptAck(request_, reply_);
    } else {
        std::cerr << "[Client] Failed to send Accept for seq=" << request_.seqnum()
                  << " to leader. Status: " << status_.error_message() << std::endl;
        // Optionally, implement retry logic here
    }

    delete this; // cleanup
}

void AcceptClientCallData::Cancel() {
    // Safe to call from any thread.
    LOG << "[Leader] Cancelling Accept RPC for seq " << request_.seqnum() << std::endl;
    context_.TryCancel();  // signals cancellation
}

CommitClientCallData::CommitClientCallData(std::shared_ptr<paxos::Paxos::Stub> stub,
                                           grpc::CompletionQueue* cq,
                                           PaxosNode* node,
                                           const paxos::CommitEntry& request)
    : stub_(stub), cq_(cq), node_(node), request_(request) {

    reply_ = paxos::Ack();

    // Start the asynchronous Commit RPC
    response_reader_ = stub_->AsyncCommit(&context_, request_, cq_);
    response_reader_->Finish(&reply_, &status_, this);
}

void CommitClientCallData::OnComplete(bool ok) {
    if (ok && status_.ok()) {

        LOG << "[Client] Successfully sent Commit for seq=" << request_.seqnum()
                  << " to backup node " << std::endl;
    } else {
        std::cerr << "[Client] Failed to send Commit for seq=" << request_.seqnum()
                  << " to backup node " << node_->GetNodeId() << ". Status: " << status_.error_message() << std::endl;

    }

    delete this; 
}


ClientCallData::ClientCallData(std::shared_ptr<paxos::ClientService::Stub> stub,
                               grpc::CompletionQueue* cq,
                               const paxos::ClientReply& request)
    : stub_(std::move(stub)), cq_(cq), request_(request)
{
    // Start async RPC: SendClientReply(ClientReply) -> TransactionAck
    response_reader_ = stub_->AsyncSendClientReply(&context_, request_, cq_);
    // When the RPC completes the completion-queue will return 'this' as the tag
    response_reader_->Finish(&reply_ack_, &status_, this);
}

void ClientCallData::OnComplete(bool ok) {
    if (ok && status_.ok()) {

        LOG << "[Node] Successfully sent ClientReply to client "
                  << request_.client_id() << ", ack=" << reply_ack_.accepted() << "\n";
    } else {
        std::cerr << "[Node] Failed to deliver ClientReply to client "
                  << request_.client_id() << ". gRPC error: "
                  << (status_.ok() ? "(ok but !ok flag)" : status_.error_message()) << "\n";

    }

    delete this;
}


//used to send AcceptAck to leader
AsyncAcceptAckCall::AsyncAcceptAckCall(std::shared_ptr<paxos::Paxos::Stub> stub,
                                       const paxos::Ack& a,
                                       grpc::CompletionQueue* cq)
    : ack(a), node_id_(a.node_id())
{
    response_reader = stub->AsyncReceiveAcceptAck(&context, ack, cq);
    response_reader->Finish(&reply_, &status, this);
    LOG << "[Node " << node_id_ << "] finish called for asyncacceptack" << std::endl;
}

void AsyncAcceptAckCall::OnComplete(bool ok) {
    if (!ok || !status.ok()) {
        std::cerr << "[Node " << node_id_ << "] AcceptAck RPC failed for seq: "
                  << ack.seqnum() << ", ballot: (" << ack.ballot().counter() << ", "
                  << ack.ballot().node_id() << ") with error: " << status.error_message() << "\n";
    } else {
        LOG << "[Node " << node_id_ << "] AcceptAck successfully sent.\n";
    }
    delete this;
}

void AsyncAcceptAckCall::Cancel() {
    LOG << "[Node " << node_id_ << "] Cancelling AcceptAck RPC for seq: "
                << ack.seqnum() << std::endl;
    context.TryCancel();
}


// Leader -> backup heartbeat. Fire-and-forget: the backup only resets its timer,
// so we ignore ok/status entirely and just clean up.
AsyncHeartbeatCall::AsyncHeartbeatCall(std::shared_ptr<paxos::Paxos::Stub> stub,
                                       const paxos::HeartbeatRequest& r,
                                       grpc::CompletionQueue* cq)
    : req(r)
{
    response_reader = stub->AsyncHeartbeat(&context, req, cq);
    response_reader->Finish(&reply_, &status, this);
}

void AsyncHeartbeatCall::OnComplete(bool ok) {
    delete this;
}


AsyncClientReplyCallData::AsyncClientReplyCallData(
    std::shared_ptr<paxos::ClientService::Stub> stub,
    grpc::CompletionQueue* cq,
    const paxos::ClientReply& request)
    : stub_(stub), cq_(cq), request_(request) 
{
    // Start the async RPC
    response_reader_ = stub_->AsyncSendClientReply(&context_, request_, cq_);
    response_reader_->Finish(&reply_, &status_, this);
}

void AsyncClientReplyCallData::OnComplete(bool ok) {
    if (!ok) {
        std::cerr << "[Leader] ClientReply RPC failed to complete (network/queue issue)\n";
    } else if (status_.ok()) {
        LOG << "[Leader] Successfully sent reply to client "
                  << request_.client_id()
                  << " (ts=" << request_.timestamp() << ")\n";
    } else {
        LOG << "[Leader] Failed to send reply to client "
                  << request_.client_id()
                  << " (ts=" << request_.timestamp() << ") "
                  << "error=" << status_.error_message() << std::endl;
    }

    delete this;
}






GetNodeInfoClient::GetNodeInfoClient(std::shared_ptr<paxos::Paxos::Stub> stub)
    : stub_(stub) {}

paxos::NodeInfo GetNodeInfoClient::GetNodeInfo(paxos::InfoRequest& request) {
    paxos::NodeInfo reply;
    grpc::ClientContext context;


    grpc::Status status = stub_->GetNodeInfo(&context, request, &reply);

    if (!status.ok()) {
        throw std::runtime_error(
            "GetNodeInfo RPC failed: " + status.error_message()
        );
    }

    return reply;
}


AsyncCall::AsyncCall(std::shared_ptr<paxos::Paxos::Stub> stub,
                     grpc::CompletionQueue* cq,
                     const paxos::ClientRequest request,
                     std::function<void(const paxos::TransactionAck&)> completion_cb)
    : stub_(stub), cq_(cq), request_(request), completion_callback(completion_cb)
{
    // Start the async RPC
    response_reader = stub_->AsyncSendClientRequest(&context, request_, cq_);

    // LOG BEFORE Finish(): once Finish() publishes `this` as the CQ tag, a polling
    // thread can dequeue it and `delete this` immediately -- so touching request_ (or
    // any member) after Finish() is a use-after-free. This raced under the retry storm
    // to a dead leader and segfaulted the client (~1/300). Log while still exclusively owned.
    LOG << "[AsyncCall] Node sending RPC to target: "
              << request_.to_account()  // or some unique identifier
              << ", timestamp: " << request_.timestamp() << std::endl;

    response_reader->Finish(&reply, &status, this); // 'this' used as tag for completion queue -- must be LAST
}

void AsyncCall::OnComplete(bool ok) {
    if (ok && status.ok()) {
        // completion_callback(reply);
        LOG << "[AsyncCall] RPC succeeded: " << reply.DebugString() << std::endl;
    } else if (!status.ok()) {
        LOG << "[AsyncCall] RPC failed: " << status.error_message() << std::endl;
    }

    // Cleanup self
    delete this;
}

void AsyncCall::Cancel() {
    context.TryCancel();
}



// ---------------- PaxosClient Server Side CallData ----------------------

AsyncServerCall::AsyncServerCall(paxos::ClientService::AsyncService* service,
                                 grpc::ServerCompletionQueue* cq,
                                 PaxosClient* client)
    : service_(service), cq_(cq), client_(client), responder_(&ctx_) {
    status_ = CREATE;
    Proceed();
}

void AsyncServerCall::Proceed() {
    if (status_ == CREATE) {
        LOG << "[DEBUG] Call data starting handleSendClientReply" << std::endl;
        status_ = PROCESS;
        service_->RequestSendClientReply(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {

        new AsyncServerCall(service_, cq_, client_);
        // LOG << "[DEBUG] Call data starting hadlelleaderreply" << std::endl;   
        client_->HandleLeaderReply(request_);

        // Send dummy ack back to the caller
        reply_.set_accepted(true);
        status_ = FINISH;
        responder_.Finish(reply_, grpc::Status::OK, this);
    } else if (status_ == FINISH) {
        delete this;
    }
}





// additions for p3 

TwoPCCallData::TwoPCCallData(paxos::Paxos::AsyncService* service, ServerCompletionQueue* cq, PaxosNode* paxos_node)
    : responder_(&ctx_), paxos_node_(paxos_node) {
    service_ = service;
    cq_ = cq;
    status_ = CREATE;
    Proceed();
}

void TwoPCCallData::Proceed() {
    if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestSendTwoPCMsg(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
        new TwoPCCallData(service_, cq_, paxos_node_);
        if (request_.type() == "PREPARED" || request_.type() == "ABORT" || request_.type() == "COMMIT") {
            paxos_node_->HandlePreparedAndAborted(request_);
        }
        else if (request_.type() == "COMMITTED" || request_.type() == "A_PART_COMMIT") {
            paxos_node_->HandleCommittedAndAborted(request_);

        }
        else {
            LOG << "[Node " << paxos_node_->GetNodeId() << "] Unknown TwoPCMsg type: " << request_.type() << std::endl;
        }
        
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
    } else {
        assert(status_ == FINISH);
        LOG << "[Node " << paxos_node_->GetNodeId() << "] 2PC call finished" << std::endl;
        delete this;
    }
}

TwoPCClientCallData::TwoPCClientCallData(std::shared_ptr<paxos::Paxos::Stub> stub,
                                           grpc::CompletionQueue* cq,
                                           PaxosNode* node,
                                           const paxos::TwoPCMsg& request)
    : stub_(stub), cq_(cq), node_(node), request_(request) {

    reply_ = google::protobuf::Empty();

    // Start the asynchronous Commit RPC
    response_reader_ = stub_->AsyncSendTwoPCMsg(&context_, request_, cq_);
    response_reader_->Finish(&reply_, &status_, this);
}

void TwoPCClientCallData::OnComplete(bool ok) {
    if (ok && status_.ok()) {

        LOG << "[Client] Successfully sent TwoPCMsg"<< std::endl;
    } else {
        std::cerr << "[Client] Failed to send TwoPCMsg" << " Status: " << status_.error_message() << std::endl;

    }

    delete this; 
}
