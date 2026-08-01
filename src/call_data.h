#ifndef CALL_DATA_H
#define CALL_DATA_H
// starting point for rewarok!!!!!!
#include <grpcpp/grpcpp.h>
#include "../grpc/paxos.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

class PaxosNode;
class PaxosClient;


/** 
 * CallData is abstract base class managing lifecycle of single asynch gRPC in multi-threaded Paxos Node.
 * Handles incoming RPC w/o blocking and supports concurrent request handling
 */
class CallData {
public:
    virtual void Proceed() = 0;
    ~CallData() = default;

protected:
    paxos::Paxos::AsyncService* service_;
    ServerCompletionQueue* cq_;
    ServerContext ctx_;
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;
};


class PrepareCallData : public CallData {
public:
    PrepareCallData(paxos::Paxos::AsyncService* service,
                    grpc::ServerCompletionQueue* cq,
                    PaxosNode* node);

    void Proceed() override;
    void Respond(paxos::PromiseResponse reply);


private:
    PaxosNode* paxos_node_;
    grpc::ServerAsyncResponseWriter<paxos::PromiseResponse> responder_;
    paxos::PrepareRequest request_;
    paxos::PromiseResponse reply_;
};


// Class for handling Accept RPCs.
class AcceptCallData : public CallData {
public:
    AcceptCallData(paxos::Paxos::AsyncService* service, ServerCompletionQueue* cq, PaxosNode* paxos_node);
    void Proceed() override;

private:
    paxos::AcceptedEntry request_;
    paxos::Ack reply_;
    ServerAsyncResponseWriter<paxos::Ack> responder_;
    PaxosNode* paxos_node_;
};

// Class for handling Commit RPCs.
class CommitCallData : public CallData {
public:
    CommitCallData(paxos::Paxos::AsyncService* service, ServerCompletionQueue* cq, PaxosNode* paxos_node);
    void Proceed() override;

private:
    paxos::CommitEntry request_;
    paxos::Ack reply_;
    ServerAsyncResponseWriter<paxos::Ack> responder_;
    PaxosNode* paxos_node_;
};

// Class for handling NewView RPCs.
class NewViewCallData : public CallData {
public:
    NewViewCallData(paxos::Paxos::AsyncService* service, ServerCompletionQueue* cq, PaxosNode* paxos_node);
    void Proceed() override;
    void Respond(const paxos::Ack& reply);

private:
    paxos::NewViewRequest request_;
    paxos::Ack reply_;
    ServerAsyncResponseWriter<paxos::Ack> responder_;
    PaxosNode* paxos_node_;
};

// Class for handling client requests (Client -> Leader)
class SendClientRequestCallData : public CallData {
public:
    SendClientRequestCallData(paxos::Paxos::AsyncService* service,
                              grpc::ServerCompletionQueue* cq,
                              PaxosNode* node);

    void Proceed() override;
    void Respond(const paxos::TransactionAck& reply);

    // Getter for the request
    const paxos::ClientRequest& GetRequest() const;

    // Getter for the responder
    grpc::ServerAsyncResponseWriter<paxos::TransactionAck>& GetResponder();  

private:
    PaxosNode* paxos_node_;
    grpc::ServerAsyncResponseWriter<paxos::TransactionAck> responder_;
    paxos::ClientRequest request_;
    paxos::TransactionAck reply_;
};

class SubmitTransactionCallData : public CallData {
public:
    SubmitTransactionCallData(paxos::ClientService::AsyncService* service,
                              ServerCompletionQueue* cq);

    void Proceed() override;

private:
    paxos::ClientRequest request_;
    paxos::TransactionAck reply_;
    grpc::ServerAsyncResponseWriter<paxos::TransactionAck> responder_;
    paxos::ClientService::AsyncService* service_;
};



class ReceiveAcceptAckCallData : public CallData {
public:
    ReceiveAcceptAckCallData(paxos::Paxos::AsyncService* service,
                             grpc::ServerCompletionQueue* cq,
                             PaxosNode* node);

    void Proceed() override;

private:
    PaxosNode* paxos_node_;
    paxos::Ack request_;
    google::protobuf::Empty reply_;
    grpc::ServerAsyncResponseWriter<google::protobuf::Empty> responder_;
};


// Leader -> backup heartbeat. Backup just resets its election timer; no meaningful reply.
class HeartbeatCallData : public CallData {
public:
    HeartbeatCallData(paxos::Paxos::AsyncService* service,
                      grpc::ServerCompletionQueue* cq,
                      PaxosNode* node);

    void Proceed() override;

private:
    PaxosNode* paxos_node_;
    paxos::HeartbeatRequest request_;
    google::protobuf::Empty reply_;
    grpc::ServerAsyncResponseWriter<google::protobuf::Empty> responder_;
};


class AliveUpdateCallData : public CallData {
public:
    AliveUpdateCallData(paxos::Paxos::AsyncService* service,
                        grpc::ServerCompletionQueue* cq,
                        PaxosNode* paxos_node);

    void Proceed() override;

private:
    paxos::Paxos::AsyncService* service_;
    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;
    paxos::TransactionAck reply_;  // dummy response
    grpc::ServerAsyncResponseWriter<paxos::TransactionAck> responder_;
    paxos::AliveUpdateRequest request_;
    PaxosNode* paxos_node_;
};

class GetNodeInfoCallData : public CallData {
public:
    GetNodeInfoCallData(paxos::Paxos::AsyncService* service,
                        grpc::ServerCompletionQueue* cq,
                        PaxosNode* paxos_node);

    void Proceed() override;

private:
    paxos::Paxos::AsyncService* service_;
    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;
    paxos::NodeInfo reply_;  // dummy response
    grpc::ServerAsyncResponseWriter<paxos::NodeInfo> responder_;
    paxos::InfoRequest request_;
    PaxosNode* paxos_node_;
};

class MoveOnCallData : public CallData {
public:
    MoveOnCallData(paxos::Paxos::AsyncService* service,
                   ServerCompletionQueue* cq,
                   PaxosNode* paxos_node);

    void Proceed() override;

private:
    paxos::Paxos::AsyncService* service_;
    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;
    paxos::MoveOnRequest request_;
    paxos::TransactionAck reply_;
    ServerAsyncResponseWriter<paxos::TransactionAck> responder_;
    PaxosNode* paxos_node_;
};

// ______________________Client-side calls________________________


// Base class for async client calls
struct AsyncClientCallBase {
    virtual ~AsyncClientCallBase() = default;
    virtual void OnComplete(bool ok) = 0;
};

// Prepare RPC client call
struct PrepareClientCallData : public AsyncClientCallBase {
    PrepareClientCallData(std::shared_ptr<paxos::Paxos::Stub> stub,
                          grpc::CompletionQueue* cq,
                          PaxosNode* node,
                          paxos::PrepareRequest request);
    void OnComplete(bool ok) override;
    void End(); // to be called to finish the RPC

private:
    std::shared_ptr<paxos::Paxos::Stub> stub_;
    grpc::ClientContext context_;
    grpc::CompletionQueue* cq_;
    PaxosNode* node_;
    paxos::PrepareRequest request_;
    paxos::PromiseResponse reply_;
    grpc::Status status_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<paxos::PromiseResponse>> response_reader_;
};

// NewView RPC client call
struct NewViewClientCallData : public AsyncClientCallBase {
    NewViewClientCallData(std::shared_ptr<paxos::Paxos::Stub> stub,
                          grpc::CompletionQueue* cq,
                          PaxosNode* node,
                          const paxos::NewViewRequest& request);
    void OnComplete(bool ok) override;

private:
    std::shared_ptr<paxos::Paxos::Stub> stub_;
    grpc::ClientContext context_;
    grpc::CompletionQueue* cq_;
    PaxosNode* node_;
    paxos::NewViewRequest request_;
    paxos::Ack reply_;
    grpc::Status status_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<paxos::Ack>> response_reader_;
};

/**
 * Asynchronous client call for sending alive updates to nodes.
 * Non-blocking: handles RPC response in OnComplete.
 */
class AliveAsyncCall : public AsyncClientCallBase {
public:
    AliveAsyncCall(std::shared_ptr<paxos::Paxos::Stub> stub,
                   grpc::CompletionQueue* cq,
                   int node_id,
                   bool is_alive,
                   bool kill_leader,
                   bool recover,
                   bool prompt_pause,
                   bool reset_state,
                   bool reshard,
                   const paxos::ShardMap& new_shard_map
                  );

    void OnComplete(bool ok) override;

private:
    std::shared_ptr<paxos::Paxos::Stub> stub_;
    grpc::ClientContext context_;
    grpc::CompletionQueue* cq_;
    int node_id_;
    paxos::AliveUpdateRequest request_;
    paxos::TransactionAck reply_;
    grpc::Status status_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<paxos::TransactionAck>> response_reader_;
    //std::function<void(bool)> callback_;
};

// Accept RPC client call
struct AcceptClientCallData : public AsyncClientCallBase {
    AcceptClientCallData(std::shared_ptr<paxos::Paxos::Stub> stub,
                         grpc::CompletionQueue* cq,
                         PaxosNode* node,
                         paxos::AcceptedEntry request); // changed from const ref

    void OnComplete(bool ok) override;
    void Cancel();

private:
    std::shared_ptr<paxos::Paxos::Stub> stub_;
    grpc::ClientContext context_;
    grpc::CompletionQueue* cq_;
    PaxosNode* node_;
    paxos::AcceptedEntry request_;
    paxos::Ack reply_;
    grpc::Status status_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<paxos::Ack>> response_reader_;
};

// Commit RPC client call (leader -> backups)
struct CommitClientCallData : public AsyncClientCallBase {
    CommitClientCallData(std::shared_ptr<paxos::Paxos::Stub> stub,
                         grpc::CompletionQueue* cq,
                         PaxosNode* node,
                         const paxos::CommitEntry& request);

    void OnComplete(bool ok) override;

private:
    std::shared_ptr<paxos::Paxos::Stub> stub_;
    grpc::ClientContext context_;
    grpc::CompletionQueue* cq_;
    PaxosNode* node_;
    paxos::CommitEntry request_;
    paxos::Ack reply_;
    grpc::Status status_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<paxos::Ack>> response_reader_;
};


// ClientReply -> Client (async) call (leader -> client)
struct ClientCallData : public AsyncClientCallBase {
    ClientCallData(std::shared_ptr<paxos::ClientService::Stub> stub,
                   grpc::CompletionQueue* cq,
                   const paxos::ClientReply& request);

    void OnComplete(bool ok) override;

private:
    std::shared_ptr<paxos::ClientService::Stub> stub_;
    grpc::ClientContext context_;
    grpc::CompletionQueue* cq_;
    paxos::ClientReply request_;         
    paxos::TransactionAck reply_ack_;   
    grpc::Status status_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<paxos::TransactionAck>> response_reader_;
};


struct AsyncAcceptAckCall : public AsyncClientCallBase {
    paxos::Ack ack;
    grpc::ClientContext context;
    grpc::Status status;
    std::unique_ptr<grpc::ClientAsyncResponseReader<google::protobuf::Empty>> response_reader;
    google::protobuf::Empty reply_;
    int node_id_; // optional, for logging

    AsyncAcceptAckCall(std::shared_ptr<paxos::Paxos::Stub> stub,
                       const paxos::Ack& a,
                       grpc::CompletionQueue* cq);

    void OnComplete(bool ok) override;

    void Cancel();
};


// Leader -> backup heartbeat, fire-and-forget (reply is ignored).
struct AsyncHeartbeatCall : public AsyncClientCallBase {
    paxos::HeartbeatRequest req;
    grpc::ClientContext context;
    grpc::Status status;
    std::unique_ptr<grpc::ClientAsyncResponseReader<google::protobuf::Empty>> response_reader;
    google::protobuf::Empty reply_;

    AsyncHeartbeatCall(std::shared_ptr<paxos::Paxos::Stub> stub,
                       const paxos::HeartbeatRequest& r,
                       grpc::CompletionQueue* cq);

    void OnComplete(bool ok) override;
};


struct AsyncClientReplyCallData : public AsyncClientCallBase {
    AsyncClientReplyCallData(std::shared_ptr<paxos::ClientService::Stub> stub,
                             grpc::CompletionQueue* cq,
                             const paxos::ClientReply& request);

    void OnComplete(bool ok) override;

private:
    std::shared_ptr<paxos::ClientService::Stub> stub_;
    grpc::ClientContext context_;
    grpc::CompletionQueue* cq_;
    paxos::ClientReply request_;
    paxos::TransactionAck reply_;
    grpc::Status status_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<paxos::TransactionAck>> response_reader_;
};


class GetNodeInfoClient {
public:
    // Construct with a stub to a specific Paxos node
    explicit GetNodeInfoClient(std::shared_ptr<paxos::Paxos::Stub> stub);

    // Synchronous call: returns NodeInfo. Throws runtime_error if RPC fails.
    paxos::NodeInfo GetNodeInfo(paxos::InfoRequest& request);

private:
    std::shared_ptr<paxos::Paxos::Stub> stub_;
};



struct AsyncCall : public AsyncClientCallBase {
    AsyncCall(std::shared_ptr<paxos::Paxos::Stub> stub,
              grpc::CompletionQueue* cq,
              const paxos::ClientRequest request,
              std::function<void(const paxos::TransactionAck&)> completion_cb = nullptr);

    void OnComplete(bool ok) override;
    void Cancel();

    // gRPC / client state
    grpc::ClientContext context;
    paxos::TransactionAck reply;
    grpc::Status status;
    std::unique_ptr<grpc::ClientAsyncResponseReader<paxos::TransactionAck>> response_reader;


    std::shared_ptr<paxos::Paxos::Stub> stub_;
    grpc::CompletionQueue* cq_;
    paxos::ClientRequest request_;
    std::function<void(const paxos::TransactionAck&)> completion_callback;
};


// ________________________________ Client Server Side CallData ________________________________
// for incoming messages from leader
class AsyncServerCall {
public:
    AsyncServerCall(paxos::ClientService::AsyncService* service,
                    grpc::ServerCompletionQueue* cq,
                    PaxosClient* client);

    void Proceed();

protected:
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;

    grpc::ServerContext ctx_;
    paxos::ClientService::AsyncService* service_;
    grpc::ServerCompletionQueue* cq_;
    PaxosClient* client_;
    paxos::ClientReply request_;
    paxos::TransactionAck reply_;
    grpc::ServerAsyncResponseWriter<paxos::TransactionAck> responder_;
};






// additions for project 3


struct TwoPCClientCallData: public AsyncClientCallBase {
    TwoPCClientCallData(std::shared_ptr<paxos::Paxos::Stub> stub,
                         grpc::CompletionQueue* cq,
                         PaxosNode* node,
                         const paxos::TwoPCMsg& request);

    void OnComplete(bool ok) override;

private:
    std::shared_ptr<paxos::Paxos::Stub> stub_;
    grpc::ClientContext context_;
    grpc::CompletionQueue* cq_;
    PaxosNode* node_;
    paxos::TwoPCMsg request_;
    google::protobuf::Empty reply_;
    grpc::Status status_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<google::protobuf::Empty>> response_reader_;
};


class TwoPCCallData : public CallData {
public:
    TwoPCCallData(paxos::Paxos::AsyncService* service, ServerCompletionQueue* cq, PaxosNode* paxos_node);
    void Proceed() override;

private:
    paxos::TwoPCMsg request_;
    google::protobuf::Empty reply_;
    ServerAsyncResponseWriter<google::protobuf::Empty> responder_;
    PaxosNode* paxos_node_;
};








#endif 
