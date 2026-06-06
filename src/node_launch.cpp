#include <iostream>
#include <cstdlib>
#include <string>
#include <thread>
#include "paxos_node.h"
#include "paxos_client.h"
#include "benchmark.h"
#include <fstream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <grpcpp/grpcpp.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/select.h>
#include <unistd.h>


std::atomic<int> remaining_transactions{0};
std::atomic<int> current_set_number{0};
std::atomic<bool> stop_input{false};
std::set<int> active_nodes_set;
std::unordered_map<int, std::shared_ptr<paxos::Paxos::Stub>> stubs_;
std::atomic<bool> move_on_{false}; 
int TOTAL_ITEMS = 9000;
std::vector<std::pair<int,int>> transactions_processed;
std::chrono::steady_clock::time_point start_time;
std::chrono::steady_clock::time_point end_time;


void run_node(int node_id, const std::unordered_map<int, std::string>& node_addresses, std::vector<std::string> client_ids, 
               std::unordered_map<int, int> shard_map, int num_clusters) {
    
    PaxosNode::Role role = PaxosNode::Role::BACKUP; // everyone starts as backup

    std::unordered_map<std::string, std::string> client_addr;
    for (const auto& client_id : client_ids) {
        client_addr[client_id] = "127.0.0.1:" + std::to_string(6000 + (client_id[0] - 'A') + 1);
    }

    // Create and run the Paxos node
    PaxosNode node(node_id, node_addresses.at(node_id), role, node_addresses, client_addr, shard_map, num_clusters);
    node.Run();
}


struct ClientContext {
    std::unique_ptr<PaxosClient> client;
    std::queue<paxos::ClientRequest> requests;
    std::atomic<bool> busy{false}; // this only works because we have a single thread per client
};

/**
 * Set alive status of a node asynchronously based off the active node list from the test file
 */
void SendAliveUpdateAsync(const std::set<int>& alive_nodes,
                          std::unordered_map<int, std::shared_ptr<paxos::Paxos::Stub>>& stubs,
                          grpc::CompletionQueue& cq,
                          bool fail = false,
                          bool recover = false,
                          int mod_node = -1,
                          bool prompt_user = false, 
                          bool reset_state = false) {
    if (!fail and !recover)
        for (auto& [node_id, stub] : stubs) {
            bool is_alive = alive_nodes.count(node_id) > 0;
            // currently async but should switch to sync 
            new AliveAsyncCall(stub, &cq, node_id, is_alive, fail, recover, prompt_user, reset_state, false, paxos::ShardMap());
        }
    else {
        // only send to the modified node
        auto stub = stubs.at(mod_node);
        bool is_alive = !fail; // if failing, set to false, if recovering, set to true
        new AliveAsyncCall(stub, &cq, mod_node, is_alive, fail, recover, prompt_user, reset_state, false, paxos::ShardMap());
    }
}


// Helper: start sending queued requests for a client
void StartClientRequests(const std::string& client_id,
                         std::unordered_map<std::string, std::unique_ptr<ClientContext>>& clients,
                         std::set<int>& active_nodes,
                         bool set_num_empty,
                         int num_nodes,
                         std::unordered_map<int, std::shared_ptr<paxos::Paxos::Stub>>& stubs,
                         grpc::CompletionQueue& cq,
                         paxos::ClientRequest& req) {

    auto& ctx = clients.at("A");
    
    if (!set_num_empty) { // should only ocur at beginning of set 
        SendAliveUpdateAsync(active_nodes, stubs, cq, false, false, -1, false, true); // gRPC call to the node and reset state
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // 500
    }


    std::cout << "testing client send" << std::endl;
    if (!set_num_empty) {
        
    }
    ctx->client->SendRequest(req, nullptr);  // null callback because handled via server

}


std::unordered_map<int,int> Reshard(
    int num_clusters,
    const std::vector<std::pair<int,int>>& transactions,
    const std::unordered_map<int,int>& current_mapping,
    int total_items
) {
    std::unordered_map<int,int> new_mapping = current_mapping;

    // 1. Compute cluster loads (number of transactions each cluster participated in)
    std::vector<int> cluster_loads(num_clusters, 0);
    for (const auto& t : transactions) {
        int s = t.first;
        int r = t.second;
        int cluster_s = current_mapping.at(s);
        cluster_loads[cluster_s]++;
        if (r != -1){
            int cluster_r = current_mapping.at(r);
            cluster_loads[cluster_r]++;
        }
    }
    // 2. Count cross-cluster transactions per item
    std::vector<int> cross_count(total_items + 1, 0);
    std::cout << transactions.size() << std::endl;
    for (const auto& t : transactions) {
        int s = t.first;
        int r = t.second;
        if (r != -1 && current_mapping.at(s) != current_mapping.at(r)) {
            cross_count[s]++;
            cross_count[r]++;
        }
    }

    // 3. Define max allowed cluster size (no cluster >30% bigger than average)
    int avg_size = total_items / num_clusters;
    int max_size = static_cast<int>(avg_size * 1.3);

    // 4. Try to reduce cross-shard transactions
    for (const auto& t : transactions) {
        int s = t.first;
        int r = t.second;
        int cluster_s = new_mapping[s];
        if (r == -1) continue; // skip read-only transactions
        int cluster_r = new_mapping[r];

        // If they are in different clusters, consider moving the lighter-loaded item
        if (cluster_s != cluster_r) {
            int load_s = cluster_loads[cluster_s];
            int load_r = cluster_loads[cluster_r];

            if (cross_count[s] < cross_count[r] && cluster_loads[cluster_r] + 1 <= max_size) {
                new_mapping[s] = cluster_r;
                cluster_loads[cluster_r]++;
                cluster_loads[cluster_s]--;
            } else if (cluster_loads[cluster_s] + 1 <= max_size) {
                new_mapping[r] = cluster_s;
                cluster_loads[cluster_s]++;
                cluster_loads[cluster_r]--;
            }
        }
    }
    
    // 5. Rebalance clusters if any cluster exceeds 30% of average
    bool balanced = false;
    while (!balanced) {
        balanced = true;
        int min_cluster = std::min_element(cluster_loads.begin(), cluster_loads.end()) - cluster_loads.begin();
        int max_cluster = std::max_element(cluster_loads.begin(), cluster_loads.end()) - cluster_loads.begin();

        if (cluster_loads[max_cluster] > max_size) {
            // Move an item with low cross-count from max_cluster to min_cluster
            int candidate = -1;
            for (const auto& [item, cluster] : new_mapping) {
                if (cluster == max_cluster && cross_count[item] == 0) {
                    candidate = item;
                    break;
                }
            }
            if (candidate != -1) {
                new_mapping[candidate] = min_cluster;
                cluster_loads[max_cluster]--;
                cluster_loads[min_cluster]++;
                balanced = false;
            }
        }
    }

    return new_mapping;
}

void PromptUser(std::unordered_map<int, std::shared_ptr<paxos::Paxos::Stub>>& stubs, 
                std::unordered_map<int, int> shard_map,
                int num_clusters,
                grpc::CompletionQueue& cq) {
    std::string input;
    std::vector<std::string> args;
    int node_id = -1;
    
    paxos::NodeInfo output;
    while (true) {
        std::cout << "Enter command to get up to and including set " << current_set_number.load() << ": " << std::endl;
        paxos::InfoRequest request;
        if (std::getline(std::cin, input)) {
            boost::algorithm::trim(input);
            boost::split(args, input, boost::is_any_of(" \t"), boost::token_compress_on);

            if (args[0] == "PrintLog") {
                if (args.size() != 2) {
                    std::cout << "Invalid input. Usage: PrintLog <node_id>\n";
                    continue;
                }
                GetNodeInfoClient call = GetNodeInfoClient(stubs[std::stoi(args[1])]);
                request.set_print_log(true);

                output = call.GetNodeInfo(request);
                std::cout << "Node " << args[1] << " - Current Log State:\n";
                std::cout << output.print_log() << std::endl;

            } else if (args[0] == "PrintDB") {
                for (const auto& [id, stub] : stubs) {
                    GetNodeInfoClient call = GetNodeInfoClient(stub);
                    request.set_print_db(true);
                    output = call.GetNodeInfo(request);
                    std::cout << "Node " << id << " - Current Database State:\n";
                    for (const auto& entry : output.database()) {
                        std::cout << "Account: " << entry.client()
                                  << ", Balance: " << entry.balance() << std::endl;
                    }
                }

            } else if (args[0] == "PrintStatus") {
                if (args.size() != 2) {
                    std::cout << "Invalid input. Usage: PrintStatus <seq_num>\n";
                    continue;
                }
                for (const auto& [id, stub] : stubs) {
                    GetNodeInfoClient call = GetNodeInfoClient(stub);
                    request.set_print_status(true);
                    request.set_seqnum(std::stoi(args[1]));
                    output = call.GetNodeInfo(request);
                    std::cout << "Node " << id << " - Current Status For SeqNum " << args[1] << ": " << output.status() << "\n";
                }

            } else if (args[0] == "PrintView") { 
                std::map<std::pair<int,int>, paxos::NewViewRequest> all_views;

                for (auto& stub : stubs) {
                    GetNodeInfoClient call = GetNodeInfoClient(stub.second);
                    request.set_print_newview(true);
                    auto output = call.GetNodeInfo(request);

                    for (const auto& nv : output.newviews()) {
                        auto key = std::make_pair(nv.ballot().counter(), nv.ballot().node_id());
                        all_views[key] = nv; // NewBiew Requests should be identical for same ballot so overwrite is fine
                    }
                }

                // iterate over conglomerate map and print
                for (auto& [key, nv] : all_views) {
                    std::map<int, paxos::AcceptedEntry> accept_log;
                    for (const auto& entry : nv.accept_log()) {
                        accept_log[entry.seqnum()] = entry;
                    }
                    std::cout << "<NEW-VIEW, (" << key.first << "," << key.second << "), "
                            << PaxosNode::AcceptLogToString(accept_log) << ">\n";
                }
                std::cout << std::endl;

            }
            else if (args[0] == "PrintBalance") {
                // printbalance of item_id x 
                if (args.size() != 2) {
                    std::cout << "Invalid input. Usage: PrintBalance <item_id>\n";
                    continue;
                }
                int item_id = std::stoi(args[1]);
                int cluster = shard_map[item_id];
                int first_node = (cluster - 1) * (stubs.size() / num_clusters) + 1;
                int last_node = cluster * (stubs.size() / num_clusters);
                for (int node_id = first_node; node_id <= last_node; ++node_id) {
                    GetNodeInfoClient call = GetNodeInfoClient(stubs[node_id]);
                    request.set_print_db(true);
                    output = call.GetNodeInfo(request);
                    std::cout << "Node " << node_id << " Balance for item " << item_id << ": ";
                    bool found = false;
                    for (const auto& entry : output.database()) {
                        if (item_id == std::stoi(entry.client())){
                            std::cout << entry.balance() << std::endl;
                            found = true;
                        }
                        
                    }
                    if (!found) {
                        std::cout << 10 << std::endl;
                    }
                }

            }

            else if (args[0] == "Performance") {
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
                double seconds = duration / 1000.0;
                int total_transactions = transactions_processed.size();
                double tps = total_transactions / seconds;

                std::cout << "Performance Metrics:\n";
                std::cout << "Throughput (transactions/second): " << tps << "\n";
                std::cout << "Latency Total Time (seconds): " << seconds << "\n";
                std::cout << "Avg Latency per txn: " << seconds / total_transactions << "\n";

            }

            else if (args[0] == "PrintReshard") {
                auto new_shard_map = Reshard(num_clusters, transactions_processed, shard_map, TOTAL_ITEMS);
                std::cout << "Items which moved shards:\n";
                paxos::ShardMap new_shard_map_proto;
                for (const auto& [item, cluster] : new_shard_map) {
                    if (new_shard_map[item] != shard_map[item]) {
                        std::cout << "(" << item << ", " << shard_map[item] << ", " << new_shard_map[item] << ")\n";
                    }
                }

                for (const auto& [item, cluster] : new_shard_map) {
                    auto entry = new_shard_map_proto.add_entry();
                    entry->set_item_id(item);
                    entry->set_cluster(cluster);
                }
                std::cout << "sending change" <<std::endl;

                for (auto& [id, stub] : stubs) {
                    new AliveAsyncCall(stub, &cq, 1, false, false, false, false, false, true, new_shard_map_proto); // reset state for all nodes
                }
            }
            else if (args[0] == "Continue") {
                return; 
            }
            else {
                std::cout << "Invalid input. Please enter PrintLog, PrintDB, PrintStatus, PrintView or Continue.\n";
            } 
        }
    }
}

bool SendMoveOn(std::shared_ptr<paxos::Paxos::Stub>& stub, int target_node_id) {
    paxos::MoveOnRequest request;
    request.set_client_id("A");

    paxos::TransactionAck reply;
    grpc::ClientContext context;

    grpc::Status status = stub->SendMoveOn(&context, request, &reply);

    if (!status.ok()) {
        std::cerr << "[Client] MoveOn RPC failed: " << status.error_message() << std::endl;
        return false;
    } else{
        std::cout << "[Client] MoveOn RPC to node " << target_node_id 
                  << " succeeded: " <<  std::endl;
    }

    return reply.accepted();
}

// CharGPT made this
void InputThread(std::unordered_map<std::string, std::unique_ptr<ClientContext>>& clients) {
    while (!stop_input.load()) {
        fd_set set;
        struct timeval timeout;

        FD_ZERO(&set);
        FD_SET(STDIN_FILENO, &set);

        timeout.tv_sec = 0;
        timeout.tv_usec = 100 * 1000; // 100ms

        int rv = select(STDIN_FILENO + 1, &set, nullptr, nullptr, &timeout);

        if (rv > 0) {
            std::string input;
            if (std::getline(std::cin, input)) {
                if (input == "Continue" || input == "continue") {
                    std::cout << "[User] requested to move on.\n";

                    // Trigger move-on logic
                    for (int node_id : active_nodes_set) SendMoveOn(stubs_[node_id], node_id);
                    for (auto& [client_id, ctx] : clients) ctx->client->HandleMoveOn();

                    stop_input = true;
                    
                    // so i can read again
                    std::cin.clear();
                    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
                    return; // exit thread
                } else {
                    std::cout << "Unrecognized input: " << input << std::endl;
                }
            }
        }

        // If select timed out, loop again and check stop_input
    }
}

// num_clusters: number of clusters
// total_items: number of data items
std::unordered_map<int, int> MakeShardMap(int total_items, int num_clusters) {
    std::unordered_map<int, int> item_to_cluster;
    item_to_cluster.reserve(total_items);

    int items_per_cluster = total_items / num_clusters;
    int remainder = total_items % num_clusters;

    int current_item = 1;
    for (int cluster = 1; cluster <= num_clusters; ++cluster) {

        // Each cluster gets items_per_cluster, and the first 'remainder' clusters get +1
        int count = items_per_cluster + (cluster <= remainder ? 1 : 0);

        for (int i = 0; i < count; ++i) {
            item_to_cluster[current_item] = cluster;
            current_item++;
        }
    }

    return item_to_cluster;
}









int main(int argc, char* argv[]) {

    if (argc != 9) {
        std::cerr << "Usage: " << argv[0] << " <num_nodes>\n";
        return 1;
    }
    std::string filename = argv[1];
    int num_nodes_per_cluster = std::stoi(argv[2]);
    int num_clusters = std::stoi(argv[3]);
    bool benchmark_mode = (std::string(argv[4]) == "true");
    int total_txns = std::stoi(argv[5]);
    float read_pct = std::stof(argv[6]);
    float cross_shard_pct = std::stof(argv[7]);
    float skewness = std::stof(argv[8]);
    
    auto r = MakeShardMap(TOTAL_ITEMS, num_clusters); // total items hardcoded for now

    if (benchmark_mode) {
        filename = "benchmark.csv";
        std::cout << "Running in benchmark mode." << std::endl;
        srand(time(nullptr));
        generate_txns(total_txns, TOTAL_ITEMS, num_clusters, {"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"}, read_pct, cross_shard_pct, skewness);
    } else {
        std::cout << "Running in normal mode." << std::endl;
    }


    std::unordered_map<std::string, std::unique_ptr<ClientContext>> clients;
    std::unordered_map<std::string, std::string> client_addresses;
    std::vector<std::string> client_ids = {"A"};

    std::unordered_map<int, std::string> node_addresses;
    node_addresses.reserve(num_nodes_per_cluster * num_clusters);

    for (int i = 1; i <= num_nodes_per_cluster * num_clusters; i++) {
        node_addresses[i] = "127.0.0.1:" + std::to_string(5000 + i);
    }


    std::unordered_map<int, pid_t> node_pid_map;
    // Fork node processes
    for (const auto& [id, addr] : node_addresses) {
        pid_t pid = fork();
        if (pid < 0) {
            perror("fork failed");
            exit(1);
        } else if (pid == 0) {
            // Child process: run a single node
            run_node(id, node_addresses, client_ids, r, num_clusters);
            std::cout << "Problem: node process exited unexpectedly." << std::endl;
            exit(0); // should never reach here
        } else {
            node_pid_map[id] = pid;
        }
    }
    std::cout << "Launched all nodes." << std::endl;

    // stubs for nodes to send alive updates
    std::unordered_map<int, std::shared_ptr<paxos::Paxos::Stub>> stubs;
    for (auto& [node_id, address] : node_addresses) {
        grpc::ChannelArguments args;

        // Allow thousands of concurrent streams per channel
        args.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, 10000); // or higher if needed
        auto channel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
        stubs[node_id] = paxos::Paxos::NewStub(channel);
    }
    stubs_ = stubs; 

    // Give nodes time to boot up
    std::this_thread::sleep_for(std::chrono::milliseconds(200)); //400


    // for async alive updates - should change to sync in future
    grpc::CompletionQueue parent_cq;
    std::atomic<bool> cq_running{true};

    // Thread to poll the CompletionQueue
    std::thread cq_thread([&]() {
        void* tag;
        bool ok;
        while (cq_running.load()) {
            if (parent_cq.Next(&tag, &ok)) {
                if (tag) {
                    auto* call = static_cast<AliveAsyncCall*>(tag);
                    call->OnComplete(ok);
                }
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200)); //400

    // create clients

    for (const auto& client_id : client_ids) {
        std::string client_address =
            "127.0.0.1:" + std::to_string(6000 + (client_id[0] - 'A') + 1);
        client_addresses[client_id] = client_address;
        int first_leader_id = 1; // assume node 1 is initial leader (may not be true)


        auto ctx = std::make_unique<ClientContext>();
        ctx->client = std::make_unique<PaxosClient>(
            client_address,
            client_id,
            first_leader_id,
            stubs, 
            node_addresses,
            num_clusters,
            r
        );

        clients[client_id] = std::move(ctx);
    }
    


    // read from file and send requests
    // text file in format: set_num, "(from, to, amount)", "[list of active nodes]"
    // nodes list example could be [1,2,3] or [2,3,4,5]

    std::this_thread::sleep_for(std::chrono::milliseconds(400)); // 800

    // ------------------- Parent reads transactions -------------------
    std::ifstream infile("../tests/test_data/" + filename);
    if (!infile) {
        std::cerr << "Failed to open test file" << std::endl;
        return 1;
    }
    // single thread reading file, so no mutex needed for clients map or elsewhere 
    // this needs a rework to support multiple threads reading different files or 1 file


    std::cout << "[Parent] Reading transactions from file..." << std::endl;
    std::string line;
    std::thread input_thread = std::thread(InputThread, ref(clients));
    int num = 0;
    while (std::getline(infile, line)) {
        if (line.empty()) continue;
        if (line[0]=='S') continue; // skip header
        typedef boost::tokenizer<boost::escaped_list_separator<char>> Tokenizer;
        Tokenizer tok(line);

        std::vector<std::string> fields;
        for (const auto& t : tok) fields.push_back(t);

        if (fields.size() != 3) {
            std::cerr << "Failed to parse line (expecting 3 fields): " << line << std::endl;
            continue;
        }

        
        // allow for prompting between sets
        if (!fields[0].empty() && current_set_number != 0) {
            std::cout << "[Parent] Waiting for client responses..." << std::endl;
            
            while (remaining_transactions.load() > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            stop_input = true;
            std::cout << "Passed the while loop" << std::endl;
            
            
            if (input_thread.joinable()) {
                input_thread.join();
                std::cout << "joined back" << std::endl;
            } else {
                std::cout << "input thread not joinable" << std::endl;
            }
            
            
            std::cout << "starting pause sequence to query user" << std::endl;
            SendAliveUpdateAsync(std::set<int>{}, stubs, parent_cq, false, false,-1, true); // prompt pause true
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            PromptUser(stubs, r, num_clusters, parent_cq);
            stop_input = false;
            input_thread = std::thread(InputThread, ref(clients));

            clients["A"]->client->reset_leaders_per_cluster(); // reset leaders to initial nodes
            transactions_processed.clear(); // clear for resharding
        }

        bool set_num_empty = false;
        // Parse set number
        if (!fields[0].empty()) {
            current_set_number = std::stoi(fields[0]);
            active_nodes_set.clear();
        } else {
            set_num_empty = true;
        }


        // Parse transaction "(from, to, amount)"
        int from_c, to_c;
        int amount;
        std::string transaction_str = fields[1];
        bool leader_fail = false;
        bool recover = false;
        int fail_node_id = -1;
        int recover_node_id = -1;
        bool read_only = false;

        if (transaction_str[0] == 'F') { // fail leader
            leader_fail = true;
            std::cout << "parseed as f" << std::endl;
            // Find 'n' and the closing ')'
            size_t n_pos = transaction_str.find('n');
            size_t end_pos = transaction_str.find(')', n_pos);
            fail_node_id = std::stoi(transaction_str.substr(n_pos + 1, end_pos - (n_pos + 1)));

        } 
        else if (transaction_str[0] == 'R') { // fail specific node
            recover = true;

            size_t n_pos = transaction_str.find('n');
            size_t end_pos = transaction_str.find(')', n_pos);
            recover_node_id = std::stoi(transaction_str.substr(n_pos + 1, end_pos - (n_pos + 1)));
        }
        else {
            boost::algorithm::trim(transaction_str);

            // Try three-integer format: (from, to, amount)
            int parsed = sscanf(transaction_str.c_str(), "(%d, %d, %d)", &from_c, &to_c, &amount);

            if (parsed == 3) {
                
            } 
            else {
                // Try single-integer format: (from)
                parsed = sscanf(transaction_str.c_str(), "(%d)", &from_c);

                if (parsed == 1) {
                    // single value parsed: treat as from_c only
                    // You can choose default values for the others
                    to_c = -1;        
                    amount = -1;
                    read_only = true;
                }
                else {
                    std::cerr << "Failed to parse transaction: " << transaction_str << std::endl;
                    continue;
                }
            }
        }
        
        std::string from_node = std::to_string(from_c);
        std::string to_node = std::to_string(to_c);


        // parse active n lst - GPT made this
        std::string nodes_str = boost::algorithm::trim_copy(fields[2]);

        if (!leader_fail and !recover) {

            if (!nodes_str.empty() && nodes_str.front() == '[' && nodes_str.back() == ']') {
                std::string inner = nodes_str.substr(1, nodes_str.size() - 2);
                std::istringstream nodes_iss(inner);
                std::string node_id_str;

                while (std::getline(nodes_iss, node_id_str, ',')) {
                    // Trim whitespace
                    node_id_str.erase(0, node_id_str.find_first_not_of(" \t\n\r"));
                    node_id_str.erase(node_id_str.find_last_not_of(" \t\n\r") + 1);

                    // Expect format like "n3" → remove the 'n' and parse the number
                    if (node_id_str.size() > 1 && node_id_str[0] == 'n') {
                        try {
                            int node_id = std::stoi(node_id_str.substr(1));
                            active_nodes_set.insert(node_id);
                        } catch (...) {
                            continue;
                        }
                    }
                }

            }
            int handling;
            if (read_pct == 1.0) {
                handling = 1000;
            }
            else {
                handling = 200;
            }
            if (num % handling == 0 && num!= 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(180));
            }

            std::cout << "Initiating transaction of " << amount
                    << " dollars from node " << from_node
                    << " to node " << to_node
                    << " in set " << current_set_number.load()
                    << " with active nodes [";
            bool first = true;
            for (int node_id : active_nodes_set) {
                if (!first) std::cout << ",";
                std::cout << node_id;
                first = false;
            }
            std::cout << "]" << 
            "with ts=" << num << std::endl;

            paxos::ClientRequest request;
            request.set_from_account(from_node);
            request.set_to_account(to_node);
            request.set_amount(amount);
            request.set_timestamp(num); // placeholder - set later
            request.set_read_only(read_only);
            num++;

            remaining_transactions++;
            std::cout << "pushing back transaction (" << from_c << ", " << to_c << ")" << std::endl;
            transactions_processed.push_back({from_c, to_c});
            StartClientRequests(from_node, clients, active_nodes_set, set_num_empty , node_addresses.size(), stubs, parent_cq, request);
        }
        else {
            // leader fail: wait for preceding transactions to finish, then kill or recover 
            std::cout << "fail or reco - waiting" << std::endl;

            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            SendAliveUpdateAsync(active_nodes_set, stubs, parent_cq, leader_fail, recover, leader_fail ? fail_node_id : recover_node_id); 
            std::this_thread::sleep_for(std::chrono::milliseconds(300));

        }


    }

    //allow for prompting adter last set
    while (remaining_transactions.load() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "All transactions processed." << std::endl;

    // close the input thread 
    stop_input = true;
    if (input_thread.joinable()) {
        input_thread.join();
        std::cout << "joined back" << std::endl;
    } else {
        std::cout << "input thread not joinable" << std::endl;
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    SendAliveUpdateAsync(std::set<int>{}, stubs, parent_cq); 
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    PromptUser(stubs, r, num_clusters, parent_cq);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cout << "Shutting down..." << std::endl;
    

// SHUTDOWN
for (const auto& [id, pid] : node_pid_map) {
    if (kill(pid, SIGKILL) != 0) {
        perror("Failed to send SIGKILL to node process");
    } else {
        continue;
    }
}

// ensures that the killed processes are reaped
for (const auto& [id, pid] : node_pid_map) {
    int status;
    if (waitpid(pid, &status, 0) == pid) {
        continue;
    } else {
        perror("waitpid failed");
    }
}

// Shutdown the gRPC completion queue and thread
cq_running = false;
parent_cq.Shutdown();
if (cq_thread.joinable()) cq_thread.join();

clients.clear();

std::cout << "Cleanup complete. Exiting." << std::endl;
}
