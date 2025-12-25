#pragma once

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <random>
#include <cmath>
#include <algorithm>
#include <chrono>

using namespace std;

struct Txn {
    bool isRead;
    int src;
    int dst;    // 0 for read
    int amount;
};

//----------------------------------------------------------------------
// Random engine (seeded once per program run)
inline std::mt19937 &global_rng() {
    static std::mt19937 rng((unsigned)std::chrono::high_resolution_clock::now().time_since_epoch().count());
    return rng;
}

// Uniform sampling in [0, N-1]
inline int sample_uniform(int N) {
    if (N <= 1) return 0;
    std::uniform_int_distribution<int> d(0, N - 1);
    return d(global_rng());
}

// Sample from a Gaussian centered at `hotspot` with standard deviation `sigma`.
// Reflect out-of-bound values to prevent edge bias.
inline int sample_gaussian_centered(int N, int hotspot, double sigma) {
    if (sigma <= 0.0) return hotspot % N;
    std::normal_distribution<double> dist(0.0, sigma);
    double off = dist(global_rng());
    long long val = static_cast<long long>(std::llround(hotspot + off));

    // Reflect edges instead of clamping
    while (val < 0 || val >= N) {
        if (val < 0) val = -val;
        if (val >= N) val = 2 * N - val - 2;
    }
    return static_cast<int>(val);
}

// Choose an account id in range [1..numAccounts] with skew control
inline int choose_account(int numAccounts, double skewness, int hotspot) {
    if (numAccounts <= 1) return 1;

    if (skewness <= 0.0) return sample_uniform(numAccounts) + 1;

    if (skewness >= 1.0) {
        // Fully Gaussian with very small sigma
        double sigma = 1.0; 
        return sample_gaussian_centered(numAccounts, hotspot, sigma) + 1;
    }

    // Mix uniform vs Gaussian
    std::uniform_real_distribution<double> prob(0.0, 1.0);
    if (prob(global_rng()) > skewness) {
        return sample_uniform(numAccounts) + 1;
    }

    // Gaussian sigma: now collapse more aggressively
    // Previously sigma ~ (1 - skew); now sigma ~ (1 - skew)^2 to amplify skew
    double baseSpread = numAccounts / 4.0;
    double sigma = std::max(1.0, baseSpread * (1.0 - skewness) * (1.0 - skewness));
    return sample_gaussian_centered(numAccounts, hotspot, sigma) + 1;
}

// Write CSV
inline void write_csv(const string &filename, const vector<Txn> &txns, const vector<string> &liveNodes) {
    ofstream out(filename);
    if (!out.is_open()) {
        cerr << "Failed to open " << filename << " for writing\n";
        return;
    }

    out << "Set Number,Transactions,Live Nodes\n";

    if (txns.empty()) {
        out << "1,\"\",\"[";
    } else {
        out << "1,";
        if (txns[0].isRead) out << "\"(" << txns[0].src << ")\",";
        else out << "\"(" << txns[0].src << ", " << txns[0].dst << ", " << txns[0].amount << ")\",";
        out << "\"[";
    }

    for (size_t j = 0; j < liveNodes.size(); ++j) {
        out << liveNodes[j];
        if (j + 1 < liveNodes.size()) out << ", ";
    }
    out << "]\"\n";

    for (size_t t = 1; t < txns.size(); ++t) {
        out << ",";
        if (txns[t].isRead) out << "\"(" << txns[t].src << ")\",";
        else out << "\"(" << txns[t].src << ", " << txns[t].dst << ", " << txns[t].amount << ")\",";
        out << "\n";
    }

    out.close();
}

// Generate transactions
inline vector<Txn> generate_txns(
    int totalTxns,
    int numAccounts,
    int numShards,
    const vector<string> &liveNodes,
    double readOnlyPct,
    double crossShardPct,
    double skewness,
    const string &out_csv_path = "../test_data/benchmark.csv"
) {
    vector<Txn> txns;
    txns.reserve(max(0, totalTxns));
    if (numAccounts <= 0) return txns;

    std::uniform_int_distribution<int> hotspot_dist(1, max(1, numAccounts - 2));
    int hotspot = hotspot_dist(global_rng());

    int shardSize = (numAccounts + numShards - 1) / numShards;
    auto getShard = [&](int acct1based) {
        int idx = acct1based - 1;
        return idx / max(1, shardSize);
    };

    vector<int> freq(numAccounts, 0);
    std::uniform_real_distribution<double> uniform01(0.0, 1.0);
    std::uniform_int_distribution<int> amount_dist(1, 5);

    for (int i = 0; i < totalTxns; ++i) {
        bool isRead = (uniform01(global_rng()) < readOnlyPct);
        bool forceCross = (uniform01(global_rng()) < crossShardPct);

        int src = choose_account(numAccounts, skewness, hotspot);
        int dst = choose_account(numAccounts, skewness, hotspot);

        if (!isRead) {
            int srcShard = getShard(src);
            int dstShard = getShard(dst);

            if (forceCross) {
                for (int tries = 0; tries < 4 && dstShard == srcShard; ++tries) {
                    dst = sample_uniform(numAccounts) + 1;
                    dstShard = getShard(dst);
                }
                if (dstShard == srcShard) {
                    int targetShard = (srcShard + 1) % numShards;
                    int start = targetShard * shardSize + 1;
                    int end = min(numAccounts, start + shardSize - 1);
                    if (start <= end) dst = sample_uniform(end - start + 1) + start;
                    else dst = src; // fallback
                }
            } else {
                for (int tries = 0; tries < 4 && dstShard != srcShard; ++tries) {
                    dst = sample_uniform(numAccounts) + 1;
                    dstShard = getShard(dst);
                }
                if (dstShard != srcShard) {
                    int start = srcShard * shardSize + 1;
                    int end = min(numAccounts, start + shardSize - 1);
                    if (start <= end) dst = sample_uniform(end - start + 1) + start;
                    else dst = src; // fallback
                }
            }
        } else dst = 0;

        freq[src - 1]++;
        if (isRead) txns.push_back({true, src, 0, 0});
        else {
            int amount = amount_dist(global_rng());
            txns.push_back({false, src, dst, amount});
        }
    }

    // Print distribution stats
    cout << "\n=== Distribution Statistics ===\n";
    cout << "numAccounts=" << numAccounts
         << " numShards=" << numShards
         << " hotspot=" << hotspot
         << " skewness=" << skewness << "\n";

    long long total = 0;
    int minIdx = -1, maxIdx = -1;
    for (int i = 0; i < numAccounts; ++i) {
        if (freq[i] > 0) {
            if (minIdx == -1) minIdx = i + 1;
            maxIdx = i + 1;
        }
        total += freq[i];
    }
    double meanIdx = 0.0;
    if (total > 0) {
        long long sumIdx = 0;
        for (int i = 0; i < numAccounts; ++i) sumIdx += (long long)(i + 1) * freq[i];
        meanIdx = double(sumIdx) / double(total);
    }

    cout << "Mean account index (weighted): " << meanIdx << "\n";

    vector<pair<int,int>> sv;
    sv.reserve(numAccounts);
    for (int i = 0; i < numAccounts; ++i) sv.emplace_back(freq[i], i + 1);
    sort(sv.begin(), sv.end(), [](auto &a, auto &b){ return a.first > b.first; });

    cout << "Top accounts (by src frequency):\n";
    for (size_t k = 0; k < sv.size() && k < 10; ++k) {
        if (sv[k].first == 0) break;
        cout << "  Account " << sv[k].second << " -> " << sv[k].first << " occurrences\n";
    }
    cout << "============================================\n\n";

    // Remove self-transfer transactions
    txns.erase(std::remove_if(txns.begin(), txns.end(),
                            [](const Txn &t) { return !t.isRead && t.src == t.dst; }),
            txns.end());
    write_csv(out_csv_path, txns, liveNodes);
    return txns;
}