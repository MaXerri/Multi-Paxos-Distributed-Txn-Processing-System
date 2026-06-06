# Distributed Transaction Processing System

This project implements a distributed transaction processing system using Multi-Paxos-based consensus within clusters and Two-Phase Commit (2PC) for handling cross-shard transactions.

## Overview

- **Consensus:** Each cluster uses Multi-Paxos to agree on transaction order.
- **Sharding:** Data is partitioned across multiple clusters (shards).
- **Cross-Shard Transactions:** Transactions spanning multiple clusters use 2PC for atomicity.
- **Benchmarking:** A generation engine to test performance under different workloads

## System Design

![System Design](sys_design.png)

We use a sharded structure with c clusters. Nodes within a single cluster replicate a shard — a subset of the system state — so each cluster is a replica group responsible for that shard. Intra-cluster agreement is achieved with Multi-Paxos; cross-shard (cross-cluster) transactions are coordinated using Two-Phase Commit.

## How to Run

Before running the project, ensure that you have consistent versions of **protobuf** and **gRPC** installed on your system. Mismatched versions may cause build or runtime errors. Refer to your language's documentation for installation and version management instructions.

Build the program via

```bash
mkdir build && cd build 
cmake ..
make -j4
```

## Usage

```bash
./paxos_node <testfile> <num_nodes_per_cluster> <num_clusters> <benchmark (0|1)> <num_txns_for_bench> <readonly_frac> <x-shard_frac> <skew>
```

- `<testfile>`: Path to the input file with transaction definitions.
- `<num_nodes_per_cluster>`: Number of nodes in each cluster.
- `<num_clusters>`: Number of clusters (shards).
- `<benchmark>`: Set to 1 to enable benchmarking, 0 otherwise.
- `<num_txns_for_bench>`: Number of transactions to run in benchmark mode.
- `<readonly_frac>`: Fraction of transactions that are read-only (0.0 - 1.0).
- `<x-shard_frac>`: Fraction of transactions that are cross-shard (0.0 - 1.0).
- `<skew>`: Skew parameter for transaction distribution.



## Features

- Multi-Paxos consensus for intra-cluster agreement
- Two-Phase Commit for cross-shard atomicity
- Configurable benchmarking and workload parameters
- Custom cluster configurations
- Resharding via hypergraph partitioning

## Bonuses Completed

- Custom clusters

## File Structure

- `paxos_node`: Main executable for running a node in the system
- `README.md`: Project documentation

## References

- [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)
- [Two-Phase Commit Protocol](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)




