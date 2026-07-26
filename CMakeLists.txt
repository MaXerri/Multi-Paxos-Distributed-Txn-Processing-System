cmake_minimum_required(VERSION 3.15)

set(CMAKE_C_COMPILER "/opt/homebrew/opt/llvm/bin/clang")
set(CMAKE_CXX_COMPILER "/opt/homebrew/opt/llvm/bin/clang++")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

project(PaxosGRPC CXX)

# Trace logging is OFF by default (LOG statements compiled out for zero overhead).
# Enable with: cmake -S . -B build_trace -DPAXOS_TRACE=ON
option(PAXOS_TRACE "Compile in LOG trace statements" OFF)

find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)
find_package(SQLite3 REQUIRED)

include_directories(${CMAKE_SOURCE_DIR} /opt/homebrew/include /opt/homebrew/opt/libpq/include)

add_executable(paxos_node
    src/node_launch.cpp
    src/paxos_node.cpp
    src/call_data.cpp
    src/paxos_client.cpp
    src/benchmark.h
    grpc/paxos.pb.cc
    grpc/paxos.grpc.pb.cc
)
if(PAXOS_TRACE)
    target_compile_definitions(paxos_node PRIVATE PAXOS_TRACE)
endif()

# Find where Homebrew installed libc++:
find_library(HOMEBREW_LIBCXX c++ PATHS /opt/homebrew/opt/llvm/lib)

target_link_libraries(paxos_node
    gRPC::grpc++
    protobuf::libprotobuf
    SQLite::SQLite3
    ${HOMEBREW_LIBCXX}   # force Homebrew libc++
    /opt/homebrew/opt/libpq/lib/libpq.dylib  # PostgreSQL client library

)