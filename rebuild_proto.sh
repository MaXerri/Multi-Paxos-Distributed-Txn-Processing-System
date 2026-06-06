#!/bin/zsh

# scipt to rebuilt the proto file   

PROTO_DIR=./grpc
PROTO_FILE=paxos.proto
OUT_DIR=./grpc

mkdir -p $OUT_DIR

protoc -I=$PROTO_DIR \
       --cpp_out=$OUT_DIR \
       --grpc_out=$OUT_DIR \
       --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` \
       $PROTO_DIR/$PROTO_FILE

echo "Proto files rebuilt!"