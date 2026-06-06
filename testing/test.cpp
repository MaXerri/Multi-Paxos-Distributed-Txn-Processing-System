#include <grpcpp/grpcpp.h>
#include <cstdlib>

int main(){
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:5010", grpc::InsecureServerCredentials(), &port);
    std::cout << "port bound: " << port << std::endl;
}

