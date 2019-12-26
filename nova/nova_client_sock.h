
//
// Created by Haoyu Huang on 12/24/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_CLIENT_SOCK_H
#define LEVELDB_NOVA_CLIENT_SOCK_H


#include "nova_common.h"

namespace nova {
    class NovaClientSock {
    public:
        NovaClientSock();

        void Connect(const Host &host);

        void Send(char *send, int size);

        int Receive();

        char *send_buf() { return send_buf_; }

        char *recv_buf() { return recv_buf_; }

    private:
        int sockfd_ = 0;
        char *send_buf_;
        char *recv_buf_;
    };
}


#endif //LEVELDB_NOVA_CLIENT_SOCK_H
