
//
// Created by Haoyu Huang on 12/24/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova_client_sock.h"
#include "nova_mem_config.h"

namespace nova {
    NovaClientSock::NovaClientSock() {
        send_buf_ = new char[NovaConfig::config->max_msg_size];
        recv_buf_ = new char[NovaConfig::config->max_msg_size];
    }

    void NovaClientSock::Connect(const Host &host) {
        struct sockaddr_in serv_addr;
        RDMA_ASSERT((sockfd_ = socket(AF_INET, SOCK_STREAM, 0)) >= 0)
            << "socket creation error";
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        {
            auto server = gethostbyname(host.ip.c_str());
            memcpy((char *) &serv_addr.sin_addr.s_addr,
                   (char *) server->h_addr,
                   server->h_length);
        }
        serv_addr.sin_port = htons(host.port);
        int ret = connect(sockfd_, (struct sockaddr *) &serv_addr,
                          sizeof(serv_addr));
        while (ret != 0) {
            usleep(10000);
            ret = connect(sockfd_, (struct sockaddr *) &serv_addr,
                          sizeof(serv_addr));
        }
    }

    void NovaClientSock::Send(char* send_buf, int size) {
        if (send_buf == nullptr) {
            send_buf = send_buf_;
        }
        int written = 0;
        do {
            char *buf = send_buf + written;
            int n = write(sockfd_, buf, size - written);
            if (n > 0) {
                written += n;
            }
        } while (written != size);
    }

    int NovaClientSock::Receive() {
        char *buf = recv_buf_;
        // read at much as possible for the first time.
        int readBytes = read(sockfd_, recv_buf_,
                             NovaConfig::config->max_msg_size);
        if (readBytes > 0) {
            buf += readBytes;
        }
        // read one byte at a time until we know the length.
        do {
            int ret = read(sockfd_, buf, 1);
            if (ret == 1) {
                readBytes++;
                buf++;
            }
        } while (readBytes >= 4);
        uint32_t size = leveldb::DecodeFixed32(recv_buf_);
        buf = recv_buf_ + 4;
        int count = 0;
        // read the remaining bytes.
        while (count < size) {
            int cnt = read(sockfd_, buf, size - count);
            if (cnt > 0) {
                count += cnt;
                buf += cnt;
            }
        }
        return size;
    }

}