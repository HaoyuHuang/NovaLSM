
//
// Created by Haoyu Huang on 12/24/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova_client_sock.h"
#include "nova_config.h"

namespace nova {

    namespace {
        std::string host_to_ip(const std::string &host) {
            std::string res = "";
            struct addrinfo hints, *infoptr;
            memset(&hints, 0, sizeof hints);
            hints.ai_family = AF_INET; // AF_INET means IPv4 only addresses

            int result = getaddrinfo(host.c_str(), NULL, &hints, &infoptr);
            if (result) {
                fprintf(stderr, "getaddrinfo: %s at %s\n", gai_strerror(result),
                        host.c_str());
                return "";
            }
            char ip[64];
            memset(ip, 0, sizeof(ip));

            for (struct addrinfo *p = infoptr; p != NULL; p = p->ai_next) {
                getnameinfo(p->ai_addr, p->ai_addrlen, ip, sizeof(ip), NULL, 0,
                            NI_NUMERICHOST);
            }
            res = std::string(ip);
            return res;
        }
    }

    NovaClientSock::NovaClientSock() {
        send_buf_ = new char[NovaConfig::config->max_msg_size];
        recv_buf_ = new char[NovaConfig::config->max_msg_size];
    }

    void NovaClientSock::Connect(const Host &host) {
        while (true) {
            struct sockaddr_in serv_addr;
            sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
            NOVA_ASSERT(sockfd_ >= 0) << "socket creation error";
            memset(&serv_addr, 0, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(host.port);

            auto ip = host_to_ip(host.ip);
            NOVA_ASSERT(ip != "");
            serv_addr.sin_addr.s_addr = inet_addr(ip.c_str());
            if (connect(sockfd_, (struct sockaddr *) &serv_addr,
                        sizeof(serv_addr)) == 0) {
                break;
            }
            close(sockfd_);
            usleep(10000);
        }
        NOVA_LOG(DEBUG) << "Socket " << sockfd_ << " connected to host "
                        << host.ip << ":" << host.port;
    }

    void NovaClientSock::Send(char *send_buf, int size) {
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
            NOVA_ASSERT(n >= 0) << "write error:" << strerror(errno) << " "
                                << sockfd_;
        } while (written != size);
    }

    int NovaClientSock::Receive() {
        char *buf = recv_buf_;
        // read at much as possible for the first time.
        int readBytes = read(sockfd_, buf, NovaConfig::config->max_msg_size);
        if (readBytes > 0) {
            buf += readBytes;
        }
        // read one byte at a time until we know the length.
        while (readBytes < 4) {
            int ret = read(sockfd_, buf, 1);
            if (ret == 1) {
                readBytes++;
                buf++;
            }
        }
        uint32_t size = leveldb::DecodeFixed32(recv_buf_);
        int count = readBytes - 4;
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