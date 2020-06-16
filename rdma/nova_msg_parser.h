
//
// Created by Haoyu Huang on 2/24/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MSG_PARSER_H
#define RLIB_NOVA_MSG_PARSER_H

#include "rdma_ctrl.hpp"

namespace nova {
    using namespace std;
    using namespace rdmaio;

#define REQ_TERMINATER_CHAR '!'
#define REQ_BATCH_DELIMITER_CHAR ';'
#define REQ_DELIMITER_CHAR '?'
#define NOVA_REQ_GET 'g'
#define NOVA_REQ_GET_MISS 'M'
#define NOVA_REQ_GET_HIT 'H'
#define NOVA_REQ_SET 's'
#define NOVA_REQ_SET_RESPONSE 'S'

    class Request {
    public:
        Request() {}

        Request(char cmd, const string &key, const string &value) : cmd(cmd),
                                                                    key(key),
                                                                    value(value) {}

        char cmd;
        string key;
        string value;
    };

    class RequestParser {
    public:
        static Request parse_request(char *buf) {
            char cmd = buf[0];
            int len = 0;
            for (;;) {
                if (buf[len] == REQ_TERMINATER_CHAR) {
                    break;
                }
                len++;
            }
            len++;

            if (cmd == NOVA_REQ_GET) {
                char *key_str = buf + 1;
                auto key = std::string(key_str, len - 2);
                NOVA_LOG(DEBUG) << "Received get request " << key;
                return Request(NOVA_REQ_GET, key, "");
            } else if (cmd == NOVA_REQ_SET) {
                char *key_str = buf + 1;
                char *value_str = buf + 1;
                int nkey = 0;
                while (value_str[0] != REQ_DELIMITER_CHAR) {
                    value_str++;
                    nkey++;
                }
                value_str++;
                int nval = len - nkey - 1 - 1 - 1;
                auto key = std::string(key_str, nkey);
                auto value = std::string(value_str, nval);
                NOVA_LOG(DEBUG) << "Received put request " << key << " "
                                << value;
                return Request(NOVA_REQ_SET, key, value);
            }
            NOVA_ASSERT(false) << "unknow request " << buf;
            return Request();
        }

        static vector<Request> parse_multi_request(char *buf) {
            vector<Request> requests;
            int len = 0;
            char *tmpbuf = buf;
            while (tmpbuf[len] != REQ_TERMINATER_CHAR) {
                if (tmpbuf[len] == REQ_BATCH_DELIMITER_CHAR) {
                    char cmd = tmpbuf[0];
                    if (cmd == NOVA_REQ_GET) {
                        char *key_str = tmpbuf + 1;
                        auto key = std::string(key_str, len - 1);
                        NOVA_LOG(DEBUG) << "Received get request " << key;
                        requests.push_back(Request(NOVA_REQ_GET, key, ""));
                    } else if (cmd == NOVA_REQ_SET) {
                        char *key_str = tmpbuf + 1;
                        char *value_str = tmpbuf + 1;
                        int nkey = 0;
                        while (value_str[0] != REQ_DELIMITER_CHAR) {
                            value_str++;
                            nkey++;
                        }
                        value_str++;
                        int nval = len - nkey - 1 - 1 - 1;
                        auto key = std::string(key_str, nkey);
                        auto value = std::string(value_str, nval);
                        NOVA_LOG(DEBUG) << "Received put request " << key << " "
                                        << value;
                        requests.push_back(Request(NOVA_REQ_SET, key, value));
                    } else {
                        NOVA_ASSERT(false) << "unknown request " << buf;
                    }
                    tmpbuf += len + 1;
                    len = 0;
                    continue;
                }
                len++;
            }
            return requests;
        }
    };
}
#endif //RLIB_NOVA_MSG_PARSER_H
