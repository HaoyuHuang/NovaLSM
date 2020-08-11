
//
// Created by Haoyu Huang on 4/1/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <sys/stat.h>
#include "nova_common.h"

namespace nova {
    // random vector from http://home.comcast.net/~bretm/hash/10.html
    static const uint32_t sbox[] =
            {
                    0xF53E1837, 0x5F14C86B, 0x9EE3964C, 0xFA796D53,
                    0x32223FC3, 0x4D82BC98, 0xA0C7FA62, 0x63E2C982,
                    0x24994A5B, 0x1ECE7BEE, 0x292B38EF, 0xD5CD4E56,
                    0x514F4303, 0x7BE12B83, 0x7192F195, 0x82DC7300,
                    0x084380B4, 0x480B55D3, 0x5F430471, 0x13F75991,
                    0x3F9CF22C, 0x2FE0907A, 0xFD8E1E69, 0x7B1D5DE8,
                    0xD575A85C, 0xAD01C50A, 0x7EE00737, 0x3CE981E8,
                    0x0E447EFA, 0x23089DD6, 0xB59F149F, 0x13600EC7,
                    0xE802C8E6, 0x670921E4, 0x7207EFF0, 0xE74761B0,
                    0x69035234, 0xBFA40F19, 0xF63651A0, 0x29E64C26,
                    0x1F98CCA7, 0xD957007E, 0xE71DDC75, 0x3E729595,
                    0x7580B7CC, 0xD7FAF60B, 0x92484323, 0xA44113EB,
                    0xE4CBDE08, 0x346827C9, 0x3CF32AFA, 0x0B29BCF1,
                    0x6E29F7DF, 0xB01E71CB, 0x3BFBC0D1, 0x62EDC5B8,
                    0xB7DE789A, 0xA4748EC9, 0xE17A4C4F, 0x67E5BD03,
                    0xF3B33D1A, 0x97D8D3E9, 0x09121BC0, 0x347B2D2C,
                    0x79A1913C, 0x504172DE, 0x7F1F8483, 0x13AC3CF6,
                    0x7A2094DB, 0xC778FA12, 0xADF7469F, 0x21786B7B,
                    0x71A445D0, 0xA8896C1B, 0x656F62FB, 0x83A059B3,
                    0x972DFE6E, 0x4122000C, 0x97D9DA19, 0x17D5947B,
                    0xB1AFFD0C, 0x6EF83B97, 0xAF7F780B, 0x4613138A,
                    0x7C3E73A6, 0xCF15E03D, 0x41576322, 0x672DF292,
                    0xB658588D, 0x33EBEFA9, 0x938CBF06, 0x06B67381,
                    0x07F192C6, 0x2BDA5855, 0x348EE0E8, 0x19DBB6E3,
                    0x3222184B, 0xB69D5DBA, 0x7E760B88, 0xAF4D8154,
                    0x007A51AD, 0x35112500, 0xC9CD2D7D, 0x4F4FB761,
                    0x694772E3, 0x694C8351, 0x4A7E3AF5, 0x67D65CE1,
                    0x9287DE92, 0x2518DB3C, 0x8CB4EC06, 0xD154D38F,
                    0xE19A26BB, 0x295EE439, 0xC50A1104, 0x2153C6A7,
                    0x82366656, 0x0713BC2F, 0x6462215A, 0x21D9BFCE,
                    0xBA8EACE6, 0xAE2DF4C1, 0x2A8D5E80, 0x3F7E52D1,
                    0x29359399, 0xFEA1D19C, 0x18879313, 0x455AFA81,
                    0xFADFE838, 0x62609838, 0xD1028839, 0x0736E92F,
                    0x3BCA22A3, 0x1485B08A, 0x2DA7900B, 0x852C156D,
                    0xE8F24803, 0x00078472, 0x13F0D332, 0x2ACFD0CF,
                    0x5F747F5C, 0x87BB1E2F, 0xA7EFCB63, 0x23F432F0,
                    0xE6CE7C5C, 0x1F954EF6, 0xB609C91B, 0x3B4571BF,
                    0xEED17DC0, 0xE556CDA0, 0xA7846A8D, 0xFF105F94,
                    0x52B7CCDE, 0x0E33E801, 0x664455EA, 0xF2C70414,
                    0x73E7B486, 0x8F830661, 0x8B59E826, 0xBB8AEDCA,
                    0xF3D70AB9, 0xD739F2B9, 0x4A04C34A, 0x88D0F089,
                    0xE02191A2, 0xD89D9C78, 0x192C2749, 0xFC43A78F,
                    0x0AAC88CB, 0x9438D42D, 0x9E280F7A, 0x36063802,
                    0x38E8D018, 0x1C42A9CB, 0x92AAFF6C, 0xA24820C5,
                    0x007F077F, 0xCE5BC543, 0x69668D58, 0x10D6FF74,
                    0xBE00F621, 0x21300BBE, 0x2E9E8F46, 0x5ACEA629,
                    0xFA1F86C7, 0x52F206B8, 0x3EDF1A75, 0x6DA8D843,
                    0xCF719928, 0x73E3891F, 0xB4B95DD6, 0xB2A42D27,
                    0xEDA20BBF, 0x1A58DBDF, 0xA449AD03, 0x6DDEF22B,
                    0x900531E6, 0x3D3BFF35, 0x5B24ABA2, 0x472B3E4C,
                    0x387F2D75, 0x4D8DBA36, 0x71CB5641, 0xE3473F3F,
                    0xF6CD4B7F, 0xBF7D1428, 0x344B64D0, 0xC5CDFCB6,
                    0xFE2E0182, 0x2C37A673, 0xDE4EB7A3, 0x63FDC933,
                    0x01DC4063, 0x611F3571, 0xD167BFAF, 0x4496596F,
                    0x3DEE0689, 0xD8704910, 0x7052A114, 0x068C9EC5,
                    0x75D0E766, 0x4D54CC20, 0xB44ECDE2, 0x4ABC653E,
                    0x2C550A21, 0x1A52C0DB, 0xCFED03D0, 0x119BAFE2,
                    0x876A6133, 0xBC232088, 0x435BA1B2, 0xAE99BBFA,
                    0xBB4F08E4, 0xA62B5F49, 0x1DA4B695, 0x336B84DE,
                    0xDC813D31, 0x00C134FB, 0x397A98E6, 0x151F0E64,
                    0xD9EB3E69, 0xD3C7DF60, 0xD2F2C336, 0x2DDD067B,
                    0xBD122835, 0xB0B3BD3A, 0xB0D54E46, 0x8641F1E4,
                    0xA0B38F96, 0x51D39199, 0x37A6AD75, 0xDF84EE41,
                    0x3C034CBA, 0xACDA62FC, 0x11923B8B, 0x45EF170A,
            };
    static uint64_t g_seed = 101;

    uint32_t fastrand() {
        g_seed = (214013 * g_seed + 2531011);
        return (g_seed >> 16) & 0x7FFF;
    }

    std::string Host::DebugString() const {
        return fmt::format("{}:{}:{}", server_id, ip, port);
    }


    uint64_t tab_hash(const char *key, size_t len) {
        // a large prime number
        uint32_t h = 4294967291U;
        while (len) {
            len--;
            // tabulation hashing -- Carter and Wegman (STOC'77)
            h ^= sbox[*key];
            key++;
        }
        return (uint64_t) h;
    }

    uint64_t sbox_hash(char *key, size_t len) {
        // a large prime number
        uint32_t h = 4294967291U;
        while (len) {
            len--;
            h ^= sbox[*key];
            h *= 3;
            key++;
        }
        return (uint64_t) h;
    }

    uint64_t noop_hash(const char *key, size_t len) {
//    assert(len == sizeof(uint64_t));
//    (void) len;
        return *(uint64_t *) key;
    }

    uint64_t mul_hash(const char *key, size_t len) {
//    assert(len == sizeof(uint64_t));
//    (void) len;
        // a large prime number
        return *(uint64_t *) key * 18446744073709551557UL;
    }

// MD4 truncated to 12 B
//#include <openssl/md4.h>
//
//static uint64_t hash_md4(const char *key, size_t len) {
//    size_t temp_hash[(MD4_DIGEST_LENGTH + sizeof(size_t) - 1) / sizeof(size_t)];
//    MD4(key, len, (uint8_t *) temp_hash);
//    assert(8 <= MD4_DIGEST_LENGTH);
//    return *(size_t *) temp_hash;
//}

    uint64_t cityhash(const char *key, size_t len) {
        return CityHash64((const char *) key, len);
    }


    uint32_t safe_mod(uint32_t key, uint32_t n) {
        if (n <= 1) {
            return 0;
        }
        return key % n;
    }

    void MarkCharAsWaitingForRDMAWRITE(char *ptr, uint32_t size) {
        ptr[size - 1] = 0;
    }

    bool IsRDMAWRITEComplete(char *ptr, uint32_t size) {
        return true;
//        return ptr[size - 1] != 0;
    }

    uint32_t nint_to_str(uint64_t x) {
        uint32_t len = 0;
        do {
            x = x / 10;
            len++;
        } while (x);
        return len;
    }

    std::vector<std::string>
    SplitByDelimiter(std::string *s, std::string delimiter) {
        size_t pos = 0;
        std::string token;
        std::vector<std::string> tokens;
        while ((pos = s->find(delimiter)) != std::string::npos) {
            token = s->substr(0, pos);
            tokens.push_back(token);
            s->erase(0, pos + delimiter.length());
        }
        if (!s->empty()) {
            tokens.push_back(*s);
        }
        return tokens;
    }

    std::vector<uint32_t>
    SplitByDelimiterToInt(std::string *s, std::string delimiter) {
        size_t pos = 0;
        std::string token;
        std::vector<uint32_t> tokens;
        while ((pos = s->find(delimiter)) != std::string::npos) {
            token = s->substr(0, pos);
            tokens.push_back(std::stoi(token));
            s->erase(0, pos + delimiter.length());
        }
        if (!s->empty()) {
            tokens.push_back(std::stoi(*s));
        }
        return tokens;
    }

    std::string ToString(const std::vector<uint32_t> &x) {
        std::string str;
        for (int i = 0; i < x.size(); i++) {
            str += std::to_string(x[i]);
            str += ",";
        }
        return str;
    }

    LTCFragment::LTCFragment() : is_ready_(false),
                                 is_stoc_migrated_(false),
                                 is_ready_signal_(&is_ready_mutex_), is_complete_(false) {
    }

    std::string LTCFragment::DebugString() {
        return fmt::format("[{},{}): {}-{}", range.key_start, range.key_end, ltc_server_id, dbid);
    }

    std::string
    DBName(const std::string &dbname, uint32_t index) {
        return dbname + "/" + std::to_string(index);
    }

    void ParseDBIndexFromDBName(const std::string &dbname, uint32_t *index) {
        int iend = dbname.size() - 1;
        int istart = dbname.find_last_of('/') + 1;
        uint64_t i64;
        str_to_int(dbname.data() + istart, &i64, iend - istart + 1);
        *index = i64;
    }

    void mkdirs(const char *dir) {
        char tmp[1024];
        char *p = NULL;
        size_t len;

        snprintf(tmp, sizeof(tmp), "%s", dir);
        len = strlen(tmp);
        if (tmp[len - 1] == '/')
            tmp[len - 1] = 0;
        for (p = tmp + 1; *p; p++) {
            if (*p == '/') {
                *p = 0;
                mkdir(tmp, 0777);
                *p = '/';
            }
        }
        mkdir(tmp, 0777);
    }

    std::string ibv_wr_opcode_str(ibv_wr_opcode code) {
        switch (code) {
            case IBV_WR_RDMA_WRITE:
                return "WRITE";
            case IBV_WR_RDMA_WRITE_WITH_IMM:
                return "WRITE_IMM";
            case IBV_WR_SEND:
                return "SEND";
            case IBV_WR_SEND_WITH_IMM:
                return "SEND_IMM";
            case IBV_WR_RDMA_READ:
                return "READ";
            case IBV_WR_ATOMIC_CMP_AND_SWP:
                return "CAS";
            case IBV_WR_ATOMIC_FETCH_AND_ADD:
                return "FA";
        }
    }

    std::string ibv_wc_opcode_str(ibv_wc_opcode code) {
        switch (code) {
            case IBV_WC_SEND:
                return "SEND";
            case IBV_WC_RDMA_WRITE:
                return "WRITE";
            case IBV_WC_RDMA_READ:
                return "READ";
            case IBV_WC_COMP_SWAP:
                return "CAS";
            case IBV_WC_FETCH_ADD:
                return "FA";
            case IBV_WC_BIND_MW:
                return "MW";
            case IBV_WC_RECV:
                return "RECV";
            case IBV_WC_RECV_RDMA_WITH_IMM:
                return "RECV_IMM";
        }
    }

//inline __attribute__ ((always_inline))
    uint32_t int_to_str(char *str, uint64_t x) {
        uint32_t len = 0, p = 0;
        do {
            str[len] = static_cast<char>((x % 10) + '0');
            x = x / 10;
            len++;
        } while (x);
        int q = len - 1;
        char temp;
        while (p < q) {
            temp = str[p];
            str[p] = str[q];
            str[q] = temp;
            p++;
            q--;
        }
        str[len] = TERMINATER_CHAR;
        return len + 1;
    }

    int
    GenerateRDMARequest(RequestType req_type, char *buf,
                        uint64_t from_server_id,
                        uint64_t from_sock_fd, char *key, uint64_t nkey) {
        int size = 0;
        buf[0] = req_type;
        buf++;
        size++;
        int n = int_to_str(buf, from_server_id);
        buf += n;
        size += n;
        n = int_to_str(buf, from_sock_fd);
        buf += n;
        size += n;
        n = int_to_str(buf, nkey);
        buf += n;
        size += n;
        memcpy(buf, key, nkey);
        size += nkey;
        return size;
    }

    uint32_t
    ParseRDMARequest(char *buf, RequestType *req_type, uint64_t *from_server_id,
                     uint64_t *from_sock_fd, char **key, uint64_t *nkey) {
        uint32_t len = 0;
        *req_type = char_to_req_type(buf[0]);
        buf += 1;
        len += 1;
        uint32_t n = str_to_int(buf, from_server_id);
        buf += n;
        len += n;
        n = str_to_int(buf, from_sock_fd);
        buf += n;
        len += n;
        n = str_to_int(buf, nkey);
        buf += n;
        len += n;
        *key = buf;
        len += *nkey;
        return len;
    }

//    Host convert_host(string host_str) {
//        RDMA_LOG(INFO) << host_str;
//
//        std::vector<std::string> ip_port;
//        std::stringstream ss_ip_port(host_str);
//        while (ss_ip_port.good()) {
//            std::string substr;
//            getline(ss_ip_port, substr, ':');
//            ip_port.push_back(substr);
//        }
//        return Host{ip_port[0], atoi(ip_port[1].c_str())};
//    }

    vector<Host> convert_hosts(string hosts_str) {
        NOVA_LOG(INFO) << hosts_str;
        vector<Host> hosts;
        std::stringstream ss_hosts(hosts_str);
        uint32_t host_id = 0;
        while (ss_hosts.good()) {
            string host_str;
            getline(ss_hosts, host_str, ',');

            if (host_str.empty()) {
                continue;
            }
            std::vector<std::string> ip_port;
            std::stringstream ss_ip_port(host_str);
            while (ss_ip_port.good()) {
                std::string substr;
                getline(ss_ip_port, substr, ':');
                ip_port.push_back(substr);
            }
            Host host = {};
            host.server_id = host_id;
            host.ip = ip_port[0];
            host.port = atoi(ip_port[1].c_str());
            hosts.push_back(host);
            host_id++;
        }
        return hosts;
    }

    uint64_t keyhash(const char *key, uint64_t nkey) {
        uint64_t hv = 0;
        str_to_int(key, &hv, nkey);
        return hv;
    }

}