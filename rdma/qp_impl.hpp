#pragma once

#include <limits>

#include "pre_connector.hpp"

namespace rdmaio {

    const int MAX_INLINE_SIZE = 64;

/**
 * These are magic numbers, served as the keys / identifications
 * Currently we not allow user defined keys, but can simply added
 */
    const uint32_t DEFAULT_QKEY = 0x11111111;
    const uint32_t DEFAULT_PSN = 3185;

/**
 * QP encoder, provde a default naming to identity QPs
 */
    enum {
        RC_ID_BASE = 0,
        UC_ID_BASE = 10000,
        UD_ID_BASE = 20000
    };

    inline constexpr RCConfig default_rc_config() {
        return RCConfig{
                .access_flags       = (IBV_ACCESS_REMOTE_WRITE |
                                       IBV_ACCESS_REMOTE_READ |
                                       IBV_ACCESS_REMOTE_ATOMIC),
                .max_rd_atomic      = 16,
                .max_dest_rd_atomic = 16,
                .rq_psn             = DEFAULT_PSN,
                .sq_psn             = DEFAULT_PSN,
                .timeout            = 20
        };
    }

    inline constexpr RCConfig default_uc_config() {
        return RCConfig{
                .access_flags       = (IBV_ACCESS_REMOTE_WRITE),
                .max_rd_atomic      = 0,
                .max_dest_rd_atomic = 0,
                .rq_psn             = DEFAULT_PSN,
                .sq_psn             = DEFAULT_PSN,
                .timeout            = 0
        };
    }

    inline constexpr uint32_t index_mask() {
        return 0xffff;
    }

    inline uint32_t mac_mask() {
        return ::rdmaio::index_mask() << 16;
    }

    inline uint32_t encode_qp_id(int m, int idx) {
        return static_cast<uint32_t>(static_cast<uint32_t>(m) << 16) |
               static_cast<uint32_t>(idx);
    }

    inline uint32_t decode_qp_mac(uint32_t key) {
        return (key & ::rdmaio::mac_mask()) >> 16;
    }

    inline uint32_t decode_qp_index(uint32_t key) {
        return key & ::rdmaio::index_mask();
    }

    class QPImpl {
    public:
        QPImpl() = default;

        ~QPImpl() = default;

        static enum ibv_qp_state query_qp_status(ibv_qp *qp) {
            struct ibv_qp_attr attr;
            struct ibv_qp_init_attr init_attr;

            if (ibv_query_qp(qp, &attr, IBV_QP_STATE, &init_attr)) {
                NOVA_ASSERT(false) << "query qp cannot cause error";
            }
            return attr.qp_state;
        }

        static ConnStatus
        get_remote_helper(ConnArg *arg, ConnReply *reply, std::string ip,
                          int port) {

            ConnStatus ret = SUCC;

            auto socket = PreConnector::get_send_socket(ip, port);
            if (socket < 0) {
                return ERR;
            }

            auto n = send(socket, (char *) (arg), sizeof(ConnArg), 0);

            if (n != sizeof(ConnArg)) {
                ret = ERR;
                goto CONN_END;
            }

            // receive reply
            if (!PreConnector::wait_recv(socket, 10000)) {
                ret = TIMEOUT;
                goto CONN_END;
            }

            n = recv(socket, (char *) ((reply)), sizeof(ConnReply),
                     MSG_WAITALL);
            if (n != sizeof(ConnReply)) {
                ret = ERR;
                goto CONN_END;
            }
            if (reply->ack != SUCC) {
                ret = NOT_READY;
                goto CONN_END;
            }
            CONN_END:
            shutdown(socket, SHUT_RDWR);
            close(socket);
            return ret;
        }

        static ConnStatus
        get_remote_mr(std::string ip, int port, uint64_t mr_id,
                      MemoryAttr *attr) {

            ConnArg arg;
            ConnReply reply;
            arg.type = ConnArg::MR;
            arg.payload.mr.mr_id = mr_id;

            auto ret = get_remote_helper(&arg, &reply, ip, port);
            if (ret == SUCC) {
                attr->key = reply.payload.mr.key;
                attr->buf = reply.payload.mr.buf;
            }
            return ret;
        }

        static ConnStatus
        poll_till_completion(ibv_cq *cq, ibv_wc &wc, struct timeval timeout) {

            struct timeval start_time;
            gettimeofday(&start_time, nullptr);
            int poll_result = 0;
            int64_t diff;
            int64_t numeric_timeout = (timeout.tv_sec == 0 &&
                                       timeout.tv_usec == 0)
                                      ? std::numeric_limits<int64_t>::max() :
                                      timeout.tv_sec * 1000 + timeout.tv_usec;
            bool donot_wait = timeout.tv_sec == no_wait.tv_sec &&
                              timeout.tv_usec == no_wait.tv_usec;
            do {
                asm volatile("":: : "memory");
                poll_result = ibv_poll_cq(cq, 1, &wc);

                if (donot_wait) {
                    break;
                }
                struct timeval cur_time;
                gettimeofday(&cur_time, nullptr);
                diff = diff_time(cur_time, start_time);
            } while ((poll_result == 0) && (diff <= numeric_timeout));

            if (poll_result == 0) {
                return TIMEOUT;
            }

            if (poll_result < 0) {
                NOVA_ASSERT(false);
                return ERR;
            }
            NOVA_LOG_IF(4, wc.status != IBV_WC_SUCCESS) <<
                                                        "poll till completion error: "
                                                        << wc.status << " "
                                                        << ibv_wc_status_str(
                                                                wc.status);
            return wc.status == IBV_WC_SUCCESS ? SUCC : ERR;
        }

        static int poll_multi_till_completion(ibv_cq *cq, ibv_wc *wcs, int nwc,
                                              struct timeval timeout) {
            struct timeval start_time;
            gettimeofday(&start_time, nullptr);
            int poll_result = 0;
            int i = 0;
            int64_t diff;
            int64_t numeric_timeout = (timeout.tv_sec == 0 &&
                                       timeout.tv_usec == 0)
                                      ? std::numeric_limits<int64_t>::max() :
                                      timeout.tv_sec * 1000 + timeout.tv_usec;
            bool donot_wait = timeout.tv_sec == no_wait.tv_sec &&
                              timeout.tv_usec == no_wait.tv_usec;
            do {
                asm volatile("":: : "memory");
                poll_result = ibv_poll_cq(cq, nwc, wcs);

                if (donot_wait) {
                    break;
                }

                struct timeval cur_time;
                gettimeofday(&cur_time, nullptr);
                diff = diff_time(cur_time, start_time);
            } while ((poll_result == 0) && (diff <= numeric_timeout));

            if (poll_result < 0) {
                NOVA_ASSERT(false);
                return ERR;
            }
            for (i = 0; i < poll_result; i++) {
//                printf("poll-%d, wr-%d\n", poll_result, wcs[i].wr_id);
                NOVA_LOG_IF(WARNING, wcs[i].status != IBV_WC_SUCCESS) <<
                                                                      "poll till completion error: "
                                                                      << wcs[i].status
                                                                      << " "
                                                                      << ibv_wc_status_str(
                                                                              wcs[i].status);
            }
            return poll_result;
        }

        static int poll_multi_from_cq(ibv_cq *cq, ibv_wc *wcs, int nwc) {
            return ibv_poll_cq(cq, nwc, wcs);
        }
    };

    class RCQPImpl {
    public:
        RCQPImpl() = default;

        ~RCQPImpl() = default;

        template<RCConfig (*F)(void)>
        static void
        ready2init(ibv_qp *qp, RNicHandler *rnic, enum ibv_qp_type qp_type) {

            auto config = F();
            if (qp_type == IBV_QPT_RC) {
                config = default_rc_config();
            } else {
                config = default_uc_config();
            }

            struct ibv_qp_attr qp_attr = {};
            qp_attr.qp_state = IBV_QPS_INIT;
            qp_attr.pkey_index = 0;
            qp_attr.port_num = rnic->port_id;
            qp_attr.qp_access_flags = config.access_flags;

            int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                        IBV_QP_ACCESS_FLAGS;
            int rc = ibv_modify_qp(qp, &qp_attr, flags);
            RDMA_VERIFY(WARNING, rc == 0)
                << "Failed to modify RC to INIT state, %s\n" << strerror(errno);

            if (rc != 0) {
                // error handling
                NOVA_LOG(WARNING) << " change state to init failed. ";
            }
        }

        template<RCConfig (*F)(void)>
        static bool ready2rcv(ibv_qp *qp, QPAttr &attr, RNicHandler *rnic,
                              enum ibv_qp_type qp_type) {

            auto config = F();

            struct ibv_qp_attr qp_attr = {};

            qp_attr.qp_state = IBV_QPS_RTR;
            // DONOT CHANGE save as perftest.
            qp_attr.path_mtu = IBV_MTU_2048;
            qp_attr.dest_qp_num = attr.qpn;
            qp_attr.rq_psn = config.rq_psn; // should this match the sender's psn ?

            if (qp_type == IBV_QPT_RC) {
                qp_attr.max_dest_rd_atomic = config.max_dest_rd_atomic;
                // https://www.rdmamojo.com/2013/01/12/ibv_modify_qp/
                // DONOT CHANGE!!!
                qp_attr.min_rnr_timer = 12;
            }

            qp_attr.ah_attr.dlid = attr.lid;
            qp_attr.ah_attr.sl = 0;
            qp_attr.ah_attr.src_path_bits = 0;
            qp_attr.ah_attr.port_num = rnic->port_id; /* Local port! */

            qp_attr.ah_attr.is_global = 1;
            qp_attr.ah_attr.grh.dgid.global.subnet_prefix = attr.addr.subnet_prefix;
            qp_attr.ah_attr.grh.dgid.global.interface_id = attr.addr.interface_id;
            qp_attr.ah_attr.grh.sgid_index = 0;
            qp_attr.ah_attr.grh.flow_label = 0;
            qp_attr.ah_attr.grh.hop_limit = 255;

            int uc_flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                           IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
            int rc_flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                           IBV_QP_DEST_QPN | IBV_QP_RQ_PSN
                           | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
            int flags = qp_type == IBV_QPT_RC ? rc_flags : uc_flags;
            auto rc = ibv_modify_qp(qp, &qp_attr, flags);
            NOVA_ASSERT(rc == 0) << "ready to rcv failed " << rc;
            return rc == 0;

        }

        template<RCConfig (*F)(void)>
        static bool ready2send(ibv_qp *qp, enum ibv_qp_type qp_type) {

            auto config = F();
            struct ibv_qp_attr qp_attr = {};

            qp_attr.qp_state = IBV_QPS_RTS;
            qp_attr.sq_psn = config.sq_psn;

            if (qp_type == IBV_QPT_RC) {
                qp_attr.retry_cnt = 7;
                qp_attr.rnr_retry = 7;
                qp_attr.timeout = config.timeout;
                qp_attr.max_rd_atomic = config.max_rd_atomic;
                qp_attr.max_dest_rd_atomic = config.max_dest_rd_atomic;
            }

            int rc_flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT |
                           IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                           IBV_QP_MAX_QP_RD_ATOMIC;
            int uc_flags = IBV_QP_STATE | IBV_QP_SQ_PSN;
            int flags = qp_type == IBV_QPT_RC ? rc_flags : uc_flags;
            auto rc = ibv_modify_qp(qp, &qp_attr, flags);
            NOVA_ASSERT(rc == 0) << "ready to rcv failed " << rc;
            return rc == 0;
        }

        template<RCConfig (*F)(void)>
        static void
        init(ibv_qp *&qp, ibv_cq *cq, ibv_cq *recv_cq, RNicHandler *rnic,
             enum ibv_qp_type qp_type) {
            RDMA_VERIFY(WARNING, cq != nullptr) << "create cq error: "
                                                << strerror(errno);

            // create the QP
            struct ibv_qp_init_attr qp_init_attr = {};

            qp_init_attr.send_cq = cq;
            qp_init_attr.recv_cq = recv_cq;
            qp_init_attr.qp_type = qp_type;
            qp_init_attr.cap.max_send_wr = RC_MAX_SEND_SIZE;
            qp_init_attr.cap.max_recv_wr = RC_MAX_RECV_SIZE;    /* Can be set to 1, if RC Two-sided is not required */
            qp_init_attr.cap.max_send_sge = 1;
            qp_init_attr.cap.max_recv_sge = 1;
            qp_init_attr.cap.max_inline_data = MAX_INLINE_SIZE;

            qp = ibv_create_qp(rnic->pd, &qp_init_attr);
            RDMA_VERIFY(WARNING, qp != nullptr);

            if (qp)
                ready2init<F>(qp, rnic, qp_type);
        }
    };

    class UDQPImpl {
    public:
        UDQPImpl() = default;

        ~UDQPImpl() = default;

        template<UDConfig (*F)(void)>
        static void
        init(ibv_qp *&qp, ibv_cq *&cq, ibv_cq *&recv_cq, RNicHandler *rnic) {

            auto config = F(); // generate the config
            NOVA_ASSERT(config.max_send_size == RC_MAX_SEND_SIZE);
            NOVA_ASSERT(config.max_recv_size == RC_MAX_RECV_SIZE);

            if (qp != nullptr)
                return;

            if ((cq = ibv_create_cq(rnic->ctx, RC_MAX_SEND_SIZE, nullptr,
                                    nullptr, 0)) == nullptr) {
                NOVA_LOG(ERROR) << "create send cq for UD QP error: "
                                << strerror(errno);
                return;
            }

            if ((recv_cq = ibv_create_cq(rnic->ctx, RC_MAX_RECV_SIZE, nullptr,
                                         nullptr, 0)) == nullptr) {
                NOVA_LOG(ERROR) << "create recv cq for UD QP error: "
                                << strerror(errno);
                return;
            }

            /* Initialize creation attributes */
            struct ibv_qp_init_attr qp_init_attr;
            memset(&qp_init_attr, 0, sizeof(struct ibv_qp_init_attr));

            qp_init_attr.send_cq = cq;
            qp_init_attr.recv_cq = recv_cq;
            qp_init_attr.qp_type = IBV_QPT_UD;
            qp_init_attr.srq = NULL;

            qp_init_attr.cap.max_send_wr = RC_MAX_SEND_SIZE;
            qp_init_attr.cap.max_recv_wr = RC_MAX_RECV_SIZE;
            qp_init_attr.cap.max_send_sge = 1;
            qp_init_attr.cap.max_recv_sge = 1;
            qp_init_attr.cap.max_inline_data = MAX_INLINE_SIZE;

            if ((qp = ibv_create_qp(rnic->pd, &qp_init_attr)) == nullptr) {
                NOVA_LOG(ERROR) << "create send qp for UD QP error: "
                                << strerror(errno);
                return;
            }

            // change QP status
//            ready2init(qp, rnic, config); // shall always succeed
            int rc;
            int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                        IBV_QP_QKEY;
            struct ibv_qp_attr qp_attr;
            memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
            qp_attr.qp_state = IBV_QPS_INIT;
            qp_attr.pkey_index = 0;
            qp_attr.port_num = rnic->port_id;
            qp_attr.qkey = DEFAULT_QKEY;
            NOVA_LOG(DEBUG) << "init qp portid: " << rnic->port_id;
            rc = ibv_modify_qp(qp, &qp_attr, flags);
            NOVA_ASSERT(rc == 0) << rc;

            memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
            flags = IBV_QP_STATE;
            qp_attr.qp_state = IBV_QPS_RTR;
            NOVA_LOG(DEBUG) << "rtr qp portid: " << rnic->port_id << " lid:"
                            << rnic->lid;
            rc = ibv_modify_qp(qp, &qp_attr, flags);
            NOVA_ASSERT(rc == 0) << rc;

//            memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
            qp_attr.qp_state = IBV_QPS_RTS;
            qp_attr.sq_psn = lrand48() & 0xffffff;
            flags = IBV_QP_STATE | IBV_QP_SQ_PSN;
            rc = ibv_modify_qp(qp, &qp_attr, flags);
            NOVA_ASSERT(rc == 0) << rc;
        }

        static ibv_ah *create_ah(RNicHandler *rnic, QPAttr &attr) {
            struct ibv_ah_attr ah_attr;
            memset(&ah_attr, 0, sizeof(ah_attr));

            ah_attr.is_global = 0;
            ah_attr.dlid = attr.lid;
            ah_attr.sl = 0;
            ah_attr.src_path_bits = 0;
            ah_attr.port_num = rnic->port_id;
            ah_attr.static_rate = 0;

            NOVA_LOG(DEBUG) << "ah: " << ah_attr.dlid << ":" << ah_attr.port_num
                            << ":" << rnic->port_id;

//            ah_attr.grh.dgid.global.subnet_prefix = attr.addr.subnet_prefix;
//            ah_attr.grh.dgid.global.interface_id = attr.addr.interface_id;
//            ah_attr.grh.flow_label = 0;
//            ah_attr.grh.hop_limit = 255;
//            ah_attr.grh.sgid_index = rnic->gid;
            struct ibv_ah *ah = ibv_create_ah(rnic->pd, &ah_attr);
            NOVA_ASSERT(ah != NULL);
            return ah;
        }

    };

} // namespace rdmaio
