#pragma once

#include "common.hpp"
#include "qp_impl.hpp" // hide the implementation

#define DEFAULT_QKEY      0x11111111

namespace rdmaio {
/**
 * The QP managed by RLib is identified by the QPIdx
 * Basically it identifies which worker(thread) is using the QP,
 * and which machine this QP is connected to.
 */
    typedef struct {
        int node_id;   // the node QP connect to
        int worker_id; // the thread/task QP belongs
        int index;     // mutliple QP may is needed to connect to the node
    } QPIdx;

// some macros for easy computer QP idx, since some use default values
    constexpr QPIdx create_rc_idx(int nid, int wid, int index) {
        return QPIdx{
                .node_id   = nid,
                .worker_id = wid,
                .index     = index
        };
    }

    constexpr QPIdx create_ud_idx(int worker_id, int idx) {
        return QPIdx{
                .node_id   = 0, // a UD qp can connect to multiple machine
                .worker_id = worker_id,
                .index     = idx
        };
    }

/**
 * Wrappers over ibv_qp & ibv_cq
 * For easy use, and connect
 */
    class QP {
    public:
        QP(RNicHandler *rnic, QPIdx idx) :
                idx_(idx),
                rnic_(rnic) {
        }

        ~QP() {
            if (qp_ != nullptr)
                ibv_destroy_qp(qp_);
            if (cq_ != nullptr)
                ibv_destroy_cq(cq_);
        }
        /**
         * Connect to remote QP
         * Note, we leverage TCP for a pre connect.
         * So the IP/Hostname and a TCP port must be given.
         *
         * WARNING:
         * This function actually should contains two functions, connect + change QP status
         * maybe split to connect + change status for more flexibility?
         */
        /**
         * connect to the specific QP at remote, specificed by the nid and wid
         * return SUCC if QP are ready.
         * return TIMEOUT if there is network error.
         * return NOT_READY if remote server fails to find the connected QP
         */
        virtual ConnStatus connect(std::string ip, int port, QPIdx idx) = 0;

        // return until the completion events
        // this call will block until a timeout
        virtual ConnStatus poll_till_completion(ibv_wc &wc,
                                                struct timeval timeout = default_timeout) {
            return QPImpl::poll_till_completion(cq_, wc, timeout);
        }

        virtual int poll_multi_till_completion(ibv_wc *wcs, int nwc,
                                               struct timeval timeout = default_timeout) {
            return QPImpl::poll_multi_till_completion(cq_, wcs, nwc, timeout);
        }

        virtual int poll_multi_from_cq(ibv_wc *wcs, int nwc) {
            return QPImpl::poll_multi_from_cq(cq_, wcs, nwc);
        }

        void bind_local_mr(MemoryAttr attr) {
            local_mr_ = attr;
        }

        QPAttr get_attr() const {
            QPAttr res = {
                    .addr     = rnic_->query_addr(),
                    .lid      = rnic_->lid,
                    .qpn      = (qp_ != nullptr) ? qp_->qp_num : 0,
                    .psn      = DEFAULT_PSN, // TODO! this may be filled later
                    .node_id  = 0, // a place holder
                    .port_id  = rnic_->port_id
            };
            return res;
        }

        /**
         * Get remote MR attribute
         */
        static ConnStatus
        get_remote_mr(std::string ip, int port, uint64_t mr_id,
                      MemoryAttr *attr) {
            return QPImpl::get_remote_mr(ip, port, mr_id, attr);
        }

        // QP identifiers
        const QPIdx idx_;

    public:
        // internal verbs structure
        struct ibv_qp *qp_ = NULL;
        struct ibv_cq *cq_ = NULL;

        // local MR used to post reqs
        MemoryAttr local_mr_;
        RNicHandler *rnic_;

    protected:
        ConnStatus
        get_remote_helper(ConnArg *arg, ConnReply *reply, std::string ip,
                          int port) {
            return QPImpl::get_remote_helper(arg, reply, ip, port);
        }
    };

/**
 * Raw RC QP
 */
    template<RCConfig (*F)(void) = default_rc_config>
    class RRCQP : public QP {
    public:
        RRCQP(RNicHandler *rnic, QPIdx idx,
              MemoryAttr local_mr, MemoryAttr remote_mr,
              enum ibv_qp_type qp_type, ibv_cq *cq, ibv_cq *recv_cq)
                : RRCQP(rnic, idx, qp_type, cq, recv_cq) {
            bind_local_mr(local_mr);
            bind_remote_mr(remote_mr);
        }

        RRCQP(RNicHandler *rnic, QPIdx idx, MemoryAttr local_mr,
              enum ibv_qp_type qp_type, ibv_cq *cq, ibv_cq *recv_cq)
                : RRCQP(rnic, idx, qp_type, cq, recv_cq) {
            bind_local_mr(local_mr);
        }

        RRCQP(RNicHandler *rnic, QPIdx idx, enum ibv_qp_type qp_type,
              ibv_cq *cq, ibv_cq *recv_cq)
                : QP(rnic, idx), qp_type_(qp_type) {
            cq_ = cq;
            recv_cq_ = recv_cq;
            RCQPImpl::init<F>(qp_, cq_, recv_cq_, rnic_, qp_type);
        }

        ConnStatus connect(std::string ip, int port) {
            return connect(ip, port, idx_);
        }

        ConnStatus connect(std::string ip, int port, QPIdx idx) {

            // first check whether QP is valid to connect
            enum ibv_qp_state state;
            if ((state = QPImpl::query_qp_status(qp_)) != IBV_QPS_INIT) {
                if (state != IBV_QPS_RTS)
                    NOVA_LOG(WARNING)
                        << "qp not in a correct state to connect!";
                return (state == IBV_QPS_RTS) ? SUCC : UNKNOWN;
            }
            ConnArg arg = {};
            ConnReply reply = {};
            arg.type = ConnArg::QP;
            arg.payload.qp.from_node = idx.node_id;
            arg.payload.qp.from_worker = idx.worker_id;
            arg.payload.qp.from_index = idx.index;
            arg.payload.qp.qp_type = IBV_QPT_RC;

            auto ret = QPImpl::get_remote_helper(&arg, &reply, ip, port);
            if (ret == SUCC) {
                // change QP status
                if (!RCQPImpl::ready2rcv<F>(qp_, reply.payload.qp, rnic_,
                                            qp_type_)) {
                    NOVA_LOG(WARNING)
                        << "change qp status to ready to receive error: "
                        << strerror(errno);
                    ret = ERR;
                    goto CONN_END;
                }

                if (!RCQPImpl::ready2send<F>(qp_, qp_type_)) {
                    NOVA_LOG(WARNING)
                        << "change qp status to ready to send error: "
                        << strerror(errno);
                    ret = ERR;
                    goto CONN_END;
                }
            }
            CONN_END:
            return ret;
        }

        /**
         * Bind this QP's operation to a remote memory region according to the MemoryAttr.
         * Since usually one QP access *one memory region* almost all the time,
         * so it is more convenient to use a bind-post;bind-post-post fashion.
         */
        void bind_remote_mr(MemoryAttr attr) {
            remote_mr_ = attr;
        }

        ConnStatus post_send_to_mr(MemoryAttr &local_mr, MemoryAttr &remote_mr,
                                   ibv_wr_opcode op, char *local_buf,
                                   uint32_t len, uint64_t off, int flags,
                                   uint64_t wr_id = 0, uint32_t imm = 0) {
            ConnStatus ret = SUCC;
            struct ibv_send_wr *bad_sr;

            // setting the SGE
            struct ibv_sge sge{
                    .addr = (uint64_t) local_buf,
                    .length = len,
                    .lkey   = local_mr.key
            };

            // setting sr, sr has to be initilized in this style
            struct ibv_send_wr sr;
            sr.wr_id = wr_id;
            sr.opcode = op;
            sr.num_sge = 1;
            sr.next = NULL;
            sr.sg_list = &sge;
            sr.send_flags = flags;
            sr.imm_data = imm;

            sr.wr.rdma.remote_addr = remote_mr.buf + off;
            sr.wr.rdma.rkey = remote_mr.key;

            if (len < MAX_INLINE_SIZE) {
                sr.send_flags |= IBV_SEND_INLINE;
            }

            auto rc = ibv_post_send(qp_, &sr, &bad_sr);
            if (rc != 0) {
                printf("send failed %d\n", rc);
            }
            return rc == 0 ? SUCC : ERR;
        }

        ConnStatus
        post_recv(char *local_buf, uint32_t len, uint64_t wr_id = 0) {
            int ret = 0;
            struct ibv_recv_wr *bad_recv_wr;
            uint32_t lkey = local_mr_.key;
            /**
             * Since one cannot control or predict in advanced which WR will be fetched from the SRQ
             * to which QP, it is highly advised that all of the WRs in the SRQ will be able to
             * handle the maximum message that any QP may receive.
             * */
            struct ibv_sge list{
                    // the receive buffer, the length, and the memory region's key.
                    .addr   = (uint64_t) local_buf,
                    .length = len,
                    .lkey   = lkey
            };
            struct ibv_recv_wr recv_wr;
            recv_wr.wr_id = wr_id;
            recv_wr.sg_list = &list;
            recv_wr.num_sge = 1;
            recv_wr.next = NULL;
            ret = ibv_post_recv(qp_, &recv_wr, &bad_recv_wr);
            if (ret != 0) {
                NOVA_LOG(WARNING) << " post receive failed " << ret;
            }
            return ret == 0 ? SUCC : ERR;
        }

        /**
         * Post request(s) to the sending QP.
         * This is just a wrapper of ibv_post_send
         */
        ConnStatus
        post_send(ibv_wr_opcode op, char *local_buf, uint32_t len, uint64_t off,
                  int flags,
                  uint64_t wr_id = 0, uint32_t imm = 0) {
            return post_send_to_mr(local_mr_, remote_mr_, op, local_buf, len,
                                   off, flags, wr_id, imm);
        }


        // one-sided atomic operations
        ConnStatus post_cas(char *local_buf, uint64_t off,
                            uint64_t compare, uint64_t swap, int flags,
                            uint64_t wr_id = 0) {
            return post_atomic<IBV_WR_ATOMIC_CMP_AND_SWP>(local_buf, off,
                                                          compare, swap, flags,
                                                          wr_id);
        }

        // one-sided fetch and add
        ConnStatus
        post_faa(char *local_buf, uint64_t off, uint64_t add_value, int flags,
                 int wr_id = 0) {
            return post_atomic<IBV_WR_ATOMIC_FETCH_AND_ADD>(local_buf, off,
                                                            add_value,
                                                            0 /* no swap value is needed*/,
                                                            flags, wr_id);
        }

        template<ibv_wr_opcode type>
        ConnStatus post_atomic(char *local_buf, uint64_t off,
                               uint64_t compare, uint64_t swap, int flags,
                               uint64_t wr_id = 0) {
            static_assert(type == IBV_WR_ATOMIC_CMP_AND_SWP ||
                          type == IBV_WR_ATOMIC_FETCH_AND_ADD,
                          "only two atomic operations are currently supported.");

            // check if address (off) is 8-byte aligned
            if ((off & 0x7) != 0) {
                return WRONG_ARG;
            }

            ConnStatus ret = SUCC;
            struct ibv_send_wr *bad_sr;

            // setting the SGE
            struct ibv_sge sge{
                    .addr = (uint64_t) local_buf,
                    .length = sizeof(uint64_t), // atomic only supports 8-byte operation
                    .lkey   = local_mr_.key
            };

            struct ibv_send_wr sr;
            sr.wr_id = wr_id;
            sr.opcode = type;
            sr.num_sge = 1;
            sr.next = NULL;
            sr.sg_list = &sge;
            sr.send_flags = flags;
            // remote memory
            sr.wr.atomic.rkey = remote_mr_.key;
            sr.wr.atomic.remote_addr = (off + remote_mr_.buf);
            sr.wr.atomic.compare_add = compare;
            sr.wr.atomic.swap = swap;

            auto rc = ibv_post_send(qp_, &sr, &bad_sr);
            return rc == 0 ? SUCC : ERR;
        }

        ConnStatus
        post_batch(struct ibv_send_wr *send_sr, ibv_send_wr **bad_sr_addr,
                   int num = 0) {
            auto rc = ibv_post_send(qp_, send_sr, bad_sr_addr);
            return rc == 0 ? SUCC : ERR;
        }

        /**
         * Poll completions. These are just wrappers of ibv_poll_cq
         */
        int poll_send_completion(ibv_wc &wc) {
            return ibv_poll_cq(cq_, 1, &wc);
        }

        ConnStatus poll_till_completion(ibv_wc &wc,
                                        struct timeval timeout = default_timeout) {
            auto ret = QP::poll_till_completion(wc, timeout);
            if (ret == SUCC) {
                low_watermark_ = high_watermark_;
            }
            return ret;
        }

        /**
         * Used to count pending reqs
         * XD: current we use 64 as default, but it is rather application defined,
         * which is related to how the QP's send to are created, etc
         */
        bool need_poll(int threshold = (RC_MAX_SEND_SIZE / 2)) {
            return (high_watermark_ - low_watermark_) >= threshold;
        }

        uint64_t high_watermark_ = 0;
        uint64_t low_watermark_ = 0;

        MemoryAttr remote_mr_;
        enum ibv_qp_type qp_type_;
        struct ibv_cq *recv_cq_ = NULL;
    };

    inline constexpr UDConfig default_ud_config() {
        return UDConfig{
                .max_send_size  = RC_MAX_SEND_SIZE,
                .max_recv_size  = RC_MAX_RECV_SIZE,
                .qkey           = DEFAULT_QKEY,
                .psn            = DEFAULT_PSN
        };
    }

/**
 * Raw UD QP
 */
    template<UDConfig (*F)(void) = default_ud_config, int MAX_SERVER_NUM = 32>
    class RUDQP : public QP {
    public:
        RUDQP(RNicHandler *rnic, QPIdx idx, MemoryAttr local_mr)
                : RUDQP(rnic, idx) {
            bind_local_mr(local_mr);
        }

        RUDQP(RNicHandler *rnic, QPIdx idx)
                : QP(rnic, idx) {
            UDQPImpl::init<F>(qp_, cq_, recv_cq_, rnic_);
            for (int i = 0; i < MAX_SERVER_NUM; i++) {
                NOVA_LOG(DEBUG) << "set ah " << i << " to null";
                ahs_[i] = nullptr;
            }
        }

        bool queue_empty() {
            return pendings == 0;
        }

        bool need_poll(int threshold) {
            return pendings >= threshold;
        }

        /**
         * Simple wrapper to expose underlying QP structures
         */
        inline __attribute__ ((always_inline))
        ibv_cq *recv_queue() {
            return recv_cq_;
        }

        inline __attribute__ ((always_inline))
        ibv_qp *send_qp() {
            return qp_;
        }

        ConnStatus connect(std::string ip, int port) {
            // UD QP is not bounded to a mac, so use idx to index
            return connect(ip, port, idx_);
        }

        ConnStatus connect(std::string ip, int port, QPIdx idx) {

            ConnArg arg;
            ConnReply reply;
            arg.type = ConnArg::QP;
            arg.payload.qp.from_worker = idx.worker_id;
            arg.payload.qp.from_index = idx.index;
            arg.payload.qp.qp_type = IBV_QPT_UD;

            auto ret = QPImpl::get_remote_helper(&arg, &reply, ip, port);

            if (ret == SUCC) {
                // create the ah, and store the address handler
                auto ah = UDQPImpl::create_ah(rnic_, reply.payload.qp);
                NOVA_LOG(INFO) << "create ah for " << reply.payload.qp.node_id
                               << ":" << reply.payload.qp.qpn << ":"
                               << reply.payload.qp.lid << ":" << ah->handle;
                ahs_[reply.payload.qp.node_id] = ah;
                attrs_[reply.payload.qp.node_id] = reply.payload.qp;
            }
            CONN_END:
            return ret;
        }

        void post_receive(char *buf, uint32_t nbuf, uint64_t wr_id) {
            struct ibv_sge sge;
            memset(&sge, 0, sizeof(struct ibv_sge));
            sge.addr = (uintptr_t) buf;
            sge.length = nbuf;
            sge.lkey = local_mr_.key;

            struct ibv_recv_wr rrs;
            memset(&rrs, 0, sizeof(struct ibv_recv_wr));
            rrs.sg_list = &sge;
            rrs.num_sge = 1;
            rrs.wr_id = wr_id;
            rrs.next = NULL;
            struct ibv_recv_wr *bad_rr;
            int ret = ibv_post_recv(qp_, &rrs, &bad_rr);

//            RDMA_LOG(DEBUG) << "post receives " << nbuf << ":" << wr_id;
            NOVA_ASSERT(ret == 0) << ret;
        }

        void
        post_send(uint32_t node_id, char *buf, uint32_t nbuf, uint64_t wr_id) {
            struct ibv_sge sge;
            memset(&sge, 0, sizeof(struct ibv_sge));
            sge.addr = (uintptr_t) buf;
            sge.length = nbuf;
            sge.lkey = local_mr_.key;

//            RDMA_LOG(DEBUG) << "send data to " << ahs_[node_id]->handle << ":" << attrs_[node_id].qpn << " nbuf" << nbuf
//                            << " wr" << wr_id << " lkey"
//                            << local_mr_.key;
            ibv_send_wr srs;
            memset(&srs, 0, sizeof(struct ibv_send_wr));
            srs.imm_data = 0;
            srs.wr_id = wr_id;
            srs.sg_list = &sge;
            srs.num_sge = 1;
            srs.next = NULL;
            srs.opcode = IBV_WR_SEND;
            srs.send_flags = IBV_SEND_SIGNALED |
                             ((nbuf < MAX_INLINE_SIZE) ? IBV_SEND_INLINE : 0);
            srs.wr.ud.ah = ahs_[node_id];
            srs.wr.ud.remote_qpn = attrs_[node_id].qpn;
            srs.wr.ud.remote_qkey = DEFAULT_QKEY;
            ibv_send_wr *bad_sr;
            int rc = ibv_post_send(qp_, &srs, &bad_sr);
            NOVA_ASSERT(rc == 0) << rc;
        }

        /**
         * whether this UD QP has been post recved
         * a UD QP should be first been post_recved; then it can be connected w others
         */
        bool ready() {
            return ready_;
        }

        void set_ready() {
            ready_ = true;
        }

        friend class UDAdapter;

        struct ibv_cq *recv_cq_ = NULL;
        /**
         * FIXME: curretly we have limited servers, so we use an array.
         * using a map will affect the perfomrance in microbenchmarks.
         * remove it, and merge this in UDAdapter?
         */
        struct ibv_ah *ahs_[MAX_SERVER_NUM];
        struct QPAttr attrs_[MAX_SERVER_NUM];
    private:
        // current outstanding requests which have not been polled
        int pendings = 0;

        bool ready_ = false;
    };

}; // end namespace rdmaio
