#pragma once

#include <memory>
#include <sys/utsname.h>
#include <mutex>
#include <set>
#include <iterator>

#include "qp.hpp"

namespace rdmaio {

    const int MAX_SERVER_SUPPORTED = 32;
    typedef RUDQP<default_ud_config, MAX_SERVER_SUPPORTED> UDQP;
    typedef RRCQP<default_rc_config> RCQP;

    typedef std::function<void(const QPConnArg &)> connection_callback_t;

    class RdmaCtrl {
    public:
        RdmaCtrl(int node_id, int tcp_base_port,
                 connection_callback_t callback = [](const QPConnArg &) {
                     // the default callback does nothing
                 },
                 std::string ip = "localhost");

        ~RdmaCtrl();

        int current_node_id();

        int listening_port();

        typedef struct {
            uint16_t dev_id;
            uint8_t port_id;
        } DevIdx;

        /**
         * Query devices info on this machine,
         * if there is a previous call, return previous results unless clear_dev_info has been called
         */
        std::vector<RNicInfo> query_devs();

        static std::vector<RNicInfo> query_devs_helper();

        // clear the cached infos by RdmaCtrl;
        void clear_dev_info();

        /**
         * Open device handlers.
         * RdmaCtrl opens a device for each thread.
         * The get_device returns previously opened device of this thread, if it is already opened
         */
        RNicHandler *open_thread_local_device(DevIdx idx);

        RNicHandler *open_device(DevIdx idx);

        RNicHandler *get_device();

        /**
         * The *callback* is called once a QP connection request is sent to this server
         */
        void register_qp_callback(connection_callback_t callback);

        void close_device();

        void close_device(RNicHandler *);

        /**
         * Register memory to a specific RNIC handler
         */
        bool register_memory(uint64_t id, const char *buf, uint64_t size, RNicHandler *rnic,
                             int flag = Memory::DEFAULT_PROTECTION_FLAG);

        ibv_cq *create_cq(RNicHandler *dev, int cqe);

        /**
         * Get the local registered memory
         * undefined if mr_id has been registered
         */
        MemoryAttr get_local_mr(uint64_t mr_id);

        /**
         * Return an arbitrary registered MR
         * return -1 if no MR is registered to RdmaCtrl
         * return the first mr index, if found one
         */
        uint64_t get_default_mr(MemoryAttr &attr);

        /**
         * Create and query QPs
         * For create, an optional local_attr can be provided to bind to this QP
         * A local MR is passed as the default local mr for this QP.
         * If local_attr = nullptr, then this QP is unbind to any MR.
         */
        RCQP *
        create_rc_qp(QPIdx idx, RNicHandler *dev, MemoryAttr *attr = NULL, ibv_cq *cq = NULL, ibv_cq *recv_cq = NULL);

        void
        destroy_rc_qp(QPIdx idx);


        RCQP *
        create_uc_qp(QPIdx idx, RNicHandler *dev, MemoryAttr *attr = NULL, ibv_cq *cq = NULL, ibv_cq *recv_cq = NULL);

        UDQP *create_ud_qp(QPIdx idx, RNicHandler *dev, MemoryAttr *attr = NULL);

        RCQP *get_rc_qp(QPIdx idx);

        UDQP *get_ud_qp(QPIdx idx);

        /**
         * Some helper functions (example usage of RdmaCtrl)
         * Fully link the QP in a symmetric way, for this thread.
         * For example, node 0 can connect to node 1, while node 1 connect to node 0.
         */
//        bool link_symmetric_rcqps(const std::vector<std::string> &cluster,
//                                  int l_mrid, uint64_t mr_id, int wid, int idx = 0);

        std::set<uint64_t> terminated_node_ids();

        bool broadcast_termination(uint64_t thread_id, std::vector<std::string> ip, std::vector<int> port,
                                   uint64_t my_node_id,
                                   uint64_t nthreads);

    private:
        class RdmaCtrlImpl;

        std::unique_ptr<RdmaCtrlImpl> impl_;
        std::mutex terminate_mutex_;
        bool termination_broadcasted_ = false;
        std::set<uint64_t> terminated_thread_ids_;
    };
} // namespace rdmaio

#include "rdma_ctrl_impl.hpp" // real implemeatation here
