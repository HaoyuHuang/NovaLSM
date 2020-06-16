#pragma once

#include <infiniband/verbs.h>
#include "common/nova_console_logging.h"

namespace rdmaio {

    struct MemoryAttr {
        uintptr_t buf;
        uint32_t key;
    };

    class Memory {
    public:
        /**
         * The default protection flag of a memory region.
         * In default, the memory can be read/write by local and remote RNIC operations.
         */
        static const int DEFAULT_PROTECTION_FLAG = (IBV_ACCESS_LOCAL_WRITE |
                                                    IBV_ACCESS_REMOTE_READ | \
                                              IBV_ACCESS_REMOTE_WRITE |
                                                    IBV_ACCESS_REMOTE_ATOMIC);

        Memory(const char *addr, uint64_t len, ibv_pd *pd, int flag) :
                addr(addr),
                len(len),
                mr(ibv_reg_mr(pd, (void *) addr, len, flag)) {
            if (mr == nullptr) {
                NOVA_LOG(WARNING) << "failed to register mr, for addr " << addr
                                  << "; len " << len << " errno: "
                                  << strerror(errno);
            } else {
                rattr.buf = (uintptr_t) addr;
                rattr.key = mr->rkey;
            }
        }

        ~Memory() {
            if (mr != nullptr) {
                int rc = ibv_dereg_mr(mr);
                NOVA_LOG_IF(ERROR, rc != 0) << "dereg mr error: "
                                            << strerror(errno);
            }
        }

        bool valid() {
            return mr != nullptr;
        }

        const char *addr;
        uint64_t len;

        MemoryAttr rattr;        // RDMA registered attr
        ibv_mr *mr = nullptr;    // mr in the driver
    };


}; // namespace rdmaio
