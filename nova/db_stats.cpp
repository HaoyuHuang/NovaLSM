//
// Created by haoyuhua on 8/18/20.
//

#include "db_stats.h"

#include <unistd.h>
namespace nova {
void DBStats::Start() {
  while (true) {
    for (uint32_t i = 0; i < dbs.size(); i++) {
      if (!dbs[i]) {
        continue;
      }
      RDMA_LOG(rdmaio::INFO) << "Database " << i;
      dbs[i]->GetOptions().Dump(dbs[i]->GetOptions().info_log.get());
      std::string value;
      dbs[i]->GetProperty("rocksdb.stats", &value);
      RDMA_LOG(rdmaio::INFO) << "\n"
                             << "rocksdb stats " << value;
    }
    sleep(600);
  }
}
}  // namespace nova