//
// Created by haoyuhua on 8/18/20.
//

#ifndef ROCKSDB_DB_STATS_H
#define ROCKSDB_DB_STATS_H

#include "logging.hpp"
#include "nova_common.h"
#include "rocksdb/db.h"

namespace nova {
class DBStats {
 public:
  void Start();
  std::vector<rocksdb::DB*> dbs;
};
}  // namespace nova

#endif  // ROCKSDB_DB_STATS_H
