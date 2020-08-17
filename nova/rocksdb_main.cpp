
//
// Created by Haoyu Huang on 2/20/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <assert.h>
#include <gflags/gflags.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include <csignal>
#include <thread>

#include "env/io_posix.h"
#include "file/filename.h"
#include "logging.hpp"
#include "nic_server.h"
#include "nova_common.h"
#include "nova_config.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"

using namespace std;
using namespace rdmaio;
using namespace nova;

NovaConfig *NovaConfig::config;

DEFINE_string(db_path, "/tmp/nova", "level db path");
DEFINE_uint64(block_cache_mb, 0, "leveldb block cache size in mb");
DEFINE_uint64(write_buffer_size_mb, 0, "write buffer size in mb");
DEFINE_uint64(num_memtables, 0, "Number of memtables");
DEFINE_uint64(max_msg_size, 0, "Maximum message size");

DEFINE_uint32(num_async_workers, 0, "Number of async worker threads.");
DEFINE_uint32(num_compaction_workers, 0,
              "Number of compaction worker threads.");
DEFINE_string(profiler_file_path, "", "profiler file path.");
DEFINE_string(servers, "localhost:11211", "A list of peer servers");
DEFINE_int64(server_id, -1, "Server id.");
DEFINE_uint64(recordcount, 0, "Number of records.");
DEFINE_string(data_partition_alg, "hash",
              "Data partition algorithm: hash, range, debug.");
DEFINE_uint64(num_conn_workers, 0, "Number of connection threads.");
DEFINE_uint64(cache_size_gb, 0, " Cache size in GB.");
DEFINE_uint64(use_fixed_value_size, 0, "Fixed value size.");

DEFINE_bool(enable_load_data, false, "Enable loading data.");
DEFINE_string(config_path, "/tmp/uniform-3-32-10000000-frags.txt",
              "The path that stores fragment configuration.");
DEFINE_uint32(l0_start_compaction_mb, 0, "");
DEFINE_uint32(l0_stop_write_mb, 0, "");
DEFINE_int32(level, 0, "");

namespace {
class YCSBKeyComparator : public rocksdb::Comparator {
 public:
  //   if a < b: negative result
  //   if a > b: positive result
  //   else: zero result
  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const {
    uint64_t ai = 0;
    str_to_int(a.data(), &ai, a.size());
    uint64_t bi = 0;
    str_to_int(b.data(), &bi, b.size());

    if (ai < bi) {
      return -1;
    } else if (ai > bi) {
      return 1;
    }
    return 0;
  }

  // Ignore the following methods for now:
  const char *Name() const { return "YCSBKeyComparator"; }

  void FindShortestSeparator(std::string *, const rocksdb::Slice &) const {}

  void FindShortSuccessor(std::string *) const {}
};
}  // namespace

void start(NovaMemServer *server) { server->Start(); }

static void _mkdir(const char *dir) {
  char tmp[1024];
  char *p = NULL;
  size_t len;

  snprintf(tmp, sizeof(tmp), "%s", dir);
  len = strlen(tmp);
  if (tmp[len - 1] == '/') tmp[len - 1] = 0;
  for (p = tmp + 1; *p; p++) {
    if (*p == '/') {
      *p = 0;
      mkdir(tmp, 0777);
      *p = '/';
    }
  }
  mkdir(tmp, 0777);
}

rocksdb::DB *CreateDatabase(int sid, int db_index) {
  rocksdb::DB *db = nullptr;
  rocksdb::Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_number = FLAGS_num_memtables;
  options.target_file_size_base = FLAGS_write_buffer_size_mb * 1024 * 1024;
  options.target_file_size_multiplier = 1;
  options.max_bytes_for_level_base =
      FLAGS_write_buffer_size_mb * 1024 * 1024 * FLAGS_num_memtables;
  options.max_bytes_for_level_multiplier = 3.4;
  options.compaction_style = rocksdb::kCompactionStyleLevel;
  // open DB
  std::string db_path = DBName(NovaConfig::config->db_path, sid, db_index);
  _mkdir(db_path.c_str());

  options.write_buffer_size = FLAGS_write_buffer_size_mb * 1024 * 1024;
  options.compression = rocksdb::CompressionType::kNoCompression;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  options.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_options));
  options.level0_file_num_compaction_trigger =
      FLAGS_l0_start_compaction_mb / FLAGS_write_buffer_size_mb;
  options.level0_stop_writes_trigger =
      FLAGS_l0_stop_write_mb / FLAGS_write_buffer_size_mb;
  options.level0_slowdown_writes_trigger = -1;
  options.num_levels = FLAGS_level;
  options.comparator = new YCSBKeyComparator();
  RDMA_ASSERT(rocksdb::Env::Default()
                  ->NewLogger(db_path + "/LOG-" + std::to_string(db_index),
                              &options.info_log)
                  .ok());
  rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
  RDMA_ASSERT(status.ok()) << "Open leveldb failed " << status.ToString();
  return db;
}

int main(int argc, char *argv[]) {
  //    system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  int i;
  const char **methods = event_get_supported_methods();
  printf("Starting Libevent %s.  Available methods are:\n",
         event_get_version());
  for (i = 0; methods[i] != NULL; ++i) {
    printf("    %s\n", methods[i]);
  }
  if (FLAGS_server_id == -1) {
    exit(0);
  }
  std::vector<gflags::CommandLineFlagInfo> flags;
  gflags::GetAllFlags(&flags);
  for (const auto &flag : flags) {
    printf("%s=%s\n", flag.name.c_str(), flag.current_value.c_str());
  }
  // data
  NovaConfig::config = new NovaConfig();
  NovaConfig::config->my_server_id = FLAGS_server_id;
  std::string servers = FLAGS_servers;
  printf("Servers %s\n", servers.c_str());
  NovaConfig::config->servers = convert_hosts(servers);
  NovaConfig::config->num_conn_workers = FLAGS_num_conn_workers;
  NovaConfig::config->cache_size_gb = FLAGS_cache_size_gb;
  NovaConfig::config->load_default_value_size = FLAGS_use_fixed_value_size;
  NovaConfig::config->max_msg_size = FLAGS_max_msg_size;
  string data_partition = FLAGS_data_partition_alg;
  // LevelDB
  NovaConfig::config->db_path = FLAGS_db_path;
  NovaConfig::config->num_async_workers = FLAGS_num_async_workers;
  NovaConfig::config->l0_start_compaction_bytes =
      FLAGS_l0_start_compaction_mb * 1024 * 1024;

  NovaConfig::config->enable_load_data = FLAGS_enable_load_data;
  std::string path = FLAGS_config_path;
  printf("config path=%s\n", path.c_str());

  if (data_partition.find("hash") != string::npos) {
    NovaConfig::config->partition_mode = NovaRDMAPartitionMode::HASH;
  } else if (data_partition.find("range") != string::npos) {
    NovaConfig::config->partition_mode = NovaRDMAPartitionMode::RANGE;
  } else {
    NovaConfig::config->partition_mode = NovaRDMAPartitionMode::DEBUG_RDMA;
  }

  RDMA_LOG(INFO) << NovaConfig::config->to_string();
  NovaConfig::config->ReadFragments(path);
  uint64_t ntotal = NovaConfig::config->cache_size_gb * 1024 * 1024 * 1024;
  RDMA_LOG(INFO) << "Allocated buffer size in bytes: " << ntotal;

  auto *buf = (char *)malloc(ntotal);
  memset(buf, 0, ntotal);
  RDMA_ASSERT(buf != NULL) << "Not enough memory";

  int ndbs = NovaConfig::config->ParseNumberOfDatabases(
      NovaConfig::config->my_server_id);
  std::vector<rocksdb::DB *> dbs;

  for (int db_index = 0; db_index < ndbs; db_index++) {
    dbs.push_back(CreateDatabase(NovaConfig::config->my_server_id, db_index));
  }
  int port = NovaConfig::config->servers[NovaConfig::config->my_server_id].port;
  auto *mem_server = new NovaMemServer(dbs, buf, port);
  mem_server->Start();
  return 0;
}
