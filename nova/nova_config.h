
//
// Created by Haoyu Huang on 2/24/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MEM_CONFIG_H
#define RLIB_NOVA_MEM_CONFIG_H

#include <fstream>
#include <sstream>
#include <string>
#include <set>

#include "rocksdb/options.h"
#include "nova_common.h"

namespace nova {
using namespace std;
using namespace rdmaio;

enum NovaRDMAPartitionMode { RANGE = 0, HASH = 1, DEBUG_RDMA = 2 };

struct Fragment {
  // for range partition only.
  uint64_t key_start;
  uint64_t key_end;
  std::vector<uint32_t> server_ids;
  uint32_t dbid;
};

class NovaConfig {
 public:
  static uint64_t keyhash(const char *key, uint64_t nkey) {
    uint64_t hv = 0;
    str_to_int(key, &hv, nkey);
    return hv;
  }

  static Fragment *home_fragment(uint64_t key) {
    if (config->partition_mode == NovaRDMAPartitionMode::HASH) {
      return config->fragments[key % config->nfragments];
    } else if (config->partition_mode == NovaRDMAPartitionMode::RANGE) {
      Fragment *home = nullptr;
      RDMA_ASSERT(key < config->fragments[config->nfragments - 1]->key_end);
      uint32_t l = 0;
      uint32_t r = config->nfragments - 1;

      while (l <= r) {
        uint32_t m = l + (r - l) / 2;
        home = config->fragments[m];
        // Check if x is present at mid
        if (key >= home->key_start && key < home->key_end) {
          break;
        }
        // If x greater, ignore left half
        if (key >= home->key_end) l = m + 1;
        // If x is smaller, ignore right half
        else
          r = m - 1;
      }
      RDMA_ASSERT(home->server_ids[0] == config->my_server_id)
          << key << ":" << ToString(home->server_ids) << ":"
          << config->my_server_id;
      return home;
    }
    assert(false);
  }

  int ParseNumberOfDatabases(uint32_t server_id) {
    std::set<uint32_t> ndbs;
    for (uint32_t i = 0; i < nfragments; i++) {
      if (fragments[i]->server_ids[0] == server_id) {
        ndbs.insert(fragments[i]->dbid);
      }
    }
    db_fragment = (Fragment **)malloc(ndbs.size() * sizeof(Fragment *));
    for (uint32_t i = 0; i < nfragments; i++) {
      if (fragments[i]->server_ids[0] == server_id) {
        db_fragment[fragments[i]->dbid] = fragments[i];
      }
    }
    return ndbs.size();
  }

  void ReadFragments(const std::string &path) {
    std::string line;
    ifstream file;
    file.open(path);
    vector<Fragment *> frags;
    while (std::getline(file, line)) {
      if (line.find("config") != std::string::npos) {
        // These fields are not used for leveldb.
        // LTC
        assert(std::getline(file, line));
        // StoC
        assert(std::getline(file, line));
        // Start time
        assert(std::getline(file, line));
        continue;
      }
      auto *frag = new Fragment();
      std::vector<std::string> tokens = SplitByDelimiter(&line, ",");
      frag->key_start = std::stoi(tokens[0]);
      frag->key_end = std::stoi(tokens[1]);
      frag->dbid = std::stoi(tokens[3]);

      int nreplicas = (tokens.size() - 4);
      for (int i = 0; i < nreplicas; i++) {
        frag->server_ids.push_back(std::stoi(tokens[i + 4]));
      }
      frags.push_back(frag);
    }
    nfragments = static_cast<uint32_t>(frags.size());
    fragments = (Fragment **)malloc(nfragments * sizeof(Fragment *));
    for (uint32_t i = 0; i < nfragments; i++) {
      fragments[i] = frags[i];
    }
    RDMA_LOG(INFO) << "Configuration has a total of " << frags.size()
                   << " fragments.";
    for (uint32_t i = 0; i < nfragments; i++) {
      RDMA_LOG(INFO) << "frag[" << i << "]: " << fragments[i]->key_start << "-"
                     << fragments[i]->key_end << "-"
                     << ToString(fragments[i]->server_ids) << "-"
                     << fragments[i]->dbid;
    }
  }

  string to_string() {
    return "";
  }

  bool enable_load_data;
  uint64_t l0_start_compaction_bytes;

  vector<Host> servers;
  int num_conn_workers;
  int num_async_workers;
  uint32_t my_server_id;
  uint64_t load_default_value_size;
  uint32_t nfragments;
  uint64_t cache_size_gb;
  Fragment **fragments;
  Fragment **db_fragment;
  uint32_t max_msg_size;

  // LevelDB.
  std::string db_path;
  NovaRDMAPartitionMode partition_mode;
  static NovaConfig *config;
};
}  // namespace nova
#endif  // RLIB_NOVA_MEM_CONFIG_H
