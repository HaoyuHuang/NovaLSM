
//
// Created by Haoyu Huang on 4/1/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova_common.h"

namespace nova {
uint32_t str_to_int(const char *str, uint64_t *out, uint32_t nkey) {
  if (str[0] == MSG_TERMINATER_CHAR) {
    return 0;
  }
  uint32_t len = 0;
  uint64_t x = 0;
  while (str[len] != TERMINATER_CHAR) {
    if (nkey != 0 && len == nkey) {
      break;
    }
    if (str[len] > '9' || str[len] < '0') {
      break;
    }
    x = x * 10 + (str[len] - '0');
    len += 1;
  }
  *out = x;
  return len + 1;
}

uint32_t nint_to_str(uint64_t x) {
  uint32_t len = 0;
  do {
    x = x / 10;
    len++;
  } while (x);
  return len;
}

std::vector<std::string> SplitByDelimiter(std::string *s,
                                          std::string delimiter) {
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

std::string ToString(const std::vector<uint32_t> &x) {
  if (x.empty()) {
    return "";
  }
  stringstream result;
  auto it = x.begin();
  result << *it;
  it++;
  for (; it != x.end(); it++) {
    result << ",";
    result << *it;
  }
  return result.str();
}

std::string DBName(const std::string &dbname, uint32_t sid, uint32_t index) {
  return dbname + "/" + std::to_string(sid) + "/" + std::to_string(index);
}

void ParseDBName(const std::string &logname, uint32_t *sid, uint32_t *index) {
  int iend = logname.find_last_of('/') - 1;
  int istart = logname.find_last_of('/', iend) + 1;
  int send = istart - 2;
  int sstart = logname.find_last_of('/', send) + 1;

  uint64_t i64;
  uint64_t s64;
  str_to_int(logname.data() + istart, &i64, iend - istart + 1);
  str_to_int(logname.data() + sstart, &s64, send - sstart + 1);

  *sid = s64;
  *index = i64;
}

uint64_t LogFileHash(const std::string &logname) {
  uint32_t sid;
  uint32_t index;
  ParseDBName(logname, &sid, &index);
  uint64_t hash = ((uint64_t)sid) << 32;
  return hash + index;
}

// inline __attribute__ ((always_inline))
uint32_t int_to_str(char *str, uint64_t x) {
  int len = 0, p = 0;
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

Host convert_host(string host_str) {
  RDMA_LOG(rdmaio::INFO) << host_str;

  std::vector<std::string> ip_port;
  std::stringstream ss_ip_port(host_str);
  while (ss_ip_port.good()) {
    std::string substr;
    getline(ss_ip_port, substr, ':');
    ip_port.push_back(substr);
  }
  return Host{ip_port[0], atoi(ip_port[1].c_str())};
}

vector<Host> convert_hosts(string hosts_str) {
  RDMA_LOG(rdmaio::INFO) << hosts_str;
  vector<Host> hosts;
  std::stringstream ss_hosts(hosts_str);
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
    hosts.push_back(Host{ip_port[0], atoi(ip_port[1].c_str())});
  }
  return hosts;
}
}  // namespace nova