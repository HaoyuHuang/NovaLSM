//
// Created by ruihong on 9/30/21.
//
#include <stdlib.h>
#include <string>
#include <iostream>
#include "include/leveldb/db.h"
#include "include/leveldb/filter_policy.h"
#include "leveldb/comparator.h"
int main()
{
  leveldb::DB* db;
  leveldb::Options options;

  options.comparator = leveldb::BytewiseComparator();
  //TODO: implement the FIlter policy before initial the database

  leveldb::Status s = leveldb::DB::Open(options, "mem_leak", &db);
  delete db;
//  DestroyDB("mem_leak", leveldb::Options());
  leveldb::DB::Open(options, "mem_leak", &db);
  std::string value;
  std::string key;
  auto option_wr = leveldb::WriteOptions();
  for (int i = 0; i<1000000; i++){
    key = std::to_string(i);
    key.insert(0, 20 - key.length(), '1');
    value = std::to_string(std::rand() % ( 10000000 ));
    value.insert(0, 400 - value.length(), '1');
    s = db->Put(option_wr, key, value);
    if (!s.ok()){
      std::cerr << s.ToString() << std::endl;
    }

    //     std::cout << "iteration number " << i << std::endl;
  }

  delete db;

}