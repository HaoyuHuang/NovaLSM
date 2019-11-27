//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include <sys/time.h>
#include <unistd.h>
#include <thread>
#include "utils.h"
#include "timer.h"
#include "client.h"
#include "core_workload.h"
#include "db_factory.h"
#include "logging.h"

using namespace std;
using namespace rdmaio;

#define SIG_OPS 10000

void UsageMessage(const char *command);

bool StrStartWith(const char *str, const char *pre);

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

void DelegateClient(ycsbc::Client *client, ycsbc::DB *db, const int num_ops,
                    bool is_loading) {
    db->Init();
    if (num_ops == 0) {
        return;
    }
    timeval start, tmp_start, end;
    gettimeofday(&start, NULL);
    gettimeofday(&tmp_start, NULL);
    uint64_t oks = 0;
    if (is_loading) {
        for (int i = 0; i < num_ops; ++i) {
            oks += client->DoInsert();
            if (oks > SIG_OPS && oks % SIG_OPS == 0) {
                gettimeofday(&end, NULL);
                RDMA_LOG(INFO) << "loaded " << oks << " tps:"
                               << oks / (end.tv_sec - start.tv_sec);
            }

        }
        return;
    }

    uint64_t prev_throughput = 0;
    while (true) {
        oks += client->DoTransaction();
        gettimeofday(&end, NULL);
        if (end.tv_sec - start.tv_sec >= num_ops) {
            break;
        }
//        if (end.tv_sec - tmp_start.tv_sec >= 1) {
//            int thpt = oks - prev_throughput;
//            RDMA_LOG(INFO) << "client[" << client_id << "] tps: " << thpt / (end.tv_sec - tmp_start.tv_sec);
//            prev_throughput = oks;
//            gettimeofday(&tmp_start, NULL);
//        }
    }
    return;
}

int main(const int argc, const char *argv[]) {
    utils::Properties props;
    string file_name = ParseCommandLine(argc, argv, props);
    const int num_threads = std::stoi(props.GetProperty("threadcount", "1"));

    auto prop_map = props.properties();
    for (auto iter = prop_map.begin(); iter != prop_map.end(); iter++) {
        RDMA_LOG(INFO) << iter->first << ": " << iter->second;
    }

    ycsbc::DB *dbs[num_threads];
    ycsbc::CoreWorkload *wls[num_threads];
    for (int i = 0; i < num_threads; i++) {
        wls[i] = new ycsbc::CoreWorkload;
        wls[i]->Init(props);
    }

//    int load = stoi(props["load"], nullptr);
//
//    if (load == 1) {
//        // Loads data
//        vector<future<int>> actual_ops;
//        int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
//        actual_ops.emplace_back(async(launch::async,
//                                      DelegateClient, 0, dbs[0], wls[0], total_ops, true));
//        assert((int) actual_ops.size() == 1);
//
//        int sum = 0;
//        for (auto &n : actual_ops) {
//            assert(n.valid());
//            sum += n.get();
//        }
//        cerr << "# Loading records:\t" << sum << endl;
//    }

    // Peforms transactions
    vector<std::thread> threads;
    vector<ycsbc::Client *> clients;
    int maxexecutiontime = std::stoi(props["maxexecutiontime"]);
    for (int i = 0; i < num_threads; i++) {
        dbs[i] = ycsbc::DBFactory::CreateDB(props);
        clients.push_back(new ycsbc::Client(*dbs[i], *wls[i]));
        threads.push_back(std::thread(DelegateClient, clients[i], dbs[i],
                                      maxexecutiontime, false));
    }

    timeval expstart;
    timeval start, end;
    uint64_t last_ops = 0;
    gettimeofday(&start, NULL);
    expstart = start;
    int now = 0;
    while (true) {
        usleep(200000);
        gettimeofday(&end, NULL);
        uint64_t sec = end.tv_sec - start.tv_sec;
        uint64_t msec = (end.tv_usec - start.tv_usec) / 1000;
        if (sec >= 1) {
            start = end;
            uint64_t duration = sec * 1000 + msec;
            uint64_t ops = 0;
            for (int i = 0; i < clients.size(); i++) {
                ops += clients[i]->ops;
            }
            uint64_t thpt = (ops - last_ops) * 1000 / duration;
            RDMA_LOG(INFO) << now << " sec throughput " << thpt;
            last_ops = ops;
            now++;
        }
        if (end.tv_sec - expstart.tv_sec > maxexecutiontime) {
            break;
        }
    }

    for (auto &t : threads) {
        t.join();
    }
}

string
ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
    int argindex = 1;
    string filename;
    while (argindex < argc && StrStartWith(argv[argindex], "-")) {
        if (strcmp(argv[argindex], "-load") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("load", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-nova_client_mode") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("nova_client_mode", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-nova_servers") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("nova_servers", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-shedload") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("shedload", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-valuesize") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("valuesize", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-recordcount") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty(ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY,
                              argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-maxexecutiontime") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("maxexecutiontime", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-threads") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("threadcount", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-db") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("dbname", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-requestdistribution") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("requestdistribution", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-port") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("port", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-slaves") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("slaves", argv[argindex]);
            argindex++;
        } else if (strcmp(argv[argindex], "-P") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            filename.assign(argv[argindex]);
            ifstream input(argv[argindex]);
            try {
                props.Load(input);
            } catch (const string &message) {
                cout << message << endl;
                exit(0);
            }
            input.close();
            argindex++;
        } else {
            cout << "Unknown option '" << argv[argindex] << "'" << endl;
            exit(0);
        }
    }

    if (argindex == 1 || argindex != argc) {
        UsageMessage(argv[0]);
        exit(0);
    }

    return filename;
}

void UsageMessage(const char *command) {
    cout << "Usage: " << command << " [options]" << endl;
    cout << "Options:" << endl;
    cout << "  -threads n: execute using n threads (default: 1)" << endl;
    cout << "  -db dbname: specify the name of the DB to use (default: basic)"
         << endl;
    cout
            << "  -P propertyfile: load properties from the given file. Multiple files can"
            << endl;
    cout
            << "                   be specified, and will be processed in the order specified"
            << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
    return strncmp(str, pre, strlen(pre)) == 0;
}

