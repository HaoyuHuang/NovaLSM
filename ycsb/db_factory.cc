//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db_factory.h"

#include <string>
#include "basic_db.h"
#include "nova_mem_client_db.h"

using namespace std;

namespace ycsbc {
    DB *DBFactory::CreateDB(utils::Properties props) {
        if (props["dbname"] == "basic") {
            return new BasicDB;
        } else if (props["dbname"] == "nova") {
            return new NovaMemClientDB(props);
        }
        return NULL;
    }
}


