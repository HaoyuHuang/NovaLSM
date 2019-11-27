
//
// Created by Haoyu Huang on 4/1/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MSG_CALLBACK_H
#define RLIB_NOVA_MSG_CALLBACK_H

class NovaMsgCallback {
public:
    virtual void ProcessRDMAREAD(char *buf) = 0;
};


#endif //RLIB_NOVA_MSG_CALLBACK_H
