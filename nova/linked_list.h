
//
// Created by Haoyu Huang on 4/8/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_LINKED_LIST_H
#define RLIB_LINKED_LIST_H

#include <iostream>
#include <string>
#include <sstream>
#include "nova_common.h"

namespace nova {
    template<class T>
    class NovaList {
    public:
        NovaList() {
            backing_array_ = new T[NOVA_LIST_BACK_ARRAY_SIZE];
        }

        void append(const T data) {
            if (index_ == size_) {
                expand();
            }
            backing_array_[index_] = data;
            index_++;
        }

        T poll() {
            if (index_ == 0) {
                return nullptr;
            }
            index_--;
            return backing_array_[index_];
        }

        int size() {
            return index_;
        }

        T value(int index) {
            if (index < index_) {
                return backing_array_[index];
            }
        }

        T *values() {
            return backing_array_;
        }

        void expand() {
            T *new_array = new T[size_ + NOVA_LIST_BACK_ARRAY_SIZE];
            for (int i = 0; i < size_; i++) {
                new_array[i] = backing_array_[i];
            }
            size_ += NOVA_LIST_BACK_ARRAY_SIZE;
            delete backing_array_;
            backing_array_ = new_array;
        }

    private:
        T *backing_array_;
        int index_ = 0;
        int size_ = NOVA_LIST_BACK_ARRAY_SIZE;
    };
}

#endif //RLIB_LINKED_LIST_H
