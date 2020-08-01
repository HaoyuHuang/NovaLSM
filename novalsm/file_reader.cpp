//
// Created by haoyuhua on 8/1/20.
//

#include <cstdio>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <fmt/core.h>
#include <gflags/gflags.h>
#include <iostream>
#include <assert.h>

DEFINE_string(file_path, "/tmp/db", "level db path");
DEFINE_uint32(position, 0, "StoC files path");
DEFINE_uint32(size, 0, "StoC files path");

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    const char *fname = FLAGS_file_path.c_str();
    FILE *filp = fopen(fname, "rb");
    if (!filp) {
        printf("Error: could not open file %s\n", fname);
        return -1;
    }

    char *buffer = new char[FLAGS_size];
    int bytes = fread(buffer, sizeof(char), FLAGS_size, filp);
    assert(bytes == FLAGS_size);
    std::string  val;
    for (int i = 0; i < FLAGS_position; i++) {
        val += buffer[i];
    }

    std::cout << fmt::format("{} {}", (int)(buffer[FLAGS_position]), val);
    // Done and close.
    fclose(filp);
    return 0;
}