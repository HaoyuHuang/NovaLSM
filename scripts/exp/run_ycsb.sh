#!/bin/bash

nthreads=$1
nova_servers=$2
debug=$3
partition=$4
recordcount=$5
maxexecutiontime=$6
dist=$7
value_size=${8}
workload=${9}
config_path=${10}
cardinality=${11}
operationcount=${12}
zipfianconstant=${13}
offset=${14}

java -cp /tmp/YCSB-Nova/jdbc/conf:/tmp/YCSB-Nova/jdbc/target/jdbc-binding-0.13.0-SNAPSHOT.jar:/users/haoyu/.m2/repository/org/apache/geronimo/specs/geronimo-jta_1.1_spec/1.1.1/geronimo-jta_1.1_spec-1.1.1.jar:/users/haoyu/.m2/repository/org/apache/htrace/htrace-core4/4.1.0-incubating/htrace-core4-4.1.0-incubating.jar:/users/haoyu/.m2/repository/net/sourceforge/serp/serp/1.13.1/serp-1.13.1.jar:/tmp/YCSB-Nova/core/target/core-0.13.0-SNAPSHOT.jar:/users/haoyu/.m2/repository/org/hdrhistogram/HdrHistogram/2.1.4/HdrHistogram-2.1.4.jar:/users/haoyu/.m2/repository/org/apache/openjpa/openjpa-jdbc/2.1.1/openjpa-jdbc-2.1.1.jar:/users/haoyu/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.9.13/jackson-mapper-asl-1.9.13.jar:/users/haoyu/.m2/repository/org/apache/geronimo/specs/geronimo-jms_1.1_spec/1.1.1/geronimo-jms_1.1_spec-1.1.1.jar:/users/haoyu/.m2/repository/org/apache/openjpa/openjpa-kernel/2.1.1/openjpa-kernel-2.1.1.jar:/users/haoyu/.m2/repository/net/spy/spymemcached/2.11.4/spymemcached-2.11.4.jar:/users/haoyu/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.9.4/jackson-core-asl-1.9.4.jar:/users/haoyu/.m2/repository/commons-collections/commons-collections/3.2.1/commons-collections-3.2.1.jar:/users/haoyu/.m2/repository/commons-lang/commons-lang/2.4/commons-lang-2.4.jar:/users/haoyu/.m2/repository/org/apache/openjpa/openjpa-lib/2.1.1/openjpa-lib-2.1.1.jar:/users/haoyu/.m2/repository/commons-pool/commons-pool/1.5.4/commons-pool-1.5.4.jar:/users/haoyu/.m2/repository/mysql/mysql-connector-java/5.1.44/mysql-connector-java-5.1.44.jar:/users/haoyu/.m2/repository/com/google/guava/guava/21.0/guava-21.0.jar com.yahoo.ycsb.Client -db com.yahoo.ycsb.db.NovaDBClient -P workloads/$workload -P db.properties -s -threads $nthreads -p nova_servers=$nova_servers -p debug=$debug -p partition=$partition -p stringkey=false -p insertorder=ordered -p recordcount=$recordcount -p maxexecutiontime=$maxexecutiontime -p requestdistribution=$dist -p valuesize=$value_size -p config_path=$config_path -p operationcount=$operationcount -p cardinality=$cardinality -p zipfianconstant=$zipfianconstant -p offset=$offset
