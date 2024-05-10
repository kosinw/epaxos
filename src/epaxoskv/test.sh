#!/usr/bin/env bash

rm -rf 2024*

../../scripts/dstest -v -v --workers 1 --iter $1 --race -t \
    TestBasic4A \
    TestSpeed4A \
    TestConcurrent4A \
    TestUnreliable4A \
    TestUnreliableOneKey4A \
    TestOnePartition4A \
    TestManyPartitionsOneClient4A \
    TestManyPartitionsManyClients4A \
    TestPersistOneClient4A \
    TestPersistConcurrent4A \
    TestPersistConcurrentUnreliable4A \
    TestPersistPartition4A \
    TestPersistPartitionUnreliable4A \
    TestPersistPartitionUnreliableLinearizable4A \
