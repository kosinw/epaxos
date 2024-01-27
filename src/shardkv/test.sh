#!/usr/bin/env bash

rm -rf 2024*

../../scripts/dstest -v -v -v -v --workers 3 --iter $1 --race -t \
    TestStaticShards5A  \
    TestRejection5A     \
    TestJoinLeave5B     \
    TestSnapshot5B      \
    TestMissChange5B    \
    TestConcurrent1_5B  \
    TestConcurrent2_5B  \
    TestConcurrent3_5B  \
    TestUnreliable1_5B  \
    TestUnreliable2_5B  \
    TestUnreliable3_5B  \
