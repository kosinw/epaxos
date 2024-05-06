#!/usr/bin/env bash

rm -rf 2024*

../../scripts/dstest -v --workers 3 --iter $1 --race -t \
    TestBasicAgree3B \
    TestRPCBytes3B \
    TestFollowerFailure3B \
    TestLeaderFailure3B \
    TestFailAgree3B \
    TestFailNoAgree3B \
    TestConcurrentStart3B \
    TestRejoin3B \
    TestBackup3B \
    TestCount3B \
