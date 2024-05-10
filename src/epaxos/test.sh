#!/usr/bin/env bash

rm -rf 2024*

../../scripts/dstest -v --workers 3 --iter $1 --race -t \
    TestBasicCommit \
    TestMultipleCommit \
    TestOneCommit \
    TestExecute \
    TestExecute2 \
    TestSCCChecker \
    TestEP2 \
    TestEPBackup \
    TestEPBack \
    TestEPReexecution \
    TestEPRecovery \
    TestEP3 \
    TestBasicAgree3B \
    TestRPCBytes3B \
    TestFollowerFailure3B \
    TestLeaderFailure3B \
    TestFailAgree3B \
    TestFailNoAgree3B \
    TestConcurrentStarts3B \
    TestRejoin3B \
    TestBackup3B \
    TestCount3B \
    TestPersist13C \
    TestPersist23C \
    TestPersist33C \
    TestUnreliableAgree3C \
    # TestFigure83C \
    # TestFigure8Unreliable3C \
    # TestReliableChurn3C \
    # TestUnreliableChurn3C  \
