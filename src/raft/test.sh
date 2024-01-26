#!/usr/bin/env bash

rm -rf 2024*

../../scripts/dstest --verbose --workers 8 --iter $1 --race -t \
    TestInitialElection3A \
    TestReElection3A \
    TestManyElections3A \
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
    TestPersist13C \
    TestPersist3C \
    TestPersist33C \
    TestFigure83C \
    TestUnreliableAgree3C \
    TestFigure8Unreliable3C \
    TestReliableChurn3C \
    TestUnreliableChurn3C \
    TestSnapshotBasic3D \
    TestSnapshotInstall3D \
    TestSnapshotInstallUnreliable3D \
    TestSnapshotInstallCrash3D \
    TestSnapshotInstallUnCrash3D \
    TestSnapshotAllCrash3D \
    TestSnapshotInit3D
