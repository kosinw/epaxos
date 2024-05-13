#!/bin/bash

VERBOSE=1 go test -run TestBasic4A | ../../scripts/dslogs -c 5 -j PACCEPT,ACCEPT,COMMIT,EXECUTE