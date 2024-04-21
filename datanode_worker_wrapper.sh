#!/bin/bash

./bin/datanode &
./bin/worker &
wait -n

exit$?