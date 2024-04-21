#!/bin/bash

./bin/namenode &
./bin/foreman &
wait -n

exit$?