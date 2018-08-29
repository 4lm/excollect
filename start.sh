#!/usr/bin/env bash

python3 excollect.py >> /dev/null 2>&1 & echo $! >> pid.log
