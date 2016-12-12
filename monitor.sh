#!/usr/bin/bash

watch -c -n 30 "ps aux | grep -i cp_s3_to_s3 | grep -v watch | grep -v grep"


