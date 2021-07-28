#!/bin/bash
python3 etl_job.py 2>/dev/null
if [ $? != 0 ];
then
    echo "EXIT 1"
fi
echo "EXIT 0"
