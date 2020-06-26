#!/bin/bash

# Log progress
get_time (){
    date +%Y%m%d%H%M
}
echo "Start time: " $(get_time)  > import.log
for script in $(ls -1 [0-9]*.py); do
    echo "Running: " $(get_time) " :$script" >> import.log
    python3 $script
done

echo "End time: " $(get_time)  >> import.log
