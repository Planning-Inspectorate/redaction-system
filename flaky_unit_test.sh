#!/bin/bash

FAIL_LOG="pytest_failures.txt"
RUNS=20

# Clear previous log
> "$FAIL_LOG"

for i in $(seq 1 $RUNS); do
    echo "Run #$i"
    python -m pytest -rP -n 4 redactor/test/unit_test >> "$FAIL_LOG" 2>&1
    # Checks if the exit status of the previous command is not zero (indicating failure); if so, executes the following block.
    if [ $? -ne 0 ]; then
        echo "Failure on run #$i" >> "$FAIL_LOG";
    else
        # Clear the log if the run was successful
        echo "Run #$i succeeded."
        > "$FAIL_LOG";
    fi
done

echo "Completed $RUNS runs. Failures (if any) are logged in $FAIL_LOG."