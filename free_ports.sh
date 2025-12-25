#!/bin/bash

# Ports to free
PORTS=$(seq 5001 5010)  
PORTS="$PORTS 6001"

for PORT in $PORTS; do
    # Find process using the port
    PID=$(lsof -t -i:$PORT)

    if [ -n "$PID" ]; then
        echo "Killing process $PID on port $PORT"
        kill -9 $PID
    else
        echo "Port $PORT is free"
    fi
done