#!/bin/bash
set -e

# Start the original Flink JobManager process in background
echo "Starting Flink JobManager..."
/docker-entrypoint.sh jobmanager &

# Tunggu sampai Flink UI (JobManager) siap di port 8081
echo "Waiting for Flink JobManager to be ready..."
until curl -s http://localhost:8081/overview; do
  sleep 2
done

echo "Flink JobManager is ready. Submitting the Python job..."

# Submit your PyFlink job
flink run -py /opt/flink/usr_jobs/flights_data.py

# Keep container running
wait