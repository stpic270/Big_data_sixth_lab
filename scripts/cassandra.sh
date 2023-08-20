#!/bin/bash
python create_table.py
echo "Sleep for 30 s before transport data to cassandra"
sleep 30
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 use_cassandra.py