#!/usr/bin/env bash

case "$#" in
    (0)
        FLAG=local
        ;;
    (1)
        FLAG=$1
        ;;
    (*)
        echo $0: "usage: ./run_spark.sh [local|bigdata]"
        exit 1
        ;;
esac


case ${FLAG} in
    (local)
        # Run application locally
        SUBMIT="/usr/local/spark-2.0.0/bin/spark-submit"
        MASTER='local[*]'
        INPUT='yellow_tripdata_*.csv'
        ;;
    (bigdata)
        # Run application on YARN
        SUBMIT="spark2-submit"
        MASTER='yarn'
        INPUT='hdfs:///data/nyc-tlc/yellow_tripdata_*.csv'
        ;;
    (*)
        echo "Invalid FLAG option [$FLAG]"
        exit 1
        ;;
esac


${SUBMIT} \
  --class edu.vanderbilt.accre.nyctaxi.TaxiApp \
  --master ${MASTER} \
  target/scala-2.11/spark-taxi_2.11-0.1.0.jar \
  ${INPUT}

