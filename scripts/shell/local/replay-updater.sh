#!/usr/bin/env bash

cd $PROJECT_HOME/platform-scripts/shell/local
source model-config.sh
source replay-utils.sh

job_config=$(config $1 '__endDate__')
start_date=$2
end_date=$3

echo "Running the $1 updater replay..."
$SPARK_HOME/bin/spark-submit --master local[*] --jars $PROJECT_HOME/platform-framework/analytics-job-driver/target/analytics-framework-1.0.jar --class org.ekstep.analytics.job.ReplaySupervisor $PROJECT_HOME/platform-modules/batch-models/target/batch-models-1.0.jar --model "$1" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-$1-replay.log"

if [ $? == 0 ] 
	then
  	echo "$1 updater replay executed successfully..."
else
 	echo "$1 updater replay failed"
 	exit 1
fi