#!/usr/bin/env bash

cd $PROJECT_HOME/platform-scripts/shell/local
source model-config.sh
source replay-utils.sh

if [ -z "$job_config" ]; then job_config=$(config $1 '__endDate__'); fi
start_date=$2
end_date=$3

backup $start_date $end_date "sandbox-data-store" "$1" "backup-$1"
if [ $? == 0 ]
 	then
  	echo "Backup completed Successfully..."
  	echo "Running the $1 job replay..."
  	$SPARK_HOME/bin/spark-submit --master local[*] --jars $PROJECT_HOME/platform-framework/analytics-job-driver/target/analytics-framework-1.0.jar --class org.ekstep.analytics.job.ReplaySupervisor $PROJECT_HOME/platform-modules/batch-models/target/batch-models-1.0.jar --model "$1" --fromDate "$start_date" --toDate "$end_date" --config "$job_config" > "logs/$end_date-$1-replay.log"
else
  	echo "Unable to take backup"
fi

if [ $? == 0 ]
	then
	echo "$1 replay executed successfully"
	delete "sandbox-data-store" "backup-$1"
else
	echo "$1 replay failed"
 	rollback "sandbox-data-store" "$1" "backup-$1"
 	delete "sandbox-data-store" "backup-$1"
fi