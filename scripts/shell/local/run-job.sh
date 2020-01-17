#!/usr/bin/env bash

#export SPARK_HOME=<add spark-installtion-directory>/spark-1.5.0-bin-hadoop2.3
#export PROJECT_HOME=<add repo directory>/sunbird-analytics
#export DP_LOGS=$PROJECT_HOME/platform-scripts/shell/local/logs

## Job to run daily
cd $PROJECT_HOME/platform-scripts/shell/local
source model-config.sh
today=$(date "+%Y-%m-%d")

if [ -z "$2" ]; then job_config=$(config $1); else job_config="$2"; fi

echo "Starting the job - $1" >> "$DP_LOGS/$today-job-execution.log"

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars $PROJECT_HOME/platform-framework/analytics-job-driver/target/analytics-framework-1.0.jar --class org.ekstep.analytics.job.JobExecutor $PROJECT_HOME/platform-modules/batch-models/target/batch-models-1.0.jar --model "$1" --config "$job_config" >> "$DP_LOGS/$today-job-execution.log" 2>&1

echo "Job execution completed - $1" >> "$DP_LOGS/$today-job-execution.log"