#!/usr/bin/env bash

export SPARK_HOME=<add spark-installtion-directory>/spark-2.4.4-bin-hadoop2.7
export MODELS_HOME=<add models directory>/models-2.0
export DP_LOGS=<add logs directory>/logs/data-products

## Job to run daily
cd <add scripts directory>
# example wfs config
# echo '{"search":{"type":"azure","queries":[{"bucket":"'$bucket'","prefix":"unique-partition/__partition__/","endDate":"'$endDate'","delta":0}]},"model":"org.ekstep.analytics.model.WorkflowSummary","modelParams":{"apiVersion":"v2", "parallelization":200},"output":[{"to":"kafka","params":{"brokerList":"'$brokerList'","topic":"'$topic'"}}],"parallelization":200,"appName":"Workflow Summarizer","deviceMapping":true}'
source model-config.sh
today=$(date "+%Y-%m-%d")

job_config=$(config $1)
start_partition=$2
end_partition=$3

echo "Starting the job - WFS" >> "$DP_LOGS/$today-job-execution.log"

$SPARK_HOME/bin/spark-submit --master local[*] --jars $MODELS_HOME/analytics-framework-2.0.jar,$MODELS_HOME/scruid_2.11-2.3.2.jar --class org.ekstep.analytics.job.WFSExecutor $MODELS_HOME/batch-models-2.0.jar --model "$1" --fromPartition $start_partition --toPartition $end_partition --config "$job_config" >> "$DP_LOGS/$today-job-execution.log"

echo "Job execution completed - $1" >> "$DP_LOGS/$today-job-execution.log"