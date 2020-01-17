## Adhoc Analysis using Spark Shell

## Setup

```scala
import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
CommonUtil.setS3Conf(sc);

```

## ASER Summary Events Mean Computation

**S3 Files**

```scala
val queries = Option(Array(Query(Option("ekstep-session-summary"), Option("prod.analytics.screener-"), Option("2015-12-20"), Option("2015-12-27"))));
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](sc, Fetcher("S3", None, queries));
val sessSummaries = DataFilter.filter(rdd, Filter("eid","EQ",Option("ME_SESSION_SUMMARY")));
val aserSummaries = sessSummaries.filter(e => "org.ekstep.aser.lite".equals(e.dimensions.gdata.get.id)).cache();
val timeSpent = aserSummaries.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("timeSpent", 0d).asInstanceOf[Double]).cache();
timeSpent.mean();
```

**Local Files**

```scala
val queries = Option(Array(JSONUtils.deserialize[Query]("{\"file\":\"/mnt/data/analytics/akshara_session_summary.log\"}")))
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](sc, Fetcher("local", None, queries));
val timeSpent = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("timeSpent", 0d).asInstanceOf[Double]).cache();
timeSpent.mean();
```

## ASER Screen Summary Events Mean Computation

**Local Files**

```scala
val queries = Option(Array(JSONUtils.deserialize[Query]("{\"file\":\"/mnt/data/analytics/aser-screen-summary.log\"}")))
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](sc, Fetcher("local", None, queries));
val akp = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("activationKeyPage", 0d).asInstanceOf[Double]).cache();
val scp = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("surveyCodePage", 0d).asInstanceOf[Double]).cache();
val cr1 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("childReg1", 0d).asInstanceOf[Double]).cache();
val cr2 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("childReg2", 0d).asInstanceOf[Double]).cache();
val cr3 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("childReg3", 0d).asInstanceOf[Double]).cache();
val al = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("assessLanguage", 0d).asInstanceOf[Double]).cache();
val ll = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("languageLevel", 0d).asInstanceOf[Double]).cache();
val snq1 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("selectNumeracyQ1", 0d).asInstanceOf[Double]).cache();
val an1 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("assessNumeracyQ1", 0d).asInstanceOf[Double]).cache();
val sn2 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("selectNumeracyQ2", 0d).asInstanceOf[Double]).cache();
val an2 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("assessNumeracyQ2", 0d).asInstanceOf[Double]).cache();
val an3 = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("assessNumeracyQ3", 0d).asInstanceOf[Double]).cache();
val sc = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("scorecard", 0d).asInstanceOf[Double]).cache();
val su = rdd.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("summary", 0d).asInstanceOf[Double]).cache();
print(akp.mean,scp.mean,cr1.mean,cr2.mean,cr3.mean,al.mean,ll.mean,snq1.mean,an1.mean,sn2.mean,an2.mean,an3.mean,sc.mean,su.mean);

akp.mean+scp.mean+cr1.mean+cr2.mean+cr3.mean+al.mean+ll.mean+snq1.mean+an1.mean+sn2.mean+an2.mean+an3.mean+sc.mean+su.mean;
```

## Events to Local File

```scala
val queries = Option(Array(Query(Option("ekstep-telemetry"), Option("prod.telemetry.unique-"), Option("2015-10-27"), Option("2016-01-04"))));
val rdd = DataFetcher.fetchBatchData[Map[String,AnyRef]](sc, Fetcher("S3", None, queries));
val aserRDD = DataFilter.filter(rdd, Filter("userId", "EQ", Option("409fe811-ef92-4ec5-b5f6-aceb787fc9ec"))).map(e => JSONUtils.serialize(e));
val aserRDD = rdd.filter(m => "409fe811-ef92-4ec5-b5f6-aceb787fc9ec".equals(m.getOrElse("uid","").asInstanceOf[String])).map(e => JSONUtils.serialize(e));
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "/mnt/data/analytics/409fe811-ef92-4ec5-b5f6-aceb787fc9ec.log")), aserRDD);
```

## Aser Session Summary Events to local file

**Config for Local Files**

```sh
aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2015-12-20","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_363","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/ss-aser-1.log"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-07","startDate":"2015-12-21"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_363","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/ss-aser-2.log"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'
```

**Config for Kafka**

```sh
aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2015-12-20","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_363","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-08","startDate":"2015-12-21"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_363","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'
```

**Run Job**

```sh
nohup spark-submit --master local[*] --jars /home/ec2-user/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.GenericSessionSummarizer /home/ec2-user/models/batch-models-1.0.jar --config "$aser_config" >> "batch-aser.log" &
```

## Akshara Session Summary Events to local file

```sh
akshara_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-07","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"numeracy_369"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_369","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/ss-akshara.log"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

akshara_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-08","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"numeracy_369"}],"model":"org.ekstep.analytics.model.GenericSessionSummary","modelParams":{"contentId":"numeracy_369","modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

nohup spark-submit --master local[*] --jars /home/ec2-user/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.GenericSessionSummarizer /home/ec2-user/models/batch-models-1.0.jar --config "$akshara_config" >> "batch-akshara.log" &
```


## Aser Lite Screen Summary Events to local file

**Config for Local File**

```sh
aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2015-12-20","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/aser-screen-summary.log"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-07","startDate":"2015-12-21"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/aser-screen-summary.log"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'
```

**Config for Kafka**

```sh
aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2015-12-20","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0","modelId":"AserScreenSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-08","startDate":"2015-12-21"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0","modelId":"AserScreenSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'
```

```sh
nohup spark-submit --master local[*] --jars /home/ec2-user/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.AserScreenSummarizer /home/ec2-user/models/batch-models-1.0.jar --config "$aser_config" >> "batch-aser-screen.log" &
```

## Learner Activity Summary Events from and to local file

```sh
la_config='{"search":{"type":"local","queries":[{"file":"/mnt/data/analytics/ss*.log"}]},"filters":[],"model":"org.ekstep.analytics.model.LearnerActivitySummary","modelParams":{"modelVersion":"1.0"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"file","params":{"file":"/mnt/data/analytics/la-events.log"}}],"parallelization":8,"appName":"Learner Activity Summarizer","deviceMapping":false}'

nohup spark-submit --master local[*] --jars /home/ec2-user/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.LearnerActivitySummarizer /home/ec2-user/models/batch-models-1.0.jar --config "$la_config" >> "batch-la.log" &
```

## Aser sessions with more than one attempt

```scala
val queries = Option(Array(Query(Option("ekstep-session-summary"), Option("prod.analytics.screener-"), Option("2015-12-28"), Option("2015-12-29"))));
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](sc, Fetcher("S3", None, queries));
val aserRDD = rdd.filter(e => "org.ekstep.aser.lite".equals(e.dimensions.gdata.get.id)).cache();
val noOfAttempts = aserRDD.map(e => e.edata.eks.asInstanceOf[Map[String,AnyRef]].getOrElse("noOfAttempts", 0).asInstanceOf[Int]).cache();
timeSpent.mean();
```

## Output events to file

```scala
val rdd = loadFile("/Users/Santhosh/Downloads/prod.telemetry.unique-2016-01-02-06-32.json");
val rdd2 = DataFilter.filter(rdd, Array(Filter("eid","IN",Option(List("OE_START","OE_END","OE_ASSESS","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"))),
                Filter("gdata.id","EQ",Option("org.ekstep.aser.lite"))));
val rdd3 = rdd2.map { x => JSONUtils.serialize(x) };
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "/Users/Santhosh/ekStep/github-repos/Learning-Platform-Analytics/platform-modules/batch-models/src/test/resources/test_data1.log")), rdd3)
```

## Migration Scripts in Sandbox

```sh

## Session Summaries
aser_config='{"search":{"type":"s3","queries":[{"bucket":"sandbox-ekstep-telemetry","prefix":"sandbox.telemetry.unique-","startDate":"2016-01-01","endDate":"2016-01-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]}],"model":"org.ekstep.analytics.model.GenericSessionSummaryV2","modelParams":{"modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.analytics.screener"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.GenericSessionSummarizerV2 /mnt/data/analytics/models/batch-models-1.0.jar --config "$aser_config" > "batch-sess-summary.log" 2>&1&

## Aser screen summaries
aser_config='{"search":{"type":"s3","queries":[{"bucket":"sandbox-ekstep-telemetry","prefix":"sandbox.telemetry.unique-","startDate":"2016-01-01","endDate":"2016-01-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0","modelId":"AserScreenSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.analytics.screener"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.AserScreenSummarizer /mnt/data/analytics/models/batch-models-1.0.jar --config "$aser_config" > "batch-aser-screen.log" 2>&1&

## Learner Activity Summaries
la_config='{"search":{"type":"s3","queries":[{"bucket":"sandbox-session-summary","prefix":"sandbox.analytics.screener-","endDate":"2016-01-27","delta":0}]},"filters":[{"name":"eventId","operator":"EQ","value":"ME_SESSION_SUMMARY"}],"model":"org.ekstep.analytics.model.LearnerActivitySummary","modelParams":{"modelVersion":"1.0","modelId":"LearnerActivitySummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"172.31.1.92:9092","topic":"sandbox.analytics.screener"}}],"parallelization":8,"appName":"Learner Activity Summarizer","deviceMapping":false}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.LearnerActivitySummarizer /mnt/data/analytics/models/batch-models-1.0.jar --config "$la_config" > "batch-la-summ.log" 2>&1&

## Learner Snapshot Updater
la_config='{"search":{"type":"s3","queries":[{"bucket":"sandbox-session-summary","prefix":"sandbox.analytics.screener-","endDate":"2016-01-15","delta":0}]},"filters":[{"name":"eventId","operator":"EQ","value":"ME_LEARNER_ACTIVITY_SUMMARY"}],"model":"org.ekstep.analytics.updater.UpdateLearnerActivity","modelParams":{"modelVersion":"1.0","modelId":"LearnerSnapshotUpdater"},"output":[{"to":"console","params":{"printEvent": true}}],"parallelization":8,"appName":"Learner Snapshot Updater","deviceMapping":false}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.LearnerSnapshotUpdater /mnt/data/analytics/models/batch-models-1.0.jar --config "$la_config" > "batch-ls-updater.log" 2>&1&
```

## Migration scripts in production

```sh

## Generic Session Summarizer

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2015-12-24","startDate":"2015-10-27"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]}],"model":"org.ekstep.analytics.model.GenericSessionSummaryV2","modelParams":{"modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","endDate":"2016-01-26","startDate":"2015-12-25"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]}],"model":"org.ekstep.analytics.model.GenericSessionSummaryV2","modelParams":{"modelVersion":"1.0","modelId":"GenericSessionSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Generic Session Summarizer","deviceMapping":true}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.GenericSessionSummarizerV2 /mnt/data/analytics/models/batch-models-1.0.jar --config "$aser_config" > "batch-sess-summary.log" 2>&1&

## Aser Screen Summarizer

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","startDate":"2015-10-27","endDate":"2015-12-24"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0","modelId":"AserScreenSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'

aser_config='{"search":{"type":"s3","queries":[{"prefix":"prod.telemetry.unique-","startDate":"2015-12-25","endDate":"2016-01-26"}]},"filters":[{"name":"eventId","operator":"IN","value":["OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"]},{"name":"gameId","operator":"EQ","value":"org.ekstep.aser.lite"}],"model":"org.ekstep.analytics.model.AserScreenSummary","modelParams":{"modelVersion":"1.0","modelId":"AserScreenSummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Aser Screen Summarizer","deviceMapping":false}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.AserScreenSummarizer /mnt/data/analytics/models/batch-models-1.0.jar --config "$aser_config" > "batch-aser-screen.log" 2>&1&

## Learner Activity Summarizer

la_config='{"search":{"type":"s3","queries":[{"bucket":"ekstep-session-summary","prefix":"prod.analytics.screener-","endDate":"2016-01-27","delta":0}]},"filters":[{"name":"eventId","operator":"EQ","value":"ME_SESSION_SUMMARY"}],"model":"org.ekstep.analytics.model.LearnerActivitySummary","modelParams":{"modelVersion":"1.0","modelId":"LearnerActivitySummarizer"},"output":[{"to":"console","params":{"printEvent": false}},{"to":"kafka","params":{"brokerList":"10.10.1.171:9092","topic":"prod.analytics.screener"}}],"parallelization":8,"appName":"Learner Activity Summarizer","deviceMapping":false}'

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars /mnt/data/analytics/models/analytics-framework-0.5.jar --class org.ekstep.analytics.job.LearnerActivitySummarizer /mnt/data/analytics/models/batch-models-1.0.jar --config "$la_config" > "batch-la-summ.log" 2>&1&
```
