package org.ekstep.analytics.model


import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.Metrics.MetricsAuditJob
import org.scalamock.scalatest.MockFactory

  class TestMetricsAuditModel extends SparkSpec(null) with MockFactory{

     implicit val fc = new FrameworkContext()

    "TestMetricsAuditJob" should "get the metrics for monitoring the data pipeline" in {
      val auditConfig = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/denorm/2019-12-03-1575312964601.json\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.content_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.device_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.dialcode_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.collection_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.derived_location_retrieved\",\"operator\":\"EQ\",\"value\":true}]},{\"name\":\"raw\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/raw/raw.json\"}]}},{\"name\":\"failed\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/failed/2019-12-04-1575420457646.json\"}]}},{\"name\":\"channel-raw\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/channel-raw/2019-12-04-1575399627832.json\"}]}},{\"name\":\"channel-summary\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/channel-summary/2019-12-04-1575510009570.json\"}]}}]},\"output\":[{\"to\":\"file\",\"params\":{\"path\":\"src/test/resources/\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
      val config = JSONUtils.deserialize[JobConfig](auditConfig)
      MetricsAuditModel.execute(sc.emptyRDD, config.modelParams)
    }

    it should "execute MetricsAudit job and won't throw any Exception" in {
      val configString ="{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/denorm/2019-12-03-1575312964601.json\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.content_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.device_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.dialcode_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.collection_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.derived_location_retrieved\",\"operator\":\"EQ\",\"value\":true}]},{\"name\":\"raw\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/raw/raw.json\"}]}}]},\"output\":[{\"to\":\"file\",\"params\":{\"file\":\"src/test/resources/audit-metrics-result\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
      val config_1= JSONUtils.deserialize[JobConfig](configString);
      val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.ExperimentDefinitionModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestExperimentDefinitionJob"))
      MetricsAuditJob.main(configString)(Option(sc));
    }

    it should "load the local file and give the denorm count" in {
      val auditConfig = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/denorm/2019-12-03-1575312964601.json\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.content_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.device_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.dialcode_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.collection_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.derived_location_retrieved\",\"operator\":\"EQ\",\"value\":true}]}]},\"output\":[{\"to\":\"file\",\"params\":{\"file\":\"src/test/resources/audit-metrics-result\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
      val config = JSONUtils.deserialize[JobConfig](auditConfig)
      val reportConfig = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.MetricsAuditModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("MetricsAuditModel"))
      val metrics = MetricsAuditModel.execute(sc.emptyRDD, config.modelParams)
      metrics.collect().map{f =>
       val metricsEdata = f.edata.asInstanceOf[Map[String, AnyRef]].get("metrics").get.asInstanceOf[List[V3MetricEdata]]
        metricsEdata.map{edata =>
          if("count".equals(edata.metric)) edata.value should be (Some(312))
          if("user_data_retrieved".equals(edata.metric)) edata.value should be (Some(0))
          if("percentage_events_with_user_data_retrieved".equals(edata.metric)) edata.value should be (Some(0))
          if("derived_location_retrieved".equals(edata.metric)) edata.value should be (Some(1))
        }
      }
    }

    it should "load the local file and give the count for other files than failed/denorm" in {
      val auditConfig = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"raw\",\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/audit-metrics-report/raw/raw.json\"}]}}]},\"output\":[{\"to\":\"file\",\"params\":{\"file\":\"src/test/resources/audit-metrics-result\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
      val config = Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/audit-metrics-report/raw/raw.json")))))
      val testRDD = DataFetcher.fetchBatchData[String](config)
      testRDD.count() should be (57792)

      val metrics = MetricsAuditModel.getTotalSecorCountAudit(testRDD)
      metrics.filter(f => "inputEvents".equals(f.metric)).map(f => f.value should be (Some(57792)))
    }

    it should "load failed files from local and give the count for failed backup" in {
      val config = Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/audit-metrics-report/failed/2019-12-04-1575420457646.json")))))
      val testRDD = DataFetcher.fetchBatchData[V3ContextEvent](config)
      testRDD.count() should be (94)

      val metrics = MetricsAuditModel.getFailedSecorAudit(testRDD)
      metrics.map{f => if("AnalyticsAPI".equals(f.metric)) f.value should be (Some(94))}
    }

    ignore should "get the metrics for druid count for monitoring" in {
      val auditConfig = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"telemetry-count\",\"search\":{\"type\":\"druid\",\"druidQuery\":{\"queryType\":\"timeSeries\",\"dataSource\":\"telemetry-events\",\"intervals\":\"LastDay\",\"aggregations\":[{\"name\":\"total_count\",\"type\":\"count\",\"fieldName\":\"count\"}],\"descending\":\"false\"}}},{\"name\":\"summary-count\",\"search\":{\"type\":\"druid\",\"druidQuery\":{\"queryType\":\"timeSeries\",\"dataSource\":\"summary-events\",\"intervals\":\"LastDay\",\"aggregations\":[{\"name\":\"total_count\",\"type\":\"count\",\"fieldName\":\"count\"}],\"descending\":\"false\"}}}]},\"output\":[{\"to\":\"file\",\"params\":{\"file\":\"src/test/resources/audit-metrics-result\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
      val config_1 = Fetcher("druid",None,None,Some(DruidQueryModel("timeSeries","telemetry-events","LastDay",None,Some(List(Aggregation(Some("total_count"),"count","count",None,None,None))),None,None,None,None,None,None,Some("false"))))
      val config = JSONUtils.deserialize[JobConfig](auditConfig)
      val metrics = MetricsAuditModel.execute(sc.emptyRDD, config.modelParams)
    }
}
