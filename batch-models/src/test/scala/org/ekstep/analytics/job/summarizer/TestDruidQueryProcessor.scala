package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model._
import org.ekstep.analytics.model.SparkSpec

class TestDruidQueryProcessor extends SparkSpec(null) {

    ignore should "execute DruidQueryProcessor job and won't throw any Exception" in {

        //val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation("count", "count", None),Aggregation("total_ts", "doubleSum", Option("edata_time_spent")))), Option(List("dimensions_channel", "dimensions_sid", "dimensions_pdata_id", "dimensions_type", "dimensions_mode", "dimensions_did", "object_id", "content_board")), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("count"), "count", "count"),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("dimensions_pdata_id", Option("producer_id")), DruidDimension("dimensions_type", Option("summary_type")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val modelParams = Map("bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/", "dims" -> List("dimensions_pdata_id"))
        val config = JobConfig(Fetcher("druid", None, None, Option(contentPlaysQuery)), null, null, "org.ekstep.analytics.model.DruidQueryProcessingModel", Option(modelParams), Option(Array(Dispatcher("console", Map("printEvent" -> true.asInstanceOf[AnyRef])))), Option(10), Option("TestDruidQueryProcessor"), Option(true))
        DruidQueryProcessor.main(JSONUtils.serialize(config))(Option(sc));
    }

    "DruidQueryProcessor" should "execute multiple queries and generate csv reports on multiple dimensions" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", "count"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", "count"),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val modelParams = Map("reportConfig" -> reportConfig, "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.DruidQueryProcessingModel", Option(modelParams), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDruidQueryProcessor"), Option(true))
        DruidQueryProcessor.main(JSONUtils.serialize(config))(Option(sc));
    }
}
