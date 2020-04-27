package org.ekstep.analytics.model

import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid._
import ing.wbaa.druid.client.DruidClient
import io.circe.Json
import io.circe.parser._
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.exception.DruidConfigException
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers}

import scala.concurrent.Future

class TestDruidQueryProcessingModel extends SparkSpec(null) with Matchers with BeforeAndAfterAll with MockFactory {

    implicit val fc = mock[FrameworkContext];

    it should "execute multiple queries and generate csv reports on multiple dimensions with dynamic interval" in {
        //        implicit val sc = CommonUtil.getSparkContext(2, "TestDruidQueryProcessingModel", None, None);
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global

        val json: String = """
          {
              "total_scans" : 9007,
              "producer_id" : "dev.sunbird.learning.platform",
              "state" : null
          }
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC), doc));
        val druidResponse = DruidResponse.apply(results, QueryType.GroupBy)

        implicit val mockDruidConfig = DruidConfig.DefaultConfig
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("consumption_usage_metrics", "groupBy", QueryDateRange(Option(QueryInterval("2020-01-01", "2020-01-07")), None, Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"), List("id", "dims", "date"))), Option(MergeConfig("DAY", "", 1, Option("ACADEMIC_YEAR"), Option("Date"), Option(1), "daily_metrics.csv")))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "store" -> "local", "container" -> "test-container", "filePath" -> "src/test/resources/druid-reports/")
        the[Exception] thrownBy {
            DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
        } should have message "Merge report script failed with exit code 127"
    }

    it should "execute multiple queries and generate csv reports on single dimension" in {
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global
        implicit val mockDruidConfig = DruidConfig.DefaultConfig
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future.apply[DruidResponse](DruidResponse.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("consumption_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute multiple queries and generate single json report" in {
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global
        implicit val mockDruidConfig = DruidConfig.DefaultConfig
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future.apply[DruidResponse](DruidResponse.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "throw exception if query has different dimensions" in {
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global
        implicit val mockDruidConfig = DruidConfig.DefaultConfig
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future.apply[DruidResponse](DruidResponse.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_city", None), DruidDimension("context_pdata_id", None))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), None, Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json", "filePath" -> "src/test/resources/")

        the[DruidConfigException] thrownBy {
            DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
        }
    }

    it should "throw exception if query does not have intervals" in {

        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, None, Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json", "filePath" -> "src/test/resources/")

        the[DruidConfigException] thrownBy {
            DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
        }
    }

    it should "execute report and generate multiple csv reports" in {
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global
        implicit val mockDruidConfig = DruidConfig.DefaultConfig

        val json: String = """
          {
              "total_scans" : 9007,
              "total_sessions" : 100,
              "producer_id" : "dev.sunbird.learning.platform",
              "device_loc_state" : "Karnataka"
          }
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC), doc));
        val druidResponse = DruidResponse.apply(results, QueryType.GroupBy)


        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

        val scansQuery1 = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", None), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery1 = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", None), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig1 = ReportConfig("data_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery1), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery1)), Map("device_loc_state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", Option("scans"), List("total_scans"), List("device_loc_state", "producer_id"), List("id", "dims", "date")), OutputConfig("csv", Option("sessions"), List("total_sessions", "total_ts"), List("device_loc_state", "producer_id"), List("id", "dims", "date"))))
        val strConfig1 = JSONUtils.serialize(reportConfig1)

        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig1), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute weekly report and generate csv reports" in {
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global
        implicit val mockDruidConfig = DruidConfig.DefaultConfig
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future.apply[DruidResponse](DruidResponse.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

        val scansQuery2 = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery2 = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig2 = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastWeek"), None), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery2), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery2)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig2 = JSONUtils.serialize(reportConfig2)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute weekly report for successful QR Scans and generate csv reports" in {
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global
        implicit val mockDruidConfig = DruidConfig.DefaultConfig
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future.apply[DruidResponse](DruidResponse.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

        //        val totalQRscansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")))))
        val totalSuccessfulQRScansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_successful_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")), DruidFilter("greaterthan", "edata_size", Option(0.asInstanceOf[AnyRef])))))
        val totalFailedQRScansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_failed_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")), DruidFilter("equals", "edata_size", Option("0".asInstanceOf[AnyRef])))))

        val totalPercentFailedQRScansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_failed_scans"),"javascript","edata_size",Option("function(current, edata_size) { return current + (edata_size == 0 ? 1 : 0); }"),Option("function(partialA, partialB) { return partialA + partialB; }"),Option("function () { return 0; }")), Aggregation(Option("total_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")))), None, Option(List(PostAggregation("javascript", "total_percent_failed_scans", PostAggregationFields("total_failed_scans", "total_scans"), "function(total_failed_scans, total_scans) { return (total_failed_scans/total_scans) * 100}"))))

        val totalcontentDownloadQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_content_download"), "count", ""))), None, Option(List(DruidFilter("equals", "edata_subtype", Option("ContentDownload-Success")),DruidFilter("equals", "eid", Option("INTERACT")), DruidFilter("equals", "context_pdata_id", Option("prod.sunbird.app")))))
        val contentPlayedQuery = DruidQueryModel("timeseries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_content_plays"), "count", ""))), None, Option(List(DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")), DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("equals", "dimensions_type", Option("content")), DruidFilter("equals", "dimensions_mode", Option("play")))))
        val uniqueDevicesQuery = DruidQueryModel("timeseries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_unique_devices"), "cardinality", "dimensions_did"))), None, Option(List(DruidFilter("equals", "dimensions_pdata_id", Option("prod.sunbird.app")),DruidFilter("equals", "dimensions_type", Option("app")))))
        val contentPlayedInHoursQuery = DruidQueryModel("timeseries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("sum__edata_time_spent"), "doubleSum", "edata_time_spent"))), None, Option(List(DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")), DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("equals", "dimensions_type", Option("content")), DruidFilter("equals", "dimensions_mode", Option("play")))), None, Option(List(PostAggregation("arithmetic", "total_time_spent_in_hours", PostAggregationFields("sum__edata_time_spent", 3600.asInstanceOf[AnyRef], "constant"), "/" ))))
        val reportConfig2 = ReportConfig("data_metrics", "timeseries",
            QueryDateRange(None, Option("LastDay"), Option("day")),
            List(
                //                Metrics("totalQRScans", "Total Scans", totalQRscansQuery),
                Metrics("totalSuccessfulScans", "Total Successful QR Scans", totalSuccessfulQRScansQuery),
                Metrics("totalFailedScans", "Total Failed QR Scans", totalFailedQRScansQuery),
                //                Metrics("totalPercentFailedScans", "Total Percent Failed QR Scans", totalPercentFailedQRScansQuery),
                Metrics("totalContentDownload", "Total Content Download", totalcontentDownloadQuery),
                Metrics("totalContentPlayed","Total Content Played", contentPlayedQuery),
                Metrics("totalUniqueDevices","Total Unique Devices", uniqueDevicesQuery),
                Metrics("totalContentPlayedInHour","Total Content Played In Hour", contentPlayedInHoursQuery)),
            Map(
                "total_scans" -> "Number of QR Scans",
                "total_successful_scans" -> "Number of successful QR Scans",
                "total_failed_scans" -> "Number of failed QR Scans", "total_content_download" -> "Total Content Downloads",
                "total_percent_failed_scans" -> "Total Percent Failed Scans",
                "total_content_plays" -> "Total Content Played",
                "total_unique_devices" -> "Total Unique Devices",
                "total_time_spent_in_hours" -> "Total time spent in hours"),
            List(OutputConfig("csv", Option("scans"),
                List("total_scans", "total_successful_scans", "total_failed_scans",
                    "total_percent_failed_scans",
                    "total_content_download", "total_content_plays",
                    "total_unique_devices",
                    "total_time_spent_in_hours"
                ))))
        val strConfig2 = JSONUtils.serialize(reportConfig2)
        //        val reportConfig = """{"id":"data_metrics","queryType":"groupBy","dateRange":{"staticInterval":"LastDay","granularity":"day"},"metrics":[{"metric":"totalQrScans","label":"Total QR Scans","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastDay","aggregations":[{"name":"total_scans","type":"count"}],"dimensions":[["device_loc_state","state"],["context_pdata_id","producer_id"]],"filters":[{"type":"isnotnull","dimension":"edata_filters_dialcodes"},{"type":"equals","dimension":"eid","value":"SEARCH"}],"descending":"false"}},{"metric":"totalSuccessfulScans","label":"Total Successful QR Scans","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastDay","aggregations":[{"name":"total_successful_scans","type":"count"}],"dimensions":[["device_loc_state","state"],["context_pdata_id","producer_id"]],"filters":[{"type":"isnotnull","dimension":"edata_filters_dialcodes"},{"type":"greaterThan","dimension":"edata_size","value":0},{"type":"equals","dimension":"eid","value":"SEARCH"}],"descending":"false"}},{"metric":"totalfailedQRScans","label":"Total Failed QR Scans","druidQuery":{"queryType":"groupBy","dataSource":"telemetry-events","intervals":"LastDay","aggregations":[{"name":"total_failed_scans","type":"count"}],"dimensions":[["device_loc_state","state"],["context_pdata_id","producer_id"]],"filters":[{"type":"isnotnull","dimension":"edata_filters_dialcodes"},{"type":"equals","dimension":"edata_size","value":0},{"type":"equals","dimension":"eid","value":"SEARCH"}],"descending":"false"}}],"labels":{"state":"State","total_sessions":"Number of Content Plays","producer_id":"Producer","total_scans":"Total Number of QR Scans","total_successful_scans":"Total Number Of Successful QR Scans","total_failed_scans":"Total Number Of Failed QR Scans"},"output":[{"type":"csv","label":"QR Scans","metrics":["total_scans","total_successful_scans","total_failed_scans"],"dims":[],"fileParameters":["id","date"]}]}"""
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute desktop metrics" in {
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global
        implicit val mockDruidConfig = DruidConfig.DefaultConfig
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future.apply[DruidResponse](DruidResponse.apply(List(), QueryType.GroupBy))).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

        val totalContentDownloadDesktopQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay",Option("all"),
            Option(List(Aggregation(Option("total_content_download_on_desktop"), "count", "mid"))),
            Option(List(DruidDimension("content_board", Option("state")))),
            Option(List(
                DruidFilter("equals", "context_env", Option("downloadManager")),
                DruidFilter("equals", "edata_state", Option("COMPLETED")),
                DruidFilter("equals", "context_pdata_id", Option("prod.sunbird.desktop")),
                DruidFilter("equals", "eid", Option("AUDIT"))
            ))
        )

        val totalContentPlayedDesktopQuery = DruidQueryModel("groupBy", "summary-events", "LastDay",Option("all"),
            Option(List(Aggregation(Option("total_content_plays_on_desktop"), "count", "mid"))),
            Option(List(DruidDimension("collection_board", Option("state")))),
            Option(List(
                DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")),
                DruidFilter("equals", "dimensions_mode", Option("play")),
                DruidFilter("equals", "dimensions_type", Option("content")),
                DruidFilter("equals", "dimensions_pdata_id", Option("prod.sunbird.desktop"))
            ))
        )

        val totalContentPlayedInHourOnDesktopQuery = DruidQueryModel("groupBy", "summary-events", "LastDay",Option("all"),
            Option(List(Aggregation(Option("sum__edata_time_spent"), "doubleSum", "edata_time_spent"))),
            Option(List(DruidDimension("collection_board", Option("state")))),
            Option(List(
                DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")),
                DruidFilter("equals", "dimensions_mode", Option("play")),
                DruidFilter("equals", "dimensions_type", Option("content")),
                DruidFilter("equals", "dimensions_pdata_id", Option("prod.sunbird.desktop"))
            )), None,
            Option(List(
                PostAggregation("arithmetic", "total_time_spent_in_hours_on_desktop",
                    PostAggregationFields("sum__edata_time_spent", 3600.asInstanceOf[AnyRef], "constant"), "/"
                )
            ))
        )

        val totalUniqueDevicesPlayedContentOnDesktopQuery = DruidQueryModel("groupBy", "summary-events", "LastDay",
            Option("all"),
            Option(List(Aggregation(Option("total_unique_devices_on_desktop_played_content"), "cardinality", "dimensions_did"))),
            Option(List(DruidDimension("collection_board", Option("state")))),
            Option(List(
                DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")),
                DruidFilter("equals", "dimensions_mode", Option("play")),
                DruidFilter("equals", "dimensions_type", Option("content")),
                DruidFilter("equals", "dimensions_pdata_id", Option("prod.sunbird.desktop"))
            ))
        )

        val reportConfig1 = ReportConfig("Desktop-Consumption-Daily-Reports", "groupBy",
            QueryDateRange(None, Option("LastDay"), Option("day")),
            List(
                Metrics("totalContentDownloadDesktop", "Total Content Download", totalContentDownloadDesktopQuery),
                Metrics("totalContentPlayedDesktop", "Total time spent in hours", totalContentPlayedDesktopQuery),
                Metrics("totalContentPlayedInHourOnDesktop", "Total Content Download", totalContentPlayedInHourOnDesktopQuery),
                Metrics("totalUniqueDevicesPlayedContentOnDesktop", "Total Unique Devices On Desktop that played content", totalUniqueDevicesPlayedContentOnDesktopQuery)
            ),
            Map(
                "state" -> "State",
                "total_content_download_on_desktop" -> "Total Content Downloads",
                "total_content_plays_on_desktop" -> "Total Content Played",
                "total_time_spent_in_hours_on_desktop" -> "Total time spent in hours",
                "total_unique_devices_on_desktop_played_content" -> "Total Unique Devices On Desktop that played content"
            ),
            List(
                OutputConfig("csv", Option("desktop"),
                    List(
                        "total_content_download_on_desktop",
                        "total_time_spent_in_hours_on_desktop",
                        "total_content_plays_on_desktop",
                        "total_unique_devices_on_desktop_played_content"
                    ),
                    List("state"),
                    List("dims")
                )
            )
        )
        val strConfig1 = JSONUtils.serialize(reportConfig1)

        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig1), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute weekly report without dimension and generate csv reports" in {
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global
        implicit val mockDruidConfig = DruidConfig.DefaultConfig

        val json: String = """
          {
              "total_scans" : 9007,
              "total_sessions" : 100,
              "total_ts" : 120.0
          }
        """
        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC), doc));
        val druidResponse = DruidResponse.apply(results, QueryType.Timeseries)

        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

        val scansQuery2 = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), None, Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery2 = DruidQueryModel("timeSeries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), None, Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.sunbird.app", "prod.sunbird.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig2 = ReportConfig("", "timeSeries", QueryDateRange(None, Option("LastWeek"), None), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery2), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery2)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"))))
        val strConfig2 = JSONUtils.serialize(reportConfig2)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    it should "execute the report with single dims" in {
        implicit val sqlContext = new SQLContext(sc)
        import scala.concurrent.ExecutionContext.Implicits.global

        val json = """{"district":"do_312998713531351040138","state":"The Constitution Quiz - English","total_sessions":99.0,"date":"2020-04-14"}"""

        val doc: Json = parse(json).getOrElse(Json.Null);
        val results = List(DruidResult.apply(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC), doc));
        val druidResponse = DruidResponse.apply(results, QueryType.GroupBy)

        implicit val mockDruidConfig = DruidConfig.DefaultConfig
        val mockDruidClient = mock[DruidClient]
        (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes();
        (fc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes();

        val DQEPlayByLanguage = DruidQueryModel("groupBy", "summary-rollup-syncts","2020-04-14/2020-05-13",Option("day"),Option(List(Aggregation(Option("total_sessions"),"longSum", "total_count"))), Option(List(DruidDimension("content_name", Option("state")), DruidDimension("object_id", Option("district")))), Option(List(DruidFilter("equals","dimensions_mode", Option("play")), DruidFilter("equals", "dimensions_type", Option("content")), DruidFilter("in", "object_id", Option("do_312998713531351040138")))))
        val reportConfig = ReportConfig("daily_quiz_play_by_lang","groupBy",QueryDateRange(Option(QueryInterval("2020-04-14", "2020-05-13")), None, Option("day")),
            List(Metrics("DailyEnglishQuizPlayByLang", "Daily English Quiz Play By Languagte", DQEPlayByLanguage)),
            Map("total_sessions" -> "Total Count English", "state" -> "Content Name", "district" -> "Object ID", "date" -> "Date", "total_content_plays" -> "Total Count Hindi"),
            List(OutputConfig("csv",None,List("total_sessions"),List("date"))),
            Option(MergeConfig("DAY", "/Users/utkarshakapoor/Documents/workspace-stackroutelabs/Data-Analytics/reports",1,Option("MONTH"),Option("Date"),Option(1),"daily_quiz_play_by_lang.csv"))
        )

        val reportStr = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](reportStr), "store" -> "local", "container" -> "test-container", "filePath" -> "druid-reports/")

        the[Exception] thrownBy {
            DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
        } should have message "Merge report script failed with exit code 127"
    }

    it should "test for setStorageConf method" in {
        DruidQueryProcessingModel.setStorageConf("s3", None, None)
        DruidQueryProcessingModel.setStorageConf("azure", None, None)
    }
}
