package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.exception.DruidConfigException
import org.ekstep.analytics.framework.util.JSONUtils

class TestDruidQueryProcessingModel extends SparkSpec(null) {

    implicit val fc = new FrameworkContext();
    ignore should "execute multiple queries and generate csv reports on multiple dimensions with dynamic interval" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("consumption_usage_metrics", "groupBy", QueryDateRange(Option(QueryInterval("2019-08-01", "2019-08-05")), None, Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"), List("id", "dims", "date"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    ignore should "execute multiple queries and generate csv reports on single dimension" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("consumption_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    ignore should "execute multiple queries and generate single json report" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    ignore should "throw exception if query has different dimensions" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_city", Option("city")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json", "filePath" -> "src/test/resources/")

        the[DruidConfigException] thrownBy {
            DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
        }
    }

    ignore should "throw exception if query does not have intervals" in {
        val scansQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("day"), Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery = DruidQueryModel("groupBy", "summary-events", "LastDay", Option("all"), Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, None, Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("json", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig = JSONUtils.serialize(reportConfig)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig), "bucket" -> "test-container", "key" -> "druid-reports/usage_metrics.json", "filePath" -> "src/test/resources/")

        the[DruidConfigException] thrownBy {
            DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
        }
    }

    ignore should "execute report and generate multiple csv reports" in {
        val scansQuery1 = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery1 = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig1 = ReportConfig("data_metrics", "groupBy", QueryDateRange(None, Option("LastDay"), Option("day")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery1), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery1)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", Option("scans"), List("total_scans"), List("state", "producer_id"), List("id", "dims", "date")), OutputConfig("csv", Option("sessions"), List("total_sessions", "total_ts"), List("state", "producer_id"), List("id", "dims", "date"))))
        val strConfig1 = JSONUtils.serialize(reportConfig1)

        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig1), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    ignore should "execute weekly report and generate csv reports" in {
        val scansQuery2 = DruidQueryModel("groupBy", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("context_pdata_id", Option("producer_id")))), Option(List(DruidFilter("greaterThan", "edata_size", Option(0.asInstanceOf[AnyRef])),DruidFilter("equals", "eid", Option("SEARCH")))))
        val contentPlaysQuery2 = DruidQueryModel("groupBy", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_sessions"), "count", ""),Aggregation(Option("total_ts"), "doubleSum", "edata_time_spent"))), Option(List(DruidDimension("device_loc_state", Option("state")), DruidDimension("dimensions_pdata_id", Option("producer_id")))), Option(List(DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("in", "dimensions_type", None, Option(List("content", "app"))))))
        val reportConfig2 = ReportConfig("usage_metrics", "groupBy", QueryDateRange(None, Option("LastWeek"), Option("all")), List(Metrics("totalSuccessfulScans", "Total Scans", scansQuery2), Metrics("totalSessions/totalContentPlays", "Total ContentPlay Sessions", contentPlaysQuery2)), Map("state" -> "State", "producer_id" -> "Producer", "total_scans" -> "Number of Successful QR Scans", "total_sessions" -> "Number of Content Plays", "total_ts" -> "Content Play Time"), List(OutputConfig("csv", None, List("total_scans", "total_sessions", "total_ts"), List("state", "producer_id"))))
        val strConfig2 = JSONUtils.serialize(reportConfig2)
        val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
        DruidQueryProcessingModel.execute(sc.emptyRDD, Option(modelParams));
    }

    ignore should "execute weekly report for successful QR Scans and generate csv reports" in {
//        val totalQRscansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")))))
        val totalSuccessfulQRScansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_successful_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")), DruidFilter("greaterthan", "edata_size", Option(0.asInstanceOf[AnyRef])))))
        val totalFailedQRScansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_failed_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")), DruidFilter("equals", "edata_size", Option("0".asInstanceOf[AnyRef])))))

        val totalPercentFailedQRScansQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_failed_scans"),"javascript","edata_size",Option("function(current, edata_size) { return current + (edata_size == 0 ? 1 : 0); }"),Option("function(partialA, partialB) { return partialA + partialB; }"),Option("function () { return 0; }")), Aggregation(Option("total_scans"), "count", ""))), None, Option(List(DruidFilter("isnotnull", "edata_filters_dialcodes", None),DruidFilter("equals", "eid", Option("SEARCH")))), None, Option(List(PostAggregation("javascript", "total_percent_failed_scans", PostAggregationFields("total_failed_scans", "total_scans"), "function(total_failed_scans, total_scans) { return (total_failed_scans/total_scans) * 100}"))))

        val totalcontentDownloadQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastDay", None, Option(List(Aggregation(Option("total_content_download"), "count", ""))), None, Option(List(DruidFilter("equals", "edata_subtype", Option("ContentDownload-Success")),DruidFilter("equals", "eid", Option("INTERACT")), DruidFilter("equals", "context_pdata_id", Option("prod.diksha.app")))))
        val contentPlayedQuery = DruidQueryModel("timeseries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_content_plays"), "count", ""))), None, Option(List(DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")), DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("equals", "dimensions_type", Option("content")), DruidFilter("equals", "dimensions_mode", Option("play")))))
        val uniqueDevicesQuery = DruidQueryModel("timeseries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("total_unique_devices"), "cardinality", "dimensions_did"))), None, Option(List(DruidFilter("equals", "dimensions_pdata_id", Option("prod.diksha.app")),DruidFilter("equals", "dimensions_type", Option("app")))))
        val contentPlayedInHoursQuery = DruidQueryModel("timeseries", "summary-events", "LastDay", None, Option(List(Aggregation(Option("sum__edata_time_spent"), "doubleSum", "edata_time_spent"))), None, Option(List(DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")), DruidFilter("in", "dimensions_pdata_id", None, Option(List("prod.diksha.app", "prod.diksha.portal"))),DruidFilter("equals", "dimensions_type", Option("content")), DruidFilter("equals", "dimensions_mode", Option("play")))), None, Option(List(PostAggregation("arithmetic", "total_time_spent_in_hours", PostAggregationFields("sum__edata_time_spent", 3600.asInstanceOf[AnyRef], "constant"), "/" ))))
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

    ignore should "execute desktop metrics" in {
        val totalContentDownloadDesktopQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay",Option("all"),
            Option(List(Aggregation(Option("total_content_download_on_desktop"), "count", "mid"))),
            Option(List(DruidDimension("content_board", Option("state")))),
            Option(List(
                DruidFilter("equals", "context_env", Option("downloadManager")),
                DruidFilter("equals", "edata_state", Option("COMPLETED")),
                DruidFilter("equals", "context_pdata_id", Option("prod.diksha.desktop")),
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
                DruidFilter("equals", "dimensions_pdata_id", Option("prod.diksha.desktop"))
            ))
        )

        val totalContentPlayedInHourOnDesktopQuery = DruidQueryModel("groupBy", "summary-events", "LastDay",Option("all"),
            Option(List(Aggregation(Option("sum__edata_time_spent"), "doubleSum", "edata_time_spent"))),
            Option(List(DruidDimension("collection_board", Option("state")))),
            Option(List(
                DruidFilter("equals", "eid", Option("ME_WORKFLOW_SUMMARY")),
                DruidFilter("equals", "dimensions_mode", Option("play")),
                DruidFilter("equals", "dimensions_type", Option("content")),
                DruidFilter("equals", "dimensions_pdata_id", Option("prod.diksha.desktop"))
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
                DruidFilter("equals", "dimensions_pdata_id", Option("prod.diksha.desktop"))
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
}
