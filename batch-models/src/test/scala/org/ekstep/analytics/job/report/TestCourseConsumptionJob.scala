package org.ekstep.analytics.job.report

import org.ekstep.analytics.framework.{Aggregation, Dispatcher, DruidDimension, DruidFilter, DruidQueryModel, Fetcher, JobConfig, PostAggregation, PostAggregationFields}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.{Metrics, OutputConfig, QueryDateRange, ReportConfig, SparkSpec}

class TestCourseConsumptionJob extends SparkSpec(null){

  "CourseConsumptionJob" should "execute and won't throw any errors/exception" in {
    val reportConfig = ReportConfig("tpd_metrics","groupBy",QueryDateRange(None,Option("2019-09-08T00:00:00+00:00/2019-09-09T00:00:00+00:00"),Option("all")),List(Metrics("totalCoursePlays","Total Course Plays (in mins)",DruidQueryModel("groupBy","summary-events","2019-09-08/2019-09-09",None,Option(List(Aggregation(Option("sum__edata_time_spent"),"doubleSum","edata_time_spent",None,None,None))),Option(List(DruidDimension("object_rollup_l1",Option("courseId")), DruidDimension("uid",Option("userId")), DruidDimension("context_cdata_id",Option("batchId")))),Option(List(DruidFilter("equals","eid",Option("ME_WORKFLOW_SUMMARY"),None), DruidFilter("in","dimensions_pdata_id",None,Option(List("dev.sunbird.app", "dev.sunbird.portal"))), DruidFilter("equals","dimensions_type",Option("content"),None), DruidFilter("equals","dimensions_mode",Option("play"),None), DruidFilter("equals","context_cdata_type",Option("batch"),None))),None,Option(List(PostAggregation("arithmetic","timespent",PostAggregationFields("sum__edata_time_spent",60.asInstanceOf[AnyRef],"constant"),"/",None)))))),Map("date" -> "Date", "status" -> "Batch Status", "timespent" -> "Timespent in mins", "courseName" -> "Course Name", "batchName" -> "Batch Name"),List(OutputConfig("csv",None,List("timespent"),List("identifier", "channel", "name"),List("id", "dims"))))
    val esConfig = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Course"), "identifier" -> List(), "status" -> List("Live"))))
    val modelParams = Map("esConfig" -> esConfig, "reportConfig" -> reportConfig, "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/", "courseStatus" -> List("Live"), "courseIds" -> List("do_1126981011606323201176", "do_112470675618004992181"))
    val con = JobConfig(Fetcher("none",None,None,None),None,None,"org.ekstep.analytics.model.TPDConsumptionMetricsModel",Option(modelParams),Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),Option(8),Option("TPD Course Consumption Metrics Model"),Option(false),None,None)

    CourseConsumptionJob.main(JSONUtils.serialize(con))(Option(sc))
  }
}