package org.ekstep.analytics.job.report

import org.ekstep.analytics.framework.{Dispatcher, DruidQueryModel, Fetcher, JobConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model._

class TestCourseEnrollmentJob extends SparkSpec(null){

  "CourseEnrollmentJob" should "execute and won't throw any errors/exception" in {
    val config = """{"reportConfig": {"id": "tpd_metrics","metrics": [],"labels": {"completionCount": "Completion Count","status": "Status","enrollmentCount": "Enrollment Count","courseName": "Course Name","batchName": "Batch Name"},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"esConfig": {"request": {"filters": {"objectType": ["Content"],"contentType": ["Course"],"identifier": [],"status": ["Live"]},"limit": 10000}},"key": "druid-reports/","filePath": "src/test/resources/","bucket": "test-container","folderPrefix": ["slug", "reportName"]}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config)

    val reportConfig = ReportConfig("tpd_metrics","",QueryDateRange(None,Option(""),Option("")),List(Metrics("","",DruidQueryModel("","","",Option("")))),Map("date" -> "Date","completionCount"->"Completion Count","status"->"Status","enrollmentCount"->"Enrollment Count","courseName"->"Course Name","batchName"->"Batch Name"),List(OutputConfig("csv",None,List("identifier", "channel", "name"),List("id", "dims"))))
    val esConfig = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Course"), "identifier" -> List(), "status" -> List("Live"))))
    val modelParams = Map("esConfig" -> esConfig, "reportConfig" -> reportConfig, "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/", "courseStatus" -> List("Live"), "courseIds" -> List("do_1126981011606323201176", "do_112470675618004992181"))
    val con = JobConfig(Fetcher("none",None,None,None),None,None,"org.ekstep.analytics.model.TPDEnrollmentMetricsModel",Option(modelParams),Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),Option(8),Option("TPD Course Consumption Metrics Model"),Option(false),None,None)

    CourseEnrollmentJob.main(JSONUtils.serialize(con))(Option(sc))
  }
}