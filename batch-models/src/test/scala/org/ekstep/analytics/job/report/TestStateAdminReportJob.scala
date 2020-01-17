package org.ekstep.analytics.job

import java.io.File

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.job.report.{BaseReportSpec, BaseReportsJob, ShadowUserData, StateAdminReportJob}
import org.ekstep.analytics.util.EmbeddedCassandra
import org.scalamock.scalatest.MockFactory
import org.sunbird.cloud.storage.conf.AppConf

class TestStateAdminReportJob extends BaseReportSpec with MockFactory {

  implicit var spark: SparkSession = _
  var map: Map[String, String] = _
  var shadowUserDF: DataFrame = _
  var orgDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdKeyspace = "sunbird"
  val shadowUserEncoder = Encoders.product[ShadowUserData].schema

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql") // Load test data in embedded cassandra server
  }


  "StateAdminReportJob" should "generate reports" in {
    implicit val fc = new FrameworkContext()
    val tempDir = AppConf.getConfig("admin.metrics.temp.dir")
    val reportDF = StateAdminReportJob.generateReport()(spark, fc)
    assert(reportDF.columns.contains("index") === true)
    assert(reportDF.columns.contains("registered") === true)
    assert(reportDF.columns.contains("blocks") === true)
    assert(reportDF.columns.contains("schools") === true)
    assert(reportDF.columns.contains("districtName") === true)
    assert(reportDF.columns.contains("slug") === true)
    val apslug = reportDF.where(col("slug") === "ApSlug")
    val districtName = apslug.select("districtName").collect().map(_ (0)).toList
    assert(districtName(0) === "GULBARGA")
    //checking reports were created under slug folder
    val slugName = apslug.select("slug").collect().map(_ (0)).toList
    val apslugDirPath = tempDir+"/renamed/"+slugName(0)+"/"
    val userDetail = new File(apslugDirPath+"user-detail.csv")
    val userSummary = new File(apslugDirPath+"user-summary.json")
    val validateUserDetail = new File(apslugDirPath+"validated-user-detail.csv")
    val validateUserSummary = new File(apslugDirPath+"validated-user-summary.json")
    assert(userDetail.exists() === true)
    assert(userSummary.exists() === true)
    assert(validateUserDetail.exists() === true)
    assert(validateUserSummary.exists() === true)
  }

}