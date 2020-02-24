package org.ekstep.analytics.job

import java.io.File

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.job.report.{BaseReportSpec, BaseReportsJob, ShadowUserData, StateAdminReportJob}
import org.ekstep.analytics.util.EmbeddedCassandra
import org.scalamock.scalatest.MockFactory
import org.sunbird.cloud.storage.conf.AppConf
import org.ekstep.analytics.framework.util.HadoopFileUtil

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
  
  override def afterAll() : Unit = {
    super.afterAll();
    (new HadoopFileUtil()).delete(spark.sparkContext.hadoopConfiguration, "src/test/resources/admin-user-reports")
  }

  //Created data : channels ApSlug and OtherSlug contains validated users created against blocks,districts and state
  //Only TnSlug doesn't contain any validated users
  "StateAdminReportJob" should "generate reports" in {
    implicit val fc = new FrameworkContext()
    val tempDir = AppConf.getConfig("admin.metrics.temp.dir")
    val reportDF = StateAdminReportJob.generateReport()(spark, fc)
    assert(reportDF.columns.contains("registered") === true)
    assert(reportDF.columns.contains("blocks") === true)
    assert(reportDF.columns.contains("schools") === true)
    assert(reportDF.columns.contains("districtName") === true)
    assert(reportDF.columns.contains("slug") === true)
    val apslug = reportDF.where(col("slug") === "ApSlug")
    val districtName = apslug.select("districtName").collect().map(_ (0)).toList
    assert(districtName(0) === "GULBARGA")
    //checking reports were created under slug folder
    val userDetail = new File("src/test/resources/admin-user-reports/user-detail/ApSlug.csv")
    val stateUserDetail = new File("src/test/resources/admin-user-reports/validated-user-detail-state/ApSlug.csv");
    val userSummary = new File("src/test/resources/admin-user-reports/user-summary/ApSlug.json")
    val validateUserDetail = new File("src/test/resources/admin-user-reports/validated-user-detail/ApSlug.csv")
    val validateUserSummary = new File("src/test/resources/admin-user-reports/validated-user-summary/ApSlug.json")
    val validateUserDstSummary = new File("src/test/resources/admin-user-reports/validated-user-summary-district/ApSlug.json");
    assert(userDetail.exists() === true)
    assert(userSummary.exists() === true)
    assert(validateUserDetail.exists() === true)
    assert(validateUserSummary.exists() === true)
    assert(validateUserDstSummary.exists())
    assert(stateUserDetail.exists())
  }

}