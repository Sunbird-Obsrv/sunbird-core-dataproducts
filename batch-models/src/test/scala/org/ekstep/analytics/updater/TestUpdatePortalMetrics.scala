package org.ekstep.analytics.updater

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.{WorkFlowUsageSummaryFact, _}
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.framework.FrameworkContext


/**
  * @author Manjunath Davanam <manjunathd@ilimi.in>
  */

class TestUpdatePortalMetrics extends SparkSpec(null) with MockFactory {

  override def beforeAll(){
    super.beforeAll()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createDeviceProfileTable()
  }

  /**
    * Truncate the data from the database before run the testcase
    */
  private def cleanDataBase(): Unit ={
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
    }
  }

  /**
    * Invoke this method to save the data into cassandra database
    *
    */
  private def saveToDB(data:Array[WorkFlowUsageSummaryFact]): Unit ={
    sc.parallelize(data).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT)
  }

  /**
    * Which is used to execute updateDashboard data product
    */
  private def executeDataProduct()(implicit fc: FrameworkContext):RDD[PortalMetrics]={
    UpdatePortalMetrics.execute(sc.emptyRDD, Option(Map("date" -> new DateTime().toString(CommonUtil.dateFormat).asInstanceOf[AnyRef])))
  }

  "UpdateDashboardModel" should "Should find the unique device count,totalContentPublished,totalTimeSpent,totalContentPlayTime and should filter when d_time>0(Cumulative)" in {
    
    implicit val fc = new FrameworkContext();
    val rdd = executeDataProduct()
    val out = rdd.collect()
    val dashboardSummary = JSONUtils.deserialize[WorkFlowUsageMetrics](JSONUtils.serialize(out.head.metrics_summary))
    dashboardSummary.totalContentPublished.language_publisher_breakdown.length should be(0)
    dashboardSummary.noOfUniqueDevices should be(0)
    dashboardSummary.totalTimeSpent should be(0.0)
    dashboardSummary.totalContentPlaySessions should be(0)
  }

  override def afterAll(): Unit ={
    super.afterAll()
    EmbeddedPostgresql.close()
  }
}