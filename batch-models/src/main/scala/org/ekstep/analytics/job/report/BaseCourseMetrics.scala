package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.CourseUtils

case class BaseCourseMetricsOutput(courseName: String, batchName: String, status: String, slug: String, courseId: String, batchId: String) extends AlgoInput

trait BaseCourseMetrics[T <: AnyRef, A <: BaseCourseMetricsOutput, B <: AlgoOutput, R <: Output] extends IBatchModelTemplate[T,BaseCourseMetricsOutput,B,R]{

  override def preProcess(events: RDD[T], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[BaseCourseMetricsOutput] = {
    CommonUtil.setStorageConf(config.getOrElse("store", "local").toString, config.get("accountKey").asInstanceOf[Option[String]], config.get("accountSecret").asInstanceOf[Option[String]])
    val data = getCourseMetrics(config)
    val encoder = Encoders.product[BaseCourseMetricsOutput]
    data.as[BaseCourseMetricsOutput](encoder).rdd
  }

  def getCourseMetrics(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): DataFrame = {
    implicit val sqlContext = new SQLContext(sc)
    val courses = CourseUtils.getCourse(config)
    val courseBatch = CourseUtils.getCourseBatchDetails()
    val tenantInfo = CourseUtils.getTenantInfo()
    val joinCourses = courses.join(courseBatch, courses.col("identifier") === courseBatch.col("courseId"), "inner")
    val joinWithTenant = joinCourses.join(tenantInfo, joinCourses.col("channel") === tenantInfo.col("id"), "inner")
    joinWithTenant.na.fill("unknown", Seq("slug")).select("courseName","batchName","status","slug", "courseId", "batchId")
  }
}