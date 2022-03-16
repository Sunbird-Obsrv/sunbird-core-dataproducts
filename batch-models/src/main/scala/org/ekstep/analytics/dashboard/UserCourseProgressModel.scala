package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.{AlgoInput, AlgoOutput, FrameworkContext, IBatchModelTemplate, Output}

// IMPORTANT: this unused import initializes JSONUtils and allows serialization to work
import org.ekstep.analytics.framework.util.JSONUtils

case class UserCourseInput(timestamp: Long) extends AlgoInput
@scala.beans.BeanInfo
case class UserCourseOutput(userID: Any, courseID: Any, percentage: Any, timestamp: Long) extends Output with AlgoOutput

object UserCourseProgressModel extends IBatchModelTemplate[String, UserCourseInput, UserCourseOutput, UserCourseOutput]{

  implicit val className: String = "org.ekstep.analytics.dashboard.UserCourseProgressModel"
  override def name(): String = "UserCourseProgressModel"

  /**
   * Pre processing steps before running the algorithm. Few pre-process steps are
   * 1. Transforming input - Filter/Map etc.
   * 2. Join/fetch data from LP
   * 3. Join/Fetch data from Cassandra
   */
  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[UserCourseInput] = {
    val timestamp = System.currentTimeMillis()
    sc.parallelize(Seq(UserCourseInput(timestamp)))
  }

  /**
   * Method which runs the actual algorithm
   */
  override def algorithm(events: RDD[UserCourseInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[UserCourseOutput] = {
    val executionTime = events.first().timestamp
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    userCourseDataRDD(executionTime)
  }

  /**
   * Post processing on the algorithm output. Some of the post processing steps are
   * 1. Saving data to Cassandra
   * 2. Converting to "MeasuredEvent" to be able to dispatch to Kafka or any output dispatcher
   * 3. Transform into a structure that can be input to another data product
   */
  override def postProcess(events: RDD[UserCourseOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[UserCourseOutput] = {
    events
  }

  def userCourseDataRDD(timestamp: Long)(implicit spark: SparkSession): RDD[UserCourseOutput] = {
    val userCourseData = spark.read.format("org.apache.spark.sql.cassandra")
      .option("inferSchema", "true")
      .option("keyspace", "sunbird_courses")
      .option("table", "user_content_consumption")
      .load().persist(StorageLevel.MEMORY_ONLY);
    val progressData = userCourseData.select("userid", "courseid", "completionpercentage");
    val outputData: RDD[UserCourseOutput] = progressData.rdd.map{f => UserCourseOutput(f(0), f(1), f(2), timestamp)}
    outputData.distinct()
  }
}