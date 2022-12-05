package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, lit}
import org.ekstep.analytics.framework.{AlgoInput, AlgoOutput, FrameworkContext, IBatchModelTemplate, Output}
import com.mongodb.spark.sql._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.rdd.RDD

import java.io.Serializable
import DashboardUtil._

/*

Prerequisites(PR) -

PR10: Nodebb API call

Metric  PR      Type                Description

OL.15   10      Scorecard           Total posts and comments made by the official on iGOT

*/
case class OPDummyInput(timestamp: Long) extends AlgoInput  // no input, there are multiple sources to query
case class OPDummyOutput() extends Output with AlgoOutput  // no output

case class OPConfig(debug: String, nodebbHost: String, nodebbPort: Int, nodebbDB: String,
                    redisHost: String, redisPort: Int, redisDB: Int, neo4jHost: String) extends Serializable


object OfficerPostsCountModel extends IBatchModelTemplate[String, OPDummyInput, OPDummyOutput, OPDummyOutput] with Serializable{

  implicit val className: String = "org.ekstep.analytics.dashboard.OfficerPostsCountModel"
  override def name() = "OfficerPostsCountModel"
  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[OPDummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(OPDummyInput(executionTime)))
  }

  override def algorithm(events: RDD[OPDummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[OPDummyOutput] = {
    val timestamp = events.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).config("spark.mongodb.input.sampleSize", 50000)
      .getOrCreate()
    processOfficerDashboardData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(events: RDD[OPDummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[OPDummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processOfficerDashboardData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: OPConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config

    val officerPosts = totalPostsDf()
    val profileViews = profileViewsDf()
    val upvotes = officerUpvotesDf()

  }

  def totalPostsDf()(implicit spark: SparkSession, conf: OPConfig): DataFrame = {
    val readConf = ReadConfig(readConfMap())
    var df = spark.read.mongo(readConf).select(
      col("sunbird-oidcId").alias("userId"),
      col("postcount").alias("postCount")
    )
    df = df.na.fill(0).filter(col("userId").isNotNull)
    show(df, "Officer posts")
    df
  }

  def profileViewsDf()(implicit spark: SparkSession, conf: OPConfig): DataFrame = {
    val readConf = ReadConfig(readConfMap())
    var df = spark.read.mongo(readConf).select(
      col("sunbird-oidcId").alias("userId"),
      col("profileviews").alias("profileViews")
    )
    df = df.na.fill(0).filter(col("userId").isNotNull)
    show(df, "Profile views")
    df
  }

  def officerUpvotesDf()(implicit spark: SparkSession, conf: OPConfig): DataFrame = {
    val readConf = ReadConfig(readConfMap())
    var df = spark.read.mongo(readConf)
    df = df.na.fill(0).filter(col("userId").isNotNull)
    show(df, "Officer Upvotes")
    df
  }

  def readConfMap()(implicit conf: OPConfig) = {
    val uri = s"mongodb://${conf.nodebbHost}:${conf.nodebbPort}"
    val configMap = Map(
      "uri" -> uri,
      "database" -> conf.nodebbDB,
      "collection" -> "objects")
    configMap
  }

  def parseConfig(config: Map[String, AnyRef]): OPConfig = {
    OPConfig(
      debug = getConfigModelParam(config, "debug"),
      redisHost = getConfigModelParam(config, "redisHost"),
      redisPort = getConfigModelParam(config, "redisPort").toInt,
      redisDB = getConfigModelParam(config, "redisDB").toInt,
      neo4jHost = getConfigModelParam(config, "neo4jHost"),
      nodebbPort = getConfigModelParam(config, "nodebbPort").toInt,
      nodebbDB = getConfigModelParam(config, "nodebbDB"),
      nodebbHost = getConfigModelParam(config, "nodebbHost")
    )
  }
}
