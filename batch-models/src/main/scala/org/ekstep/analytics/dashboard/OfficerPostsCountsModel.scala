package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.ekstep.analytics.framework.{AlgoInput, AlgoOutput, FrameworkContext, IBatchModelTemplate, Output}
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisException
import com.mongodb.spark.sql._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.rdd.RDD
import java.io.Serializable
import java.util

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


/**
 * Model for processing total posts and comments by officers
 */

class OfficerPostsCountsModel extends IBatchModelTemplate[String, OPDummyInput, OPDummyOutput, OPDummyOutput] with Serializable{

  implicit var debug: Boolean = false

  implicit val className: String = "org.ekstep.analytics.dashboard.OfficerPostsCountsModel"
  override def name() = "OfficerPostsCountsModel"

  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[OPDummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(OCDummyInput(executionTime)))
  }

  override def algorithm(events: RDD[OPDummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[OPDummyOutput] = {
    val timestamp = events.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf)
      .getOrCreate()
    processOfficerPostsData(timestamp, config)
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
  def processOfficerPostsData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: OPConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config

    val uri = s"mongodb://${conf.nodebbHost}:${conf.nodebbPort}"
    val readConf = ReadConfig(Map("uri" -> uri, "database" -> conf.nodebbDB, "collection" -> "objects"))

    val totalPostsDf = spark.read.mongo(readConf)

    val totalPostssMap = dfToMap(totalPostsDf, "uid", "post_count")
    redisDispatch("dashboard_officer_posts_comments_count", totalPostssMap)

    closeRedisConnect()
  }

  def dfToMap[T](df: DataFrame, keyField: String, valueField: String): util.Map[String, String] = {
    val map = new util.HashMap[String, String]()
    df.collect().foreach(row => map.put(row.getAs[String](keyField), row.getAs[T](valueField).toString))
    map
  }

  /* Config functions */

  def getConfig[T](config: Map[String, AnyRef], key: String, default: T = null): T = {
    val path = key.split('.')
    var obj = config
    path.slice(0, path.length - 1).foreach(f => { obj = obj.getOrElse(f, Map()).asInstanceOf[Map[String, AnyRef]] })
    obj.getOrElse(path.last, default).asInstanceOf[T]
  }
  def getConfigModelParam(config: Map[String, AnyRef], key: String): String = getConfig[String](config, key, "")
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

  /* Util functions */
  def show(df: DataFrame, msg: String = ""): Unit = {
    println("SHOWING: " + msg)
    if (debug) {
      df.show()
      println("Count: " + df.count())
    }
    df.printSchema()
  }

  def withTimestamp(df: DataFrame, timestamp: Long): DataFrame = {
    df.withColumn("timestamp", lit(timestamp))
  }

  /* redis util functions */
  var redisConnect: Jedis = null
  var redisHost: String = ""
  var redisPort: Int = 0
  def closeRedisConnect(): Unit = {
    if (redisConnect != null) {
      redisConnect.close()
      redisConnect = null
    }
  }
  def redisDispatch(key: String, data: util.Map[String, String])(implicit conf: OPConfig): Unit = {
    redisDispatch(conf.redisHost, conf.redisPort, conf.redisDB, key, data)
  }
  def redisDispatch(db: Int, key: String, data: util.Map[String, String])(implicit conf: OPConfig): Unit = {
    redisDispatch(conf.redisHost, conf.redisPort, db, key, data)
  }
  def redisDispatch(host: String, port: Int, db: Int, key: String, data: util.Map[String, String]): Unit = {
    try {
      redisDispatchWithoutRetry(host, port, db, key, data)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        redisDispatchWithoutRetry(host, port, db, key, data)
    }
  }
  def redisDispatchWithoutRetry(host: String, port: Int, db: Int, key: String, data: util.Map[String, String]): Unit = {
    if (data == null || data.isEmpty) {
      println(s"WARNING: map is empty, skipping saving to redis key=${key}")
      return
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis.getDB != db) jedis.select(db)
    jedis.hmset(key, data)
  }
  def getOrCreateRedisConnect(host: String, port: Int): Jedis = {
    if (redisConnect == null) {
      redisConnect = createRedisConnect(host, port)
    } else if (redisHost != host || redisPort != port) {
      redisConnect = createRedisConnect(host, port)
    }
    redisConnect
  }
  def getOrCreateRedisConnect(conf: OPConfig): Jedis = getOrCreateRedisConnect(conf.redisHost, conf.redisPort)
  def createRedisConnect(host: String, port: Int): Jedis = {
    redisHost = host
    redisPort = port
    new Jedis(host, port, 30000)
  }
  def createRedisConnect(conf: OPConfig): Jedis = createRedisConnect(conf.redisHost, conf.redisPort)
  /* redis util functions over */

}
