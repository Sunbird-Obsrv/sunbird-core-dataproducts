package org.ekstep.analytics.dashboard

import redis.clients.jedis.Jedis
import java.io.Serializable
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework._

import java.util


case class UDummyInput(timestamp: Long) extends AlgoInput  // no input, there are multiple sources to query
case class UDummyOutput() extends Output with AlgoOutput  // no output as we take care of kafka dispatches ourself

case class UConfig(debug: String, sparkCassandraConnectionHost: String, cassandraUserKeyspace: String,
                  cassandraUserTable: String, cassandraOrgTable: String, redisRegisteredOfficerCountKey: String,
                  redisTotalOfficerCountKey: String, redisOrgNameKey: String,
                  redisHost: String, redisPort: Int, redisDB: Int) extends Serializable

/**
 * Model for processing competency metrics
 */
object UserOrgRedisUpdateModel extends IBatchModelTemplate[String, UDummyInput, UDummyOutput, UDummyOutput] with Serializable {

  implicit var debug: Boolean = false

  implicit val className: String = "org.ekstep.analytics.dashboard.UserOrgRedisUpdateModel"
  override def name() = "UserOrgRedisUpdateModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[UDummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(UDummyInput(executionTime)))
  }

  override def algorithm(data: RDD[UDummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[UDummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    updateRedisUserOrgData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[UDummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[UDummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def updateRedisUserOrgData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: UConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config

    updateOrgUserCounts()
  }

  def updateOrgUserCounts()(implicit spark: SparkSession, sc: SparkContext, conf: UConfig): Unit = {
    val userData = cassandraTableAsDataFrame("sunbird", "user")
    // show(userData)

    val orgData = cassandraTableAsDataFrame("sunbird", "organisation")
    // show(orgData)

    val orgUserData = orgData.alias("o").join(
      userData.alias("u"), orgData.col("id") === userData.col("rootorgid"), "leftouter")
    // show(orgUserData)

    val orgUserCountData = orgUserData.groupBy("o.id", "o.orgname").count()
      .withColumn("totalCount", lit(10000))
      .select(
        col("o.id").alias("orgID"),
        col("o.orgname").alias("orgName"),
        col("count").alias("registeredCount"),
        col("totalCount")
      )
      .na.fill("", Seq("orgName"))
    show(orgUserCountData)

    // orgUserCountData.show(1000)

    val orgRegisteredUserCountMap = new util.HashMap[String, String]()
    val orgTotalUserCountMap = new util.HashMap[String, String]()
    val orgNameMap = new util.HashMap[String, String]()

    orgUserCountData.collect().foreach(row => {
      val orgID = row.getAs[String]("orgID")
      orgRegisteredUserCountMap.put(orgID, row.getAs[Long]("registeredCount").toString)
      orgTotalUserCountMap.put(orgID, row.getAs[Long]("totalCount").toString)
      orgNameMap.put(orgID, row.getAs[String]("orgName"))
    })

    val jedis = getRedisConnect(conf.redisHost, conf.redisPort)
    jedis.select(conf.redisDB) // need to use jedis because in redis-spark_2.11:2.7.0 selecting db does not seem to work

    jedis.hmset(conf.redisRegisteredOfficerCountKey, orgRegisteredUserCountMap)
    jedis.hmset(conf.redisTotalOfficerCountKey, orgTotalUserCountMap)
    jedis.hmset(conf.redisOrgNameKey, orgNameMap)

    jedis.close()
  }

  /* Config functions */

  def getConfig[T](config: Map[String, AnyRef], key: String, default: T = null): T = {
    val path = key.split('.')
    var obj = config
    path.slice(0, path.length - 1).foreach(f => { obj = obj.getOrElse(f, Map()).asInstanceOf[Map[String, AnyRef]] })
    obj.getOrElse(path.last, default).asInstanceOf[T]
  }
  def getConfigModelParam(config: Map[String, AnyRef], key: String, default: String = ""): String = getConfig[String](config, key, default)
  def parseConfig(config: Map[String, AnyRef]): UConfig = {
    UConfig(
      debug = getConfigModelParam(config, "debug"),
      sparkCassandraConnectionHost = getConfigModelParam(config, "sparkCassandraConnectionHost"),
      cassandraUserKeyspace = getConfigModelParam(config, "cassandraUserKeyspace"),
      cassandraUserTable = getConfigModelParam(config, "cassandraUserTable"),
      cassandraOrgTable = getConfigModelParam(config, "cassandraOrgTable"),
      redisRegisteredOfficerCountKey = getConfigModelParam(config, "redisRegisteredOfficerCountKey"),
      redisTotalOfficerCountKey = getConfigModelParam(config, "redisTotalOfficerCountKey"),
      redisOrgNameKey = getConfigModelParam(config, "redisOrgNameKey"),
      redisHost = getConfigModelParam(config, "redisHost"),
      redisPort = getConfigModelParam(config, "redisPort").toInt,
      redisDB = getConfigModelParam(config, "redisDB").toInt
    )
  }

  /* Util functions */
  def show(df: DataFrame): Unit = {
    if (debug) {
      df.show()
      println("Count: " + df.count())
    }
    df.printSchema()
  }

  def withTimestamp(df: DataFrame, timestamp: Long): DataFrame = {
    df.withColumn("timestamp", lit(timestamp))
  }

  def getRedisConnect(redisHost: String, redisPort: Int):Jedis = {
    new Jedis(redisHost, redisPort, 30000)
  }

  def cassandraTableAsDataFrame(keySpace: String, table: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
        .option("keyspace", keySpace).option("table", table).load().persist(StorageLevel.MEMORY_ONLY)
  }

}