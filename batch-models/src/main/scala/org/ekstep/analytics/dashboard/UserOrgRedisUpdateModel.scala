package org.ekstep.analytics.dashboard

import java.io.Serializable
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, expr, last, lit}
import org.ekstep.analytics.framework._

import java.util
import DashboardUtil._


case class UDummyInput(timestamp: Long) extends AlgoInput  // no input, there are multiple sources to query
case class UDummyOutput() extends Output with AlgoOutput  // no output as we take care of kafka dispatches ourself

case class UConfig(debug: String, broker: String, compression: String,
                   redisHost: String, redisPort: Int, redisDB: Int,
                   roleUserCountTopic: String, orgRoleUserCountTopic: String,
                   sparkCassandraConnectionHost: String, cassandraUserKeyspace: String,
                   cassandraUserTable: String, cassandraUserRolesTable: String, cassandraOrgTable: String,
                   redisRegisteredOfficerCountKey: String, redisTotalOfficerCountKey: String, redisOrgNameKey: String,
                   redisTotalRegisteredOfficerCountKey: String, redisTotalOrgCountKey: String) extends DashboardConfig

/**
 * Model for processing competency metrics
 */
object UserOrgRedisUpdateModel extends IBatchModelTemplate[String, UDummyInput, UDummyOutput, UDummyOutput] with Serializable {

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

    updateOrgUserCounts(timestamp)
  }

  def updateOrgUserCounts(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: UConfig): Unit = {
    // orgID, orgName, orgStatus
    val orgDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraOrgTable)
      .select(
        col("id").alias("orgID"),
        col("orgname").alias("orgName"),
        col("status").alias("orgStatus")
      ).na.fill("", Seq("orgName"))
    show(orgDF, "Org DataFrame")

    val activeOrgCount = orgDF.where(expr("orgStatus=1")).count()

    // userID, orgID, userStatus
    val userDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)
      .select(
        col("id").alias("userID"),
        col("rootorgid").alias("orgID"),
        col("status").alias("userStatus")
      ).na.fill("", Seq("orgID"))
    show(userDF, "User DataFrame")

    val activeUserCount = userDF.where(expr("userStatus=1")).count()

    // userID, role
    val roleDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserRolesTable)
      .select(
        col("userid").alias("userID"),
        col("role").alias("role")
      )
    show(roleDF, "User Role DataFrame")

    // userID, userStatus, orgID, orgName, orgStatus
    val userWithOrgDF = userDF.join(orgDF, Seq("orgID"), "left")
    show(userWithOrgDF)

    // userID, userStatus, orgID, orgName, orgStatus, role
    val userOrgRoleDF = userWithOrgDF.join(roleDF, Seq("userID"), "left").where(expr("userStatus=1 AND orgStatus=1"))
    show(userOrgRoleDF)

    // save role counts to kafka
    // role, count
    val roleCountDF = userOrgRoleDF.groupBy("role").agg(countDistinct("userID").alias("count"))
    show(roleCountDF)
    kafkaDispatch(withTimestamp(roleCountDF, timestamp), conf.roleUserCountTopic)

    // orgID, orgName, role, count
    val orgRoleCount = userOrgRoleDF.groupBy("orgID", "role").agg(
      last("orgName").alias("orgName"),
      countDistinct("userID").alias("count")
    )
    show(orgRoleCount)
    kafkaDispatch(withTimestamp(orgRoleCount, timestamp), conf.orgRoleUserCountTopic)

    // org user count
    val orgUserDF = orgDF.join(userDF.filter(col("orgID").isNotNull), Seq("orgID"), "left")
      .where(expr("userStatus=1 AND orgStatus=1"))
    show(orgUserDF, "Org User DataFrame")

    val orgUserCountData = orgUserDF.groupBy("orgID", "orgName").agg(expr("count(userID)").alias("registeredCount"))
      .withColumn("totalCount", lit(10000))
    show(orgUserCountData)

    val orgRegisteredUserCountMap = new util.HashMap[String, String]()
    val orgTotalUserCountMap = new util.HashMap[String, String]()
    val orgNameMap = new util.HashMap[String, String]()

    orgUserCountData.collect().foreach(row => {
      val orgID = row.getAs[String]("orgID")
      orgRegisteredUserCountMap.put(orgID, row.getAs[Long]("registeredCount").toString)
      orgTotalUserCountMap.put(orgID, row.getAs[Long]("totalCount").toString)
      orgNameMap.put(orgID, row.getAs[String]("orgName"))
    })

    val jedis = getOrCreateRedisConnect(conf.redisHost, conf.redisPort)
    jedis.select(conf.redisDB) // need to use jedis because in redis-spark_2.11:2.7.0 selecting db does not seem to work

    // set global org counts
    jedis.set(conf.redisTotalRegisteredOfficerCountKey, activeUserCount.toString)
    jedis.set(conf.redisTotalOrgCountKey, activeOrgCount.toString)

    redisReplaceMap(jedis, conf.redisRegisteredOfficerCountKey, orgRegisteredUserCountMap)
    redisReplaceMap(jedis, conf.redisTotalOfficerCountKey, orgTotalUserCountMap)
    redisReplaceMap(jedis, conf.redisOrgNameKey, orgNameMap)

    jedis.close()
  }

  /* Config functions */
  def parseConfig(config: Map[String, AnyRef]): UConfig = {
    UConfig(
      debug = getConfigModelParam(config, "debug"),
      broker = getConfigSideBroker(config),
      compression = getConfigSideBrokerCompression(config),
      redisHost = getConfigModelParam(config, "redisHost"),
      redisPort = getConfigModelParam(config, "redisPort").toInt,
      redisDB = getConfigModelParam(config, "redisDB").toInt,
      roleUserCountTopic = getConfigSideTopic(config, "roleUserCount"),
      orgRoleUserCountTopic = getConfigSideTopic(config, "orgRoleUserCount"),
      sparkCassandraConnectionHost = getConfigModelParam(config, "sparkCassandraConnectionHost"),
      cassandraUserKeyspace = getConfigModelParam(config, "cassandraUserKeyspace"),
      cassandraUserTable = getConfigModelParam(config, "cassandraUserTable"),
      cassandraUserRolesTable = getConfigModelParam(config, "cassandraUserRolesTable"),
      cassandraOrgTable = getConfigModelParam(config, "cassandraOrgTable"),
      redisRegisteredOfficerCountKey = getConfigModelParam(config, "redisRegisteredOfficerCountKey"),
      redisTotalOfficerCountKey = getConfigModelParam(config, "redisTotalOfficerCountKey"),
      redisOrgNameKey = getConfigModelParam(config, "redisOrgNameKey"),
      redisTotalRegisteredOfficerCountKey = getConfigModelParam(config, "redisTotalRegisteredOfficerCountKey"),
      redisTotalOrgCountKey = getConfigModelParam(config, "redisTotalOrgCountKey")
    )
  }

}