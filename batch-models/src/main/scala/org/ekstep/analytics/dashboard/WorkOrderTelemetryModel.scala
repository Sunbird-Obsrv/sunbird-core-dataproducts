package org.ekstep.analytics.dashboard

import java.io.Serializable
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, from_json, lit}
import org.apache.spark.sql.types.{ArrayType, BooleanType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher


// input output case classes
case class WODummyInput(timestamp: Long) extends AlgoInput  // no input, there are multiple sources to query
case class WODummyOutput() extends Output with AlgoOutput  // no output as we take care of kafka dispatches ourself

// config case class
case class WOConfig(debug: String, broker: String, compression: String, rawTelemetryTopic: String, sparkCassandraConnectionHost: String,
                  cassandraSunbirdKeyspace: String, cassandraWorkOrderTable: String, cassandraWorkAllocationTable: String,
                  cassandraUserWorkAllocationMappingTable: String) extends Serializable

// work order & allocation case classes
case class CompetencyAdditionalProperties(competencyArea: String, competencyType: String)
case class Competency(`type`: String, id: String, name: String, description: String, source: String, status: String,
                      level: String, additionalProperties: CompetencyAdditionalProperties, children: String)
case class Activity(`type`: String, id: String, name: String, description: String, status: String, source: String,
                    parentRole: String, submittedFromId: String, submittedToId: String, level: String)
case class Role(`type`: String, id: String, name: String, description: String, status: String, source: String,
                childNodes: Array[Activity], addedAt: Long, updatedAt: Long, updatedBy: String, archivedAt: Long,
                archived: Boolean)
case class RoleCompetency(roleDetails: Role, competencyDetails: Array[Competency])
case class WorkAllocation(id: String, userId: String, roleCompetencyList: Array[RoleCompetency],
                          unmappedActivities: Array[Activity], unmappedCompetencies: Array[Competency],
                          userPosition: String, positionId: String, positionDescription: String, workOrderId: String,
                          updatedAt: Long, updatedBy: String, errorCount: Long, progress: Long, createdAt: Long,
                          createdBy: String)
case class WorkOrder(id: String, name: String, deptId: String, deptName: String, status: String, userIds: Array[String],
                     createdBy: String, createdAt: Long, updatedBy: String, updatedAt: Long, progress: Long,
                     errorCount: Long, rolesCount: Long, activitiesCount: Long, competenciesCount: Long,
                     publishedPdfLink: String, signedPdfLink: String, mdo_name: String, users: Array[WorkAllocation])

// event case classes
case class EventObject(id: String, `type`: String)
case class PData(id: String, pid: String, ver: String)
case class EventContext(channel: String, pdata: PData, env: String)
case class CBData(data: WorkOrder)
case class CBObject(id: String, `type`: String, ver: String, name: String, org: String)
case class EData(state: String, props: Array[String], cb_object: CBObject, cb_data: CBData)
case class Actor(id: String, `type`: String)
case class Event(actor: Actor, eid: String, edata: EData, ver: String, ets: Long, context: EventContext, mid: String, `object`: EventObject)

/**
 * Model for processing competency metrics
 */
object WorkOrderTelemetryModel extends IBatchModelTemplate[String, WODummyInput, WODummyOutput, WODummyOutput] with Serializable {

  implicit var debug: Boolean = false

  implicit val className: String = "org.ekstep.analytics.dashboard.WorkOrderTelemetryModel"
  override def name() = "WorkOrderTelemetryModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[WODummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(WODummyInput(executionTime)))
  }

  override def algorithm(data: RDD[WODummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[WODummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    generateWorkOrderEvents(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[WODummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[WODummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  val competencyAdditionalPropertiesSchema: StructType = StructType(Seq(
    StructField("competencyArea", StringType, nullable = true),
    StructField("competencyType", StringType, nullable = true)
  ))

  val competencySchema: StructType = StructType(Seq(
    StructField("type", StringType, nullable = false),
    StructField("id", StringType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("source", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("level", StringType, nullable = true),
    StructField("additionalProperties", competencyAdditionalPropertiesSchema, nullable = true),
    StructField("children", StringType, nullable = true) // TODO
  ))

  val activitySchema: StructType = StructType(Seq(
    StructField("type", StringType, nullable = false),
    StructField("id", StringType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("source", StringType, nullable = true),
    StructField("parentRole", StringType, nullable = true),  // TODO
    StructField("submittedFromId", StringType, nullable = true),
    StructField("submittedToId", StringType, nullable = true),
    StructField("level", StringType, nullable = true)  // TODO
  ))

  val roleSchema: StructType = StructType(Seq(
    StructField("type", StringType, nullable = false),
    StructField("id", StringType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("source", StringType, nullable = true),
    StructField("childNodes", ArrayType(activitySchema), nullable = true),
    StructField("addedAt", LongType, nullable = true),
    StructField("updatedAt", LongType, nullable = true),
    StructField("updatedBy", StringType, nullable = true),
    StructField("archivedAt", LongType, nullable = true),
    StructField("archived", BooleanType, nullable = true)
  ))

  val roleCompetencySchema: StructType = StructType(Seq(
    StructField("roleDetails", roleSchema, nullable = true),
    StructField("competencyDetails", ArrayType(competencySchema), nullable = true)
  ))

  val workAllocSchema: StructType = StructType(Seq(
    StructField("id", StringType, nullable = false),
    StructField("userId", StringType, nullable = false),
    StructField("roleCompetencyList", ArrayType(roleCompetencySchema), nullable = true),
    StructField("unmappedActivities", ArrayType(activitySchema), nullable = true),
    StructField("unmappedCompetencies", ArrayType(competencySchema), nullable = true),
    StructField("userPosition", StringType, nullable = true),
    StructField("positionId", StringType, nullable = true),
    StructField("positionDescription", StringType, nullable = true),
    StructField("workOrderId", StringType, nullable = true),
    StructField("updatedAt", LongType, nullable = true),
    StructField("updatedBy", StringType, nullable = true),
    StructField("errorCount", LongType, nullable = true),
    StructField("progress", LongType, nullable = true),
    StructField("createdAt", LongType, nullable = true),
    StructField("createdBy", StringType, nullable = true)
  ))

  val workOrderSchema: StructType = StructType(Seq(
    StructField("id", StringType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("deptId", StringType, nullable = true),
    StructField("deptName", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("userIds", ArrayType(StringType), nullable = true),
    StructField("createdBy", StringType, nullable = true),
    StructField("createdAt", LongType, nullable = true),
    StructField("updatedBy", StringType, nullable = true),
    StructField("updatedAt", LongType, nullable = true),
    StructField("progress", LongType, nullable = true),
    StructField("errorCount", LongType, nullable = true),
    StructField("rolesCount", LongType, nullable = true),
    StructField("activitiesCount", LongType, nullable = true),
    StructField("competenciesCount", LongType, nullable = true),
    StructField("publishedPdfLink", StringType, nullable = true),
    StructField("signedPdfLink", StringType, nullable = true)
  ))

  /**
   * Master method, fetch all work orders from cassandra, add work allocations, remove PI info, dispatch to kafka
   *
   * Tables:
   * sunbird:work_order (id*, data)
   * sunbird:work_allocation (id*, data)
   * sunbird:user_work_allocation_mapping (userid*, workallocationid*, status, workorderid)
   *
   * Steps:
   * - get work order from sunbird:work_order, json parse $.data, remove createdByName, updatedByName, add mdo_name
   * - get all work allocations ids for the work order from sunbird:user_work_allocation_mapping
   * - collect $.data from sunbird:work_allocation for the above work allocations ids, json parse each
   * - remove userName, userEmail, updatedByName, createdByName from above work allocations
   * - remove roleCompetencyList[].roleDetails.childNodes[].(submittedFromName, submittedFromEmail, submittedToName, submittedToEmail) from work allocations
   * - remove unmappedActivities[].(submittedFromName, submittedFromEmail, submittedToName, submittedToEmail) from work allocations
   * - add list of above work orders as `users` field to work order
   * - attach to a valid CB_AUDIT event and dispatch to kafka
   *
   * @param timestamp
   * @param config
   * @param spark
   * @param sc
   * @param fc
   */
  def generateWorkOrderEvents(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: WOConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config

    val eventDS = workOrderEventDataSet()
    kafkaDispatch(eventDS, conf.rawTelemetryTopic)
  }

  def workOrderEventDataSet()(implicit spark: SparkSession, conf: WOConfig): Dataset[Event] = {
    val workOrderDS = workOrderDataSet()
    eventDataSet(workOrderDS)
  }

  def workOrderDataSet()(implicit spark: SparkSession, conf: WOConfig): Dataset[WorkOrder] = {
    import spark.implicits._

    val workOrderDF = workOrderDataFrame()
    val workAllocDF = workAllocationDataFrame()
    val workOrderWithAllocDF = workOrderAllocationDataFrame(workOrderDF, workAllocDF)

    workOrderWithAllocDF.as[WorkOrder]
  }

  def eventDataSet(workOrderAllocDS: Dataset[WorkOrder])(implicit spark: SparkSession, conf: WOConfig): Dataset[Event] = {
    import spark.implicits._
    val eventDS = workOrderAllocDS
      .map(r => {
        Event(
          actor = Actor(r.createdBy, "User"),
          eid = "CB_AUDIT",
          edata = EData(r.status, Array("WAT"), CBObject(r.id, "WorkOrder", "1.0", r.name, r.deptName), CBData(r)),
          ver = "3.0",
          ets = r.updatedAt,
          context = EventContext(r.deptId, PData("dev.mdo.portal", "mdo", "1.0"), "WAT"),
          mid = s"CB.${java.util.UUID.randomUUID.toString}",
          `object` = EventObject(r.id, "WorkOrder")
        )
      })

    showDS(eventDS)
    eventDS
  }

  def workOrderDataFrame()(implicit spark: SparkSession, conf: WOConfig): DataFrame = {
    val workOrderDF = cassandraTableAsDataFrame(conf.cassandraSunbirdKeyspace, conf.cassandraWorkOrderTable)
      .withColumn("data", from_json(col("data"), workOrderSchema))

    show(workOrderDF)
    workOrderDF
  }

  def workAllocationDataFrame()(implicit spark: SparkSession, conf: WOConfig): DataFrame = {
    val workAllocDF = cassandraTableAsDataFrame(conf.cassandraSunbirdKeyspace, conf.cassandraWorkAllocationTable)
      .withColumn("data", from_json(col("data"), workAllocSchema))

    show(workAllocDF)
    workAllocDF
  }

  def groupAllocationByWorkOrder(workAllocDF: DataFrame)(implicit spark: SparkSession, conf: WOConfig): DataFrame = {
    val workAllocGroupedDF = workAllocDF.withColumn("workOrderID", col("data.workOrderId"))
      .groupBy("workOrderID")
      .agg(collect_list("data").as("users"))

    show(workAllocGroupedDF)
    workAllocGroupedDF
  }

  def workOrderAllocationDataFrame(workOrderDF: DataFrame, workAllocDF: DataFrame)(implicit spark: SparkSession, conf: WOConfig): DataFrame = {
    val workAllocGroupedDF = groupAllocationByWorkOrder(workAllocDF)
    val workOrderWithAllocDF = workOrderDF.join(workAllocGroupedDF, col("id") === col("workOrderID"), "left")
      .select("data.*", "users")
      .withColumn("mdo_name", col("deptName"))

    show(workOrderWithAllocDF)
    workOrderWithAllocDF
  }

  /* Config functions */

  def getConfig[T](config: Map[String, AnyRef], key: String, default: T = null): T = {
    val path = key.split('.')
    var obj = config
    path.slice(0, path.length - 1).foreach(f => { obj = obj.getOrElse(f, Map()).asInstanceOf[Map[String, AnyRef]] })
    obj.getOrElse(path.last, default).asInstanceOf[T]
  }
  def getConfigModelParam(config: Map[String, AnyRef], key: String): String = getConfig[String](config, key, "")
  def getConfigSideBroker(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.brokerList", "")
  def getConfigSideBrokerCompression(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.compression", "snappy")
  def getConfigSideTopic(config: Map[String, AnyRef], key: String): String = getConfig[String](config, s"sideOutput.topics.${key}", "")
  def parseConfig(config: Map[String, AnyRef]): WOConfig = {
    WOConfig(
      debug = getConfigModelParam(config, "debug"),
      broker = getConfigSideBroker(config),
      compression = getConfigSideBrokerCompression(config),
      rawTelemetryTopic = getConfigSideTopic(config, "rawTelemetryTopic"),
      sparkCassandraConnectionHost = getConfigModelParam(config, "sparkCassandraConnectionHost"),
      cassandraSunbirdKeyspace = getConfigModelParam(config, "cassandraSunbirdKeyspace"),
      cassandraWorkOrderTable = getConfigModelParam(config, "cassandraWorkOrderTable"),
      cassandraWorkAllocationTable = getConfigModelParam(config, "cassandraWorkAllocationTable"),
      cassandraUserWorkAllocationMappingTable = getConfigModelParam(config, "cassandraUserWorkAllocationMappingTable")
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

  def showDS[T](df: Dataset[T]): Unit = {
    if (debug) {
      df.show()
      println("Count: " + df.count())
    }
    df.printSchema()
  }

  def kafkaDispatch[T](data: Dataset[T], topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: WOConfig): Unit = {
    if (topic == "") {
      println("ERROR: topic is blank, skipping kafka dispatch")
    } else if (conf.broker == "") {
      println("ERROR: broker list is blank, skipping kafka dispatch")
    } else {
      KafkaDispatcher.dispatch(Map("brokerList" -> conf.broker, "topic" -> topic, "compression" -> conf.compression), data.toJSON.rdd)
    }
  }

  def cassandraTableAsDataFrame(keySpace: String, table: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
      .option("keyspace", keySpace).option("table", table).load().persist(StorageLevel.MEMORY_ONLY)
  }

}