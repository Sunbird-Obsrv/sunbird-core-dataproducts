package org.ekstep.analytics.dashboard

import redis.clients.jedis.Jedis
import org.apache.spark.SparkContext
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import redis.clients.jedis.exceptions.JedisException
import redis.clients.jedis.params.ScanParams

import java.util
import scala.util.Try
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


trait DashboardConfig extends Serializable {
  val broker: String
  val debug: String
  val compression: String
  val redisHost: String
  val redisPort: Int
  val redisDB: Int
}

object DashboardUtil {

  implicit var debug: Boolean = false

  /* Util functions */
  def withTimestamp(df: DataFrame, timestamp: Long): DataFrame = {
    df.withColumn("timestamp", lit(timestamp))
  }

  def kafkaDispatch(data: RDD[String], topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    if (topic == "") {
      println("ERROR: topic is blank, skipping kafka dispatch")
    } else if (conf.broker == "") {
      println("ERROR: broker list is blank, skipping kafka dispatch")
    } else {
      KafkaDispatcher.dispatch(Map("brokerList" -> conf.broker, "topic" -> topic, "compression" -> conf.compression), data)
    }
  }

  def kafkaDispatch(data: DataFrame, topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    kafkaDispatch(data.toJSON.rdd, topic)
  }

  def kafkaDispatchDS[T](data: Dataset[T], topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    kafkaDispatch(data.toJSON.rdd, topic)
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
  def redisDispatch(key: String, data: util.Map[String, String])(implicit conf: DashboardConfig): Unit = {
    redisDispatch(conf.redisHost, conf.redisPort, conf.redisDB, key, data)
  }
  def redisDispatch(db: Int, key: String, data: util.Map[String, String])(implicit conf: DashboardConfig): Unit = {
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
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping saving to redis key=${key}")
      return
    }
    if (jedis.getDB != db) jedis.select(db)
    redisReplaceMap(jedis, key, data)
  }

  def redisGetHashKeys(jedis: Jedis, key: String): Seq[String] = {
    val keys = ListBuffer[String]()
    val scanParams = new ScanParams().count(100)
    var cur = ScanParams.SCAN_POINTER_START
    do {
      val scanResult = jedis.hscan(key, cur, scanParams)
      scanResult.getResult.foreach(res => {
        keys += res.getKey
      })
      cur = scanResult.getCursor
    } while (!cur.equals(ScanParams.SCAN_POINTER_START))
    keys.toList
  }
  def redisReplaceMap(jedis: Jedis, key: String, data: util.Map[String, String]): Unit = {
    // this deletes the keys that do not exist anymore manually
    val existingKeys = redisGetHashKeys(jedis, key)
    val toDelete = existingKeys.toSet.diff(data.keySet())
    if (toDelete.nonEmpty) jedis.hdel(key, toDelete.toArray:_*)
    // this will update redis hash map keys and create new ones, but will not delete ones that have been deleted
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
  def getOrCreateRedisConnect(conf: DashboardConfig): Jedis = getOrCreateRedisConnect(conf.redisHost, conf.redisPort)
  def createRedisConnect(host: String, port: Int): Jedis = {
    redisHost = host
    redisPort = port
    if (host == "") return null
    new Jedis(host, port, 30000)
  }
  def createRedisConnect(conf: DashboardConfig): Jedis = createRedisConnect(conf.redisHost, conf.redisPort)
  /* redis util functions over */

  /**
   * Convert data frame into a map, and save to redis
   *
   * @param redisKey key to save df data to
   * @param df data frame
   * @param keyField column name that forms the key (must be a string)
   * @param valueField column name that forms the value
   * @tparam T type of the value column
   */
  def redisDispatchDataFrame[T](redisKey: String, df: DataFrame, keyField: String, valueField: String)(implicit conf: DashboardConfig): Unit = {
    redisDispatch(redisKey, dfToMap[T](df, keyField, valueField))
  }

  def apiThrowException(method: String, url: String, body: String): String = {
    val request = method.toLowerCase() match {
      case "post" => new HttpPost(url)
      case _ => throw new Exception(s"HTTP method '${method}' not supported")
    }
    request.setHeader("Content-type", "application/json")  // set the Content-type
    request.setEntity(new StringEntity(body))  // add the JSON as a StringEntity
    val httpClient = HttpClientBuilder.create().build()  // create HttpClient
    val response = httpClient.execute(request)  // send the request
    val statusCode = response.getStatusLine.getStatusCode  // get status code
    if (statusCode < 200 || statusCode > 299) {
      throw new Exception(s"ERROR: got status code=${statusCode}, response=${EntityUtils.toString(response.getEntity)}")
    } else {
      EntityUtils.toString(response.getEntity)
    }
  }

  def api(method: String, url: String, body: String): String = {
    try {
      apiThrowException(method, url, body)
    } catch {
      case e: Throwable => {
        println(s"ERROR: ${e.toString}")
        return ""
      }
    }
  }

  def hasColumn(df: DataFrame, path: String): Boolean = Try(df(path)).isSuccess

  def dataFrameFromJSONString(jsonString: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val dataset = spark.createDataset(jsonString :: Nil)
    spark.read.option("mode", "DROPMALFORMED").option("multiline", value = true).json(dataset)
  }
  def emptySchemaDataFrame(schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  def druidSQLAPI(query: String, host: String, resultFormat: String = "object", limit: Int = 10000): String = {
    // TODO: tech-debt, use proper spark druid connector
    val url = s"http://${host}:8888/druid/v2/sql"
    val requestBody = s"""{"resultFormat":"${resultFormat}","header":false,"context":{"sqlOuterLimit":${limit}},"query":"${query}"}"""
    api("POST", url, requestBody)
  }

  def druidDFOption(query: String, host: String, resultFormat: String = "object", limit: Int = 10000)(implicit spark: SparkSession): Option[DataFrame] = {
    var result = druidSQLAPI(query, host, resultFormat, limit)
    result = result.trim()
    // return empty data frame if result is an empty string
    if (result == "") {
      println(s"ERROR: druidSQLAPI returned empty string")
      return None
    }
    val df = dataFrameFromJSONString(result)
    if (df.isEmpty) {
      println(s"ERROR: druidSQLAPI json parse result is empty")
      return None
    }
    // return empty data frame if there is an `error` field in the json
    if (hasColumn(df, "error")) {
      println(s"ERROR: druidSQLAPI returned error response, response=${result}")
      return None
    }
    // now that error handling is done, proceed with business as usual
    Some(df)
  }

  def dfToMap[T](df: DataFrame, keyField: String, valueField: String): util.Map[String, String] = {
    val map = new util.HashMap[String, String]()
    df.collect().foreach(row => map.put(row.getAs[String](keyField), row.getAs[T](valueField).toString))
    map
  }

  def cassandraTableAsDataFrame(keySpace: String, table: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
      .option("keyspace", keySpace).option("table", table).load().persist(StorageLevel.MEMORY_ONLY)
  }

  def elasticSearchDataFrame(host: String, index: String, query: String, fields: Seq[String])(implicit spark: SparkSession): DataFrame = {
    var df = spark.read.format("org.elasticsearch.spark.sql")
      .option("es.read.metadata", "false")
      .option("es.nodes", host)
      .option("es.port", "9200")
      .option("es.index.auto.create", "false")
      .option("es.nodes.wan.only", "false")
      .option("query", query)
      .load(index)
    df = df.select(fields.map(f => col(f)):_*) // instead of fields
    df
  }

  /* Config functions */
  def getConfig[T](config: Map[String, AnyRef], key: String, default: T = null): T = {
    val path = key.split('.')
    var obj = config
    path.slice(0, path.length - 1).foreach(f => { obj = obj.getOrElse(f, Map()).asInstanceOf[Map[String, AnyRef]] })
    obj.getOrElse(path.last, default).asInstanceOf[T]
  }
  def getConfigModelParam(config: Map[String, AnyRef], key: String, default: String = ""): String = getConfig[String](config, key, default)
  def getConfigSideBroker(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.brokerList", "")
  def getConfigSideBrokerCompression(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.compression", "snappy")
  def getConfigSideTopic(config: Map[String, AnyRef], key: String): String = getConfig[String](config, s"sideOutput.topics.${key}", "")
  /* Config functions end */

  def checkAvailableColumns(df: DataFrame, expectedColumnsInput: List[String]) : DataFrame = {
    expectedColumnsInput.foldLeft(df) {
      (df, column) => {
        if(!df.columns.contains(column)) {
          df.withColumn(column,lit(null).cast(StringType))
        }
        else (df)
      }
    }
  }

  /**
   * return parsed int or zero if parsing fails
   * @param s string to parse
   * @return int or zero
   */
  def intOrZero(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }
  }

  def show(df: DataFrame, msg: String = ""): Unit = {
    println("____________________________________________")
    println("SHOWING: " + msg)
    if (debug) {
      df.show()
      println("Count: " + df.count())
    }
    df.printSchema()
  }

  def showDS[T](ds: Dataset[T], msg: String = ""): Unit = {
    println("____________________________________________")
    println("SHOWING: " + msg)
    if (debug) {
      ds.show()
      println("Count: " + ds.count())
    }
    ds.printSchema()
  }
}
