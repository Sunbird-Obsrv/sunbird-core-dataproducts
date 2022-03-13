//package org.ekstep.analytics.dashboard
//
//import org.apache.http.client.methods.HttpPost
//import org.apache.http.entity.StringEntity
//import org.apache.http.impl.client.DefaultHttpClient
//import org.apache.http.util.EntityUtils
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
//import org.apache.spark.sql.functions.{col, current_timestamp, explode_outer, expr, from_json}
//import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
//import org.apache.spark.storage.StorageLevel
//
//
//case class CompetencyGapDataRow(timestamp: Long, userID: String, orgID: String, workOrderID: String, competencyID: String,
//                                expectedLevel: Int, declaredLevel: Int, competencyGap: Int) extends AnyRef with Serializable
//
//object CompetencyGapTest extends Serializable {
//
//  def main(args: Array[String]): Unit = {
//    implicit val spark: SparkSession =
//      SparkSession
//        .builder()
//        .appName("CompetencyTest")
//        .config("spark.master", "local[*]")
//        .config("spark.cassandra.connection.host", "10.0.0.7")
//        .config("spark.cassandra.output.batch.size.rows", "1000")
//        //.config("spark.cassandra.read.timeoutMS", "60000")
//        .getOrCreate()
//    val res = time(competencyGapRDD());
//    Console.println("Time taken to execute script", res._1);
//    spark.stop();
//  }
//
//
//  def competencyGapRDD()(implicit spark: SparkSession): RDD[CompetencyGapDataRow] = {
//    val df = competencyGapData()
//    val encoder = Encoders.bean(classOf[CompetencyGapDataRow])
//    df.as[CompetencyGapDataRow](encoder).rdd
//  }
//
//  def competencyGapData()(implicit spark: SparkSession): DataFrame = {
//
//    val ecDF = expectedCompetencyData()
//    val dcDF = declaredCompetencyData()
//
//    var gapDF = ecDF.join(dcDF,
//      ecDF.col("competency_id") <=> dcDF.col("id") && ecDF.col("user_id") <=> dcDF.col("uid"),
//      "leftouter"
//    )
//    gapDF = gapDF.na.fill(0, Seq("declared_level")) // if null values created during join fill with 0
//    gapDF = gapDF.select(
//      col("user_id").alias("userID"),
//      col("competency_id").alias("competencyID"),
//      col("org_id").alias("orgID"),
//      col("work_order_id").alias("workOrderID"),
//      col("competency_level").alias("expectedLevel"),
//      col("declared_level").alias("declaredLevel")
//    )
//    gapDF = gapDF.withColumn("competencyGap", expr("expectedLevel - declaredLevel"))
//    gapDF = gapDF.withColumn("timestamp", current_timestamp())
//
//    gapDF.show()
//    gapDF.printSchema()
//    gapDF
//  }
//
//  def druidSQLAPI(query: String, host: String, resultFormat: String = "object", limit: Int = 10000): String = {
//    val url = s"http://${host}:8888/druid/v2/sql"
//    val requestBody = s"""{"resultFormat":"${resultFormat}","header":true,"context":{"sqlOuterLimit":${limit}},"query":"${query}"}"""
//    val post = new HttpPost(url) // create an HttpPost object
//    post.setHeader("Content-type", "application/json") // set the Content-type
//    post.setEntity(new StringEntity(requestBody)) // add the JSON as a StringEntity
//    val response = (new DefaultHttpClient).execute(post) // send the post request TODO: use non-deprecated way
//    EntityUtils.toString(response.getEntity)
//  }
//
//  def expectedCompetencyData()(implicit spark: SparkSession): DataFrame = {
//    // val query = """SELECT edata_cb_data_deptId AS org_id, edata_cb_data_wa_id AS work_order_id, edata_cb_data_wa_userId AS user_id, edata_cb_data_wa_competency_id AS competency_id, CAST(REGEXP_EXTRACT(edata_cb_data_wa_competency_level, '[0-9]+') AS INTEGER) AS competency_level, edata_cb_data_wa_competency_name AS competency_name, edata_cb_data_wa_competency_status AS competency_status, edata_cb_data_wa_competency_type AS competency_type FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"""
//    val query = """SELECT edata_cb_data_deptId AS org_id, edata_cb_data_wa_id AS work_order_id, edata_cb_data_wa_userId AS user_id, edata_cb_data_wa_competency_id AS competency_id, CAST(REGEXP_EXTRACT(edata_cb_data_wa_competency_level, '[0-9]+') AS INTEGER) AS competency_level FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"""
//    val result = druidSQLAPI(query, "10.0.0.13")
//
//    import spark.implicits._
//    val dataset = spark.createDataset(result :: Nil)
//    val df = spark.read.option("multiline", value = true).json(dataset).filter(col("competency_level").notEqual(0))
//
//    df.show()
//    df.printSchema()
//    df
//  }
//
//  def declaredCompetencyData()(implicit spark: SparkSession): DataFrame = {
//
//    val competencySchema = StructType(Seq(
//      StructField("id", StringType, nullable = true),
//      StructField("name", StringType, nullable = true),
//      StructField("status", StringType, nullable = true),
//      StructField("competencyType", StringType, nullable = true),
//      StructField("competencySelfAttestedLevel", IntegerType, nullable = true)
//    ))
//
//    val profileSchema = StructType(
//      StructField("competencies", ArrayType(competencySchema), nullable = true) :: Nil
//    )
//
//    val userdata = spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
//      .option("keyspace", "sunbird").option("table", "user").load().persist(StorageLevel.MEMORY_ONLY);
//
//    Console.println("Number of users", userdata.count());
//
//    val up = userdata.where(col("profiledetails").isNotNull)
//      .select("userid", "profiledetails")
//      .withColumn("profile", from_json(col("profiledetails"), profileSchema))
//      .select(col("userid"), explode_outer(col("profile.competencies")).alias("competency"))
//      .where(col("competency").isNotNull)
//      .select(
//        col("userid").alias("uid"),
//        col("competency.id").alias("id"),
//        // col("competency.name").alias("d_name"),
//        // col("competency.status").alias("d_status"),
//        // col("competency.competencyType").alias("d_type"),
//        col("competency.competencySelfAttestedLevel").alias("declared_level")
//      )
//      .na.fill(1, Seq("declared_level")) // if competency is listed without a level assume level 1
//
//    up.show()
//    up.printSchema()
//    up
//  }
//
//  def courseCompetencyData()(implicit spark: SparkSession): Unit = {
//    val coursedata = spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
//      .option("keyspace", "dev_hierarchy_store").option("table", "content_hierarchy").load().persist(StorageLevel.MEMORY_ONLY);
//
//    val coursedata2 = coursedata.select("identifier", "hierarchy")
//
//  }
//
//  def time[R](block: => R): (Long, R) = {
//    val t0 = System.currentTimeMillis()
//    val result = block // call-by-name
//    val t1 = System.currentTimeMillis()
//    ((t1 - t0), result)
//  }
//
//}
