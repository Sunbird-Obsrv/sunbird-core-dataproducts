package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.exception.DruidConfigException
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.cloud.storage.conf.AppConf

case class DruidOutput(date: Option[String], state: Option[String], district: Option[String], producer_id: Option[String], content_name: Option[String], content_id: Option[String], default_dimension: Option[String], device_loc_state: Option[String],
                       default_metric_long: Option[Integer] = Option(0), default_metric_double: Option[Double] = Option(0), total_scans: Option[Integer] = Option(0),
                       total_count: Option[Integer] = Option(0), total_scans_on_portal: Option[Integer] = Option(0), total_scans_on_app: Option[Integer] = Option(0),
                       total_sessions: Option[Integer] = Option(0), total_ts: Option[Double] = Option(0.0),
                       total_successful_scans: Option[Integer] = Option(0), total_failed_scans: Option[Integer] = Option(0),
                       total_content_download: Option[Integer] = Option(0), total_content_plays: Option[Integer] = Option(0),
                       total_content_plays_on_portal: Option[Integer] = Option(0), total_content_plays_on_app: Option[Integer] = Option(0),
                       total_content_plays_on_desktop: Option[Integer] = Option(0), total_app_sessions: Option[Integer] = Option(0),
                       total_unique_devices: Option[Double] = Option(0), total_unique_devices_on_portal: Option[Double] = Option(0),
                       time_spent_on_app_in_hours: Option[Double] = Option(0), total_devices_playing_content: Option[Integer] = Option(0),
                       devices_playing_content_on_app: Option[Integer] = Option(0), devices_playing_content_on_portal: Option[Integer] = Option(0),
                       total_unique_devices_on_app: Option[Double] = Option(0), total_unique_devices_on_desktop: Option[Double] = Option(0),
                       total_time_spent_in_hours: Option[Double] = Option(0), total_time_spent_in_hours_on_app: Option[Double] = Option(0),
                       total_time_spent_in_hours_on_portal: Option[Double] = Option(0), total_time_spent_in_hours_on_desktop: Option[Double] = Option(0),
                       total_percent_failed_scans: Option[Double] = Option(0), total_content_download_on_app: Option[Integer] = Option(0),
                       total_content_download_on_portal: Option[Integer] = Option(0), total_content_download_on_desktop: Option[Integer] = Option(0),
                       total_unique_devices_on_desktop_played_content: Option[Integer] = Option(0)) extends Input with AlgoInput with AlgoOutput with Output

case class ReportConfig(id: String, queryType: String, dateRange: QueryDateRange, metrics: List[Metrics], labels: Map[String, String], output: List[OutputConfig], mergeConfig: Option[MergeConfig] = None)
case class QueryDateRange(interval: Option[QueryInterval], staticInterval: Option[String], granularity: Option[String])
case class QueryInterval(startDate: String, endDate: String)
case class Metrics(metric: String, label: String, druidQuery: DruidQueryModel)
case class OutputConfig(`type`: String, label: Option[String], metrics: List[String], dims: List[String] = List(), fileParameters: List[String] = List("id", "dims"))
case class MergeConfig(frequency: String, basePath: String, rollup: Integer, rollupAge: Option[String] = None, rollupCol: Option[String] = None, rollupRange: Option[Integer] = None,
                       reportPath: String)
case class MergeScriptConfig(id: String, frequency: String, basePath: String, rollup: Integer, rollupAge: Option[String] = None, rollupCol: Option[String] = None, rollupRange: Option[Integer] = None,
                             merge: MergeFiles, container: String)
case class MergeFiles(files: List[Map[String, String]], dims: List[String])

object DruidQueryProcessingModel extends IBatchModelTemplate[DruidOutput, DruidOutput, DruidOutput, DruidOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.model.DruidQueryProcessingModel"
  override def name: String = "DruidQueryProcessingModel"

  override def preProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DruidOutput] = {

    setStorageConf(getStringProperty(config, "store", "local"), config.get("accountKey").asInstanceOf[Option[String]], config.get("accountSecret").asInstanceOf[Option[String]])
    data
  }

  def setStorageConf(store: String, accountKey: Option[String], accountSecret: Option[String])(implicit sc: SparkContext) {

    store.toLowerCase() match {
      case "s3" =>
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getConfig(accountKey.getOrElse("aws_storage_key")));
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getConfig(accountSecret.getOrElse("aws_storage_secret")));
      case "azure" =>
        sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
        sc.hadoopConfiguration.set("fs.azure.account.key." + AppConf.getConfig(accountKey.getOrElse("azure_storage_key")) + ".blob.core.windows.net", AppConf.getConfig(accountSecret.getOrElse("azure_storage_secret")))
      case _ =>
      // Do nothing
    }

  }

  override def algorithm(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DruidOutput] = {
    val strConfig = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(strConfig))

    val queryDims = reportConfig.metrics.map { f =>
      f.druidQuery.dimensions.getOrElse(List()).map(f => f.aliasName.getOrElse(f.fieldName))
    }.distinct

    if (queryDims.length > 1) throw new DruidConfigException("Query dimensions are not matching")

    val interval = reportConfig.dateRange
    val granularity = interval.granularity
    val queryInterval = if (interval.staticInterval.nonEmpty) {
      interval.staticInterval.get
    } else if (interval.interval.nonEmpty)
    {
      val dateRange = interval.interval.get
      getDateRange(dateRange)
    } else {
      throw new DruidConfigException("Both staticInterval and interval cannot be missing. Either of them should be specified")
    }

    val metrics = reportConfig.metrics.flatMap { f =>
      val queryConfig = if (granularity.nonEmpty)
        JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f.druidQuery)) ++ Map("intervals" -> queryInterval, "granularity" -> granularity.get)
      else
        JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f.druidQuery)) ++ Map("intervals" -> queryInterval)

      val data = DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(queryConfig)))
      data.map { x =>
        val dataMap = JSONUtils.deserialize[Map[String, AnyRef]](x)
        val key = dataMap.filter(m => (queryDims.flatten ++ List("date")).contains(m._1)).values.map(f => f.toString).toList.sorted(Ordering.String.reverse).mkString(",")
        (key, dataMap)
      }
    }
    val finalResult = sc.parallelize(metrics).foldByKey(Map())(_ ++ _)
    finalResult.map { f => JSONUtils.deserialize[DruidOutput](JSONUtils.serialize(f._2)) }
  }

  override def postProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DruidOutput] = {

    if (data.count() > 0) {
      val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
      val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))
      val dimFields = reportConfig.metrics.flatMap { m =>
        if (m.druidQuery.dimensions.nonEmpty) m.druidQuery.dimensions.get.map(f => f.aliasName.getOrElse(f.fieldName))
        else List()
      }

      val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
      implicit val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      //Using foreach as parallel execution might conflict with local file path
      val key = config.getOrElse("key", null).asInstanceOf[String]
      reportConfig.output.foreach { f =>
        val df = data.toDF().na.fill(0L)
        val metricFields = f.metrics
        val fieldsList = (dimFields ++ metricFields ++ List("date")).distinct
        val dimsLabels = labelsLookup.filter(x => f.dims.contains(x._1)).values.toList
        val filteredDf = df.select(fieldsList.head, fieldsList.tail: _*)
        val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*).na.fill("unknown")
        val reportFinalId = if (f.label.nonEmpty && f.label.get.nonEmpty) reportConfig.id + "/" + f.label.get else reportConfig.id
        saveReport(renamedDf, config ++ Map("dims" -> dimsLabels, "reportId" -> reportFinalId, "fileParameters" -> f.fileParameters, "format" -> f.`type`))
      }
    } else {
      JobLogger.log("No data found from druid", None, Level.INFO)
    }
    data
  }

  def getDateRange(interval: QueryInterval): String = {
    val offset: Long = DateTimeZone.forID("Asia/Kolkata").getOffset(DateTime.now())
    val startDate = DateTime.parse(interval.startDate).withTimeAtStartOfDay().plus(offset).toString("yyyy-MM-dd'T'HH:mm:ss")
    val endDate = DateTime.parse(interval.endDate).withTimeAtStartOfDay().plus(offset).toString("yyyy-MM-dd'T'HH:mm:ss")
    startDate + "/" + endDate
  }

  private def getStringProperty(config: Map[String, AnyRef], key: String, defaultValue: String) : String = {
    config.getOrElse(key, defaultValue).asInstanceOf[String]
  }

  def saveReport(data: DataFrame, config: Map[String, AnyRef])(implicit sc: SparkContext): Unit = {

    val container =  getStringProperty(config, "container", "test-container")
    val storageConfig = StorageConfig(getStringProperty(config, "store", "local"),container, getStringProperty(config, "key", "/tmp/druid-reports"), config.get("accountKey").asInstanceOf[Option[String]]);
    val format = config.get("format").get.asInstanceOf[String]
    val filePath = config.getOrElse("filePath", AppConf.getConfig("spark_output_temp_dir")).asInstanceOf[String]
    val key = config.getOrElse("key", null).asInstanceOf[String]
    val reportId = config.get("reportId").get.asInstanceOf[String]
    val fileParameters = config.get("fileParameters").get.asInstanceOf[List[String]]
    val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportMergeConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap)).mergeConfig
    val dims = if (fileParameters.nonEmpty && fileParameters.contains("date")) config.get("dims").get.asInstanceOf[List[String]] ++ List("Date") else config.get("dims").get.asInstanceOf[List[String]]
    val deltaFiles = if (dims.nonEmpty) {
      val duplicateDims = dims.map(f => f.concat("Duplicate"))
      var duplicateDimsDf = data
      dims.foreach { f =>
        duplicateDimsDf = duplicateDimsDf.withColumn(f.concat("Duplicate"), col(f))
      }
      duplicateDimsDf.saveToBlobStore(storageConfig, format, reportId, Option(Map("header" -> "true")), Option(duplicateDims))
    } else {
      data.saveToBlobStore(storageConfig, format, reportId, Option(Map("header" -> "true")), None)
    }
    if(reportMergeConfig.nonEmpty){
      val mergeConf = reportMergeConfig.get
      val reportPath = mergeConf.reportPath
      val filesList = deltaFiles.map{f =>
        if(dims.size == 1 && dims.contains("Date")) {
          val finalReportPath = if (key != null) key + reportPath else reportPath
          Map("reportPath" -> finalReportPath, "deltaPath" -> f.substring(f.indexOf(storageConfig.fileName, 0)))
        } else {
          val reportPrefix = f.substring(0, f.lastIndexOf("/")).split(reportId)(1)
          Map("reportPath" -> (reportPrefix + "/" + reportPath), "deltaPath" -> f.substring(f.indexOf(storageConfig.fileName, 0)))
        }
      }
      val mergeScriptConfig = MergeScriptConfig(reportId, mergeConf.frequency, mergeConf.basePath, mergeConf.rollup,
        mergeConf.rollupAge, mergeConf.rollupCol, mergeConf.rollupRange, MergeFiles(filesList, List("Date")), container)
      mergeReport(mergeScriptConfig)
    }
    else {
      JobLogger.log(s"Merge report is not configured, hence skipping that step", None, INFO)
    }
  }

  def mergeReport(mergeConfig: MergeScriptConfig, virtualEnvDir: Option[String] = Option("/mount/venv")): Unit = {
    val mergeConfigStr = JSONUtils.serialize(mergeConfig)
    val mergeReportCommand = Seq("bash", "-c",
      s"source ${virtualEnvDir.get}/bin/activate; " +
        s"dataproducts report_merger --report_config='$mergeConfigStr'")
    JobLogger.log(s"Merge report script command:: $mergeReportCommand", None, INFO)
    val mergeReportExitCode = ScriptDispatcher.dispatch(mergeReportCommand)
    if (mergeReportExitCode == 0) {
      JobLogger.log(s"Merge report script::Success", None, INFO)
    } else {
      JobLogger.log(s"Merge report script failed with exit code $mergeReportExitCode", None, ERROR)
      throw new Exception(s"Merge report script failed with exit code $mergeReportExitCode")
    }
  }

}
