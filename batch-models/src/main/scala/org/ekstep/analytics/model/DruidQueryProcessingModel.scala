package org.ekstep.analytics.model

import akka.actor.ActorSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.exception.DruidConfigException
import org.ekstep.analytics.framework.fetcher.{AkkaHttpUtil, DruidDataFetcher}
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.util.DruidQueryUtil
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.cloud.storage.conf.AppConf
import collection.mutable.LinkedHashMap

case class ReportConfig(id: String, queryType: String, dateRange: QueryDateRange, metrics: List[Metrics], labels: LinkedHashMap[String, String], output: List[OutputConfig],
                        mergeConfig: Option[MergeConfig] = None, storageKey: Option[String] = Option(AppConf.getConfig("storage.key.config")),
                        storageSecret: Option[String] = Option(AppConf.getConfig("storage.secret.config")))
case class QueryDateRange(interval: Option[QueryInterval], staticInterval: Option[String], granularity: Option[String], intervalSlider: Int = 0)
case class QueryInterval(startDate: String, endDate: String)
case class Metrics(metric: String, label: String, druidQuery: DruidQueryModel)
case class OutputConfig(`type`: String, label: Option[String], metrics: List[String], dims: List[String] = List(), fileParameters: List[String] = List("id", "dims"), locationMapping: Option[Boolean] = Option(false))
case class MergeConfig(frequency: String, basePath: String, rollup: Integer, rollupAge: Option[String] = None, rollupCol: Option[String] = None, rollupRange: Option[Integer] = None,
                       reportPath: String, postContainer: Option[String] = None, deltaFileAccess: Option[Boolean] = Option(true), reportFileAccess: Option[Boolean] = Option(true))
case class MergeScriptConfig(id: String, frequency: String, basePath: String, rollup: Integer, rollupAge: Option[String] = None, rollupCol: Option[String] = None, rollupRange: Option[Integer] = None,
                             merge: MergeFiles, container: String, postContainer: Option[String] = None, deltaFileAccess: Option[Boolean] = Option(true), reportFileAccess: Option[Boolean] = Option(true))
case class MergeFiles(files: List[Map[String, String]], dims: List[String])



object DruidQueryProcessingModel extends IBatchModelTemplate[DruidOutput, DruidOutput, DruidOutput, DruidOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.model.DruidQueryProcessingModel"
  override def name: String = "DruidQueryProcessingModel"


  override def preProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DruidOutput] = {

    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(config.getOrElse("reportConfig", Map()).asInstanceOf[Map[String, AnyRef]]))
    setStorageConf(getStringProperty(config, "store", "local"), reportConfig.storageKey, reportConfig.storageSecret)
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
    val streamQuery = config.getOrElse("streamQuery", false).asInstanceOf[Boolean]
    val exhaustQuery = config.getOrElse("exhaustQuery", false).asInstanceOf[Boolean]
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
    } else if (interval.interval.nonEmpty) {
      val dateRange = interval.interval.get
      getDateRange(dateRange, interval.intervalSlider)
    } else {
      throw new DruidConfigException("Both staticInterval and interval cannot be missing. Either of them should be specified")
    }
    if(exhaustQuery) {
      val queryConfig = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(reportConfig.metrics.head.druidQuery)) ++
          Map("intervalSlider" -> interval.intervalSlider, "intervals" -> queryInterval, "granularity" -> granularity.get)
      DruidDataFetcher.executeSQLQuery(JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(queryConfig)), fc.getAkkaHttpUtil())
    }
    else {
      val metrics = reportConfig.metrics.map { f =>
        val queryConfig = if (granularity.nonEmpty)
          JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f.druidQuery)) ++ Map("intervalSlider" -> interval.intervalSlider, "intervals" -> queryInterval, "granularity" -> granularity.get)
        else
          JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f.druidQuery)) ++ Map("intervalSlider" -> interval.intervalSlider, "intervals" -> queryInterval)
        val config = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(queryConfig))
        val data = if (streamQuery) {
          DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(queryConfig)), true)
        }
        else {
          DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(queryConfig)))
        }
        data.map { x =>
          val dataMap = JSONUtils.deserialize[Map[String, AnyRef]](x)
          val key = dataMap.filter(m => (queryDims.flatten ++ List("date")).contains(m._1)).values.map(f => f.toString).toList.sorted(Ordering.String.reverse).mkString(",")
          (key, dataMap)

        }

      }
      val finalResult = metrics.fold(sc.emptyRDD)(_ union _).foldByKey(Map())(_ ++ _)
      finalResult.map { f =>
        DruidOutput(f._2)
      }
    }
  }


  override def postProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DruidOutput] = {
    if (fc.inputEventsCount.value > 0) {
      val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
      val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))
      val dimFields = reportConfig.metrics.flatMap { m =>
        if (m.druidQuery.dimensions.nonEmpty) m.druidQuery.dimensions.get.map(f => f.aliasName.getOrElse(f.fieldName))
        else if(m.druidQuery.sqlDimensions.nonEmpty) m.druidQuery.sqlDimensions.get.map(f => f.fieldName)
        else List()
      }

      val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
      implicit val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      //Using foreach as parallel execution might conflict with local file path
      val key = config.getOrElse("key", null).asInstanceOf[String]
      reportConfig.output.foreach { f =>
        val df = {if (f.locationMapping.getOrElse(false)) {
          DruidQueryUtil.removeInvalidLocations(sqlContext.read.json(data.map(f => JSONUtils.serialize(f))),
            DruidQueryUtil.getValidLocations(RestUtil),List("state", "district"))
        } else sqlContext.read.json(data.map(f => JSONUtils.serialize(f)))}.na.fill(0)
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

  def getDateRange(interval: QueryInterval, intervalSlider: Integer = 0): String = {
    val offset: Long = DateTimeZone.forID("Asia/Kolkata").getOffset(DateTime.now())
    val startDate = DateTime.parse(interval.startDate).withTimeAtStartOfDay().minusDays(intervalSlider).plus(offset).toString("yyyy-MM-dd'T'HH:mm:ss")
    val endDate = DateTime.parse(interval.endDate).withTimeAtStartOfDay().minusDays(intervalSlider).plus(offset).toString("yyyy-MM-dd'T'HH:mm:ss")
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
    val quoteColumns =config.get("quoteColumns").getOrElse(List()).asInstanceOf[List[String]]
    val deltaFiles = if (dims.nonEmpty) {
      val duplicateDims = dims.map(f => f.concat("Duplicate"))
      var duplicateDimsDf = data
      dims.foreach { f =>
        duplicateDimsDf = duplicateDimsDf.withColumn(f.concat("Duplicate"), col(f))

      }
      if(quoteColumns.nonEmpty) {
        import org.apache.spark.sql.functions.udf
        val quoteStr = udf((column: String) =>  "\'"+column+"\'")
        quoteColumns.map(column => {
          duplicateDimsDf = duplicateDimsDf.withColumn(column, quoteStr(col(column)))
        })
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
        mergeConf.rollupAge, mergeConf.rollupCol, mergeConf.rollupRange, MergeFiles(filesList, List("Date")), container, mergeConf.postContainer,
        mergeConf.deltaFileAccess, mergeConf.reportFileAccess)
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
