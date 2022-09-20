package org.ekstep.analytics.exhaust

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, JobContext, StorageConfig}
import org.ekstep.analytics.framework.util.CommonUtil
import org.sunbird.cloud.storage.conf.AppConf

trait BaseReportsJob {

  def getReportingFrameworkContext()(implicit fc: Option[FrameworkContext]): FrameworkContext = {
    fc match {
      case Some(value) => {
        value
      }
      case None => {
        new FrameworkContext();
      }
    }
  }

  def openSparkSession(config: JobConfig): SparkSession = {

    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val sparkCassandraConnectionHost = modelParams.get("sparkCassandraConnectionHost")
    val sparkElasticsearchConnectionHost = modelParams.get("sparkElasticsearchConnectionHost")
    val sparkRedisConnectionHost = modelParams.get("sparkRedisConnectionHost")
    val sparkUserDbRedisIndex = modelParams.get("sparkUserDbRedisIndex")
    val sparkUserDbRedisPort = modelParams.get("sparkUserDbRedisPort")
    JobContext.parallelization = CommonUtil.getParallelization(config)
    val readConsistencyLevel = modelParams.getOrElse("cassandraReadConsistency", "LOCAL_QUORUM").asInstanceOf[String];
    val writeConsistencyLevel = modelParams.getOrElse("cassandraWriteConsistency", "LOCAL_QUORUM").asInstanceOf[String]
    val sparkSession = CommonUtil.getSparkSession(JobContext.parallelization, config.appName.getOrElse(config.model), sparkCassandraConnectionHost, sparkElasticsearchConnectionHost, Option(readConsistencyLevel),sparkRedisConnectionHost, sparkUserDbRedisIndex, sparkUserDbRedisPort)
    setReportsStorageConfiguration(config)(sparkSession)
    sparkSession;

  }

  def setReportsStorageConfiguration(config: JobConfig)(implicit spark: SparkSession) {

    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val store = modelParams.getOrElse("store", "local").asInstanceOf[String];
    val storageKey = modelParams.getOrElse("storageKeyConfig", "reports_storage_key").asInstanceOf[String];
    val storageSecret = modelParams.getOrElse("storageSecretConfig", "reports_storage_secret").asInstanceOf[String];
    store.toLowerCase() match {
      case "s3" =>
        spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getConfig(storageKey));
        spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getConfig(storageSecret));
      case "azure" =>
        val storageKeyValue = AppConf.getConfig(storageKey);
        spark.sparkContext.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
        spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.$storageKeyValue.blob.core.windows.net", AppConf.getConfig(storageSecret))
        spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.keyprovider.$storageKeyValue.blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider")
      case "gcloud" =>
        spark.sparkContext.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        spark.sparkContext.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.email", AppConf.getConfig(storageKey))
        spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.private.key", AppConf.getConfig(storageSecret))
        spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.private.key.id", AppConf.getConfig("gcloud_private_secret_id"))
      case _ =>

    }

  }

  def getStorageConfig(config: JobConfig, key: String): StorageConfig = {

    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val container = modelParams.getOrElse("storageContainer", "reports").asInstanceOf[String]
    val storageKey = modelParams.getOrElse("storageKeyConfig", "reports_storage_key").asInstanceOf[String];
    val storageSecret = modelParams.getOrElse("storageSecretConfig", "reports_storage_secret").asInstanceOf[String];
    val store = modelParams.getOrElse("store", "local").asInstanceOf[String]
    StorageConfig(store, container, key, Option(storageKey), Option(storageSecret));
  }

}
