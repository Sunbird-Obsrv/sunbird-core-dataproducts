package org.ekstep.analytics.job

import com.datastax.spark.connector.cql.{CassandraConnectorConf, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{col, date_format}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}

import scala.collection.Map

object CassandraMigratorJob extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.CassandraMigratorJob"
  implicit val fc = new FrameworkContext();

  def name(): String = "CassandraMigratorJob"

  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {

    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val sparkContext = if (sc.isEmpty) CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model)) else sc.get
    val jobName = jobConfig.appName.getOrElse(name)
    JobLogger.init(jobName)
    JobLogger.start(jobName + " Started executing", Option(Map("config" -> config, "model" -> name)))
    val totalEvents = migrateData(jobConfig)
    JobLogger.end(jobName + " Completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "outputEvents" -> totalEvents)))
    CommonUtil.closeSparkContext()
  }

  def migrateData(jobConfig: JobConfig)(implicit sc: SparkContext): Long = {
    implicit val sqlContext = new SQLContext(sc)
    val modelParams =jobConfig.modelParams.get.asInstanceOf[Map[String,String]]
    val spark = sqlContext.sparkSession
    val keyspaceName =modelParams.getOrElse("keyspace","").toString
    val tableName = modelParams.getOrElse("table","").toString
    val dataDf = {
      spark.setCassandraConf("DataCluster", CassandraConnectorConf.
        ConnectionHostParam.option(modelParams.getOrElse("cassandraDataHost","localhost").toString) ++ CassandraConnectorConf.
        ConnectionPortParam.option(modelParams.getOrElse("cassandraDataPort","9042").toString))
      spark.setCassandraConf("MigrateCluster", CassandraConnectorConf.
        ConnectionHostParam.option(modelParams.getOrElse("cassandraMigrateHost","localhost").toString)  ++ CassandraConnectorConf.
        ConnectionPortParam.option(modelParams.getOrElse("cassandraMigratePort","9042").toString))
      spark.read.cassandraFormat(tableName, keyspaceName)
        .option("cluster", "DataCluster")
        .load()
    }

    val migratedData  = {
      CassandraConnector(sc.getConf.set("spark.cassandra.connection.host",
        modelParams.getOrElse("cassandraMigrateHost","localhost").toString)
        .set("spark.cassandra.connection.port",modelParams.getOrElse("cassandraMigratePort","9042").toString))
        .withSessionDo { session =>
        session.execute(s"""TRUNCATE TABLE $keyspaceName.$tableName""")
      }
      val repartitionColumns = if (!modelParams.getOrElse("repartitionColumns","").toString.isEmpty)
        modelParams.getOrElse("repartitionColumns","").toString.split(",").toSeq else Seq.empty[String]
      val repartitionDF =if(repartitionColumns.size > 0) {
        dataDf.repartition(repartitionColumns.map(f=> col(f)):_*)
      }
      else dataDf
      repartitionDF.write
        .cassandraFormat(tableName, keyspaceName)
        .option("cluster", "MigrateCluster")
        .mode("append")
        .save()
    }
    dataDf.count()
  }

}