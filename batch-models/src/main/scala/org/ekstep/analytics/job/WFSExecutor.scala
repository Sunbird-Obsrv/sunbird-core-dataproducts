package org.ekstep.analytics.job

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{JobConfig, JobContext}
import org.ekstep.analytics.framework.Level.ERROR
import org.ekstep.analytics.framework.exception.{DataFetcherException, JobNotFoundException}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}

object WFSExecutor extends Application {

    implicit val className = "org.ekstep.analytics.job.WFSExecutor"

    def main(model: String, fromPartition: String, toPartition: String, config: String) {

        JobLogger.start("Started executing WFSExecutor", Option(Map("config" -> config, "model" -> model, "fromPartition" -> fromPartition, "toPartition" -> toPartition)))
        val con = JSONUtils.deserialize[JobConfig](config)
        val sc = CommonUtil.getSparkContext(con.parallelization.getOrElse(200), con.appName.getOrElse(con.model));
        try {
            val result = CommonUtil.time({
                execute(model, fromPartition.toInt, toPartition.toInt, config)(sc);
            })
            JobLogger.end("WFSExecutor completed", "SUCCESS", Option(Map("timeTaken" -> result._1)));
        } catch {
            case ex: Exception =>
                JobLogger.log(ex.getMessage, None, ERROR);
                JobLogger.end("WFSExecutor failed", "FAILED")
                throw ex
        } finally {
            CommonUtil.closeSparkContext()(sc);
        }
    }

    def execute(model: String, fromPartition: Int, toPartition: Int, config: String)(implicit sc: SparkContext) {

        val range = fromPartition to toPartition
        range.toList.map { partition =>
            try {
                val jobConfig = config.replace("__partition__", partition.toString)
                val job = JobFactory.getJob(model);
                JobLogger.log("### Executing wfs for the partition - " + partition + " ###")
                job.main(jobConfig)(Option(sc));
            } catch {
                case ex: DataFetcherException => {
                    JobLogger.log(ex.getMessage, Option(Map("model_code" -> model, "partition" -> partition)), ERROR)
                }
                case ex: JobNotFoundException => {
                    JobLogger.log(ex.getMessage, Option(Map("model_code" -> model, "partition" -> partition)), ERROR)
                    throw ex;
                }
                case ex: Exception => {
                    JobLogger.log(ex.getMessage, Option(Map("model_code" -> model, "partition" -> partition)), ERROR)
                    ex.printStackTrace()
                }
            }
        }
    }
}
