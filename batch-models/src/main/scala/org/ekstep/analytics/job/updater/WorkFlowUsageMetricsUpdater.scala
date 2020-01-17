package org.ekstep.analytics.job.updater

/**
  * @author Manjunath Davanam <manjunathd@ili.in>
  */

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.updater.UpdateWorkFlowUsageMetricsModel
import org.ekstep.analytics.framework.FrameworkContext

object WorkFlowUsageMetricsUpdater extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.MetricsUpdater"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing metrics job updater")
    JobDriver.run("batch", config, UpdateWorkFlowUsageMetricsModel)
    JobLogger.log("UpdateMetrics Job Completed!!")
  }
}