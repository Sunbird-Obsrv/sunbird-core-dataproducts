package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.updater.UpdateWorkFlowUsageDB
import org.ekstep.analytics.framework.FrameworkContext

object WorkFlowUsageUpdater extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.WorkFlowUsageUpdater"

    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, UpdateWorkFlowUsageDB);
        JobLogger.log("Job Completed.")
    }
}