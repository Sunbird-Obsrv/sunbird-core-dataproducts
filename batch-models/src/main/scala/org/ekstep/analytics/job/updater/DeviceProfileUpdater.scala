package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.updater.UpdateDeviceProfileDB
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.FrameworkContext

object DeviceProfileUpdater extends optional.Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.updater.DeviceProfileUpdater"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, UpdateDeviceProfileDB);
        JobLogger.log("Job Completed.")
    }
}