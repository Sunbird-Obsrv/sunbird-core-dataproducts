package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.model.WorkFlowUsageSummaryModel
import org.ekstep.analytics.framework.FrameworkContext

object WorkFlowUsageSummarizer extends optional.Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.WorkFlowUsageSummarizer"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, WorkFlowUsageSummaryModel);
        JobLogger.log("Job Completed.")
    }
}