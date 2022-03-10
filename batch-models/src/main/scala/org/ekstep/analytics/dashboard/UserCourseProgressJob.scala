package org.ekstep.analytics.dashboard
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}

object UserCourseProgressJob extends optional.Application with IJob{

  implicit val className = "org.ekstep.analytics.job.UserCourseProgressJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, UserCourseProgressModel)
    JobLogger.log("Job Completed.")
  }
}