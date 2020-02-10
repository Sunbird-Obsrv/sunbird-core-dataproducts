package org.ekstep.analytics.job

import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.Metrics.MetricsAuditJob
import org.ekstep.analytics.job.summarizer._
import org.ekstep.analytics.job.updater._
import org.ekstep.analytics.job.report.{AssessmentMetricsJob, CourseConsumptionJob, CourseEnrollmentJob, CourseMetricsJob, StateAdminGeoReportJob, StateAdminReportJob}
import org.ekstep.analytics.job.batch.VideoStreamingJob

/**
 * @author Santhosh
 */

object JobFactory {
    @throws(classOf[JobNotFoundException])
    def getJob(jobType: String): IJob = {
        jobType.toLowerCase() match {
            case "monitor-job-summ" =>
                MonitorSummarizer
            case "wfs" =>
                WorkFlowSummarizer
            case "wfus" =>
                WorkFlowUsageSummarizer
            case "wfu" =>
                WorkFlowUsageUpdater
            case "ds" =>
                DeviceSummarizer
            case "dpu" =>
                DeviceProfileUpdater
            case "video-streaming" =>
                VideoStreamingJob
            case "portal-metrics" =>
                PortalMetricsUpdater
            case "workflow-usage-metrics" =>
                WorkFlowUsageMetricsUpdater
            case "data-exhaust" =>
                DataExhaustJob
            case "course-dashboard-metrics" =>
                CourseMetricsJob
            case "telemetry-replay" =>
                EventsReplayJob
            case "summary-replay" =>
                EventsReplayJob
            case "content-rating-updater" =>
                ContentRatingUpdater
            case "experiment" =>
                ExperimentDefinitionJob
            case "assessment-dashboard-metrics" =>
                AssessmentMetricsJob
            case "daily-metrics" =>
                DruidQueryProcessor
            case "desktop-consumption-report" =>
                DruidQueryProcessor
            case "district-monthly" =>
                DruidQueryProcessor
            case "district-weekly" =>
                DruidQueryProcessor
            case "admin-user-reports" =>
                StateAdminReportJob
            case "audit-metrics-report" =>
                MetricsAuditJob
            case "admin-geo-reports" =>
                StateAdminGeoReportJob
            case "course-consumption-report" =>
                CourseConsumptionJob
            case "course-enrollment-report" =>
                CourseEnrollmentJob
            case _ =>
                throw new JobNotFoundException("Unknown job type found") 
        }
    }
}
