package org.ekstep.analytics.job

import org.ekstep.analytics.framework.FrameworkContext

/**
 * @author Santhosh
 */
object JobExecutor {

    val className = "org.ekstep.analytics.job.JobExecutor"

    def main(model: String, config: String) {
        implicit val fc : FrameworkContext = null
        val job = JobFactory.getJob(model);
        job.main(config)(None, Option(fc))
    }

}

object JobExecutorV2 {

    val className = "org.ekstep.analytics.job.JobExecutorV2"

    def main(model: String, config: String)(implicit fc : FrameworkContext) {
        val job = JobFactory.getJob(model);
        job.main(config)(None, Option(fc))
    }
}