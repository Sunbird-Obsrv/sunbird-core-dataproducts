package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.exception.JobNotFoundException
import com.fasterxml.jackson.databind.JsonMappingException
import org.ekstep.analytics.framework.FrameworkContext

class TestJobExecutor extends BaseSpec {

    "JobExecutor" should "execute job when model code is passed" in {
      
        implicit val fc = new FrameworkContext();
        
        a[JsonMappingException] should be thrownBy {
            JobExecutor.main("org.ekstep.analytics.job.summarizer.WorkFlowSummarizer", "");
        }

        the[JobNotFoundException] thrownBy {
            JobExecutor.main("abc", null);
        } should have message "Unknown job type found"
    }

}