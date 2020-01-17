package org.ekstep.analytics.job

import org.ekstep.analytics.framework.Dispatcher
import com.google.common.eventbus.Subscribe
import org.ekstep.analytics.framework.V3DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.util.EventBusUtil
import org.ekstep.analytics.framework.FrameworkContext

object JobMonitor {
    
    def init(config: JobManagerConfig) {
        EventBusUtil.register(new JobEventListener(config.slackChannel, config.slackUserName));
    }
    
    def jobStartMsg(event: V3DerivedEvent) : String = {
        val dataMap = event.edata.asInstanceOf[Map[String, AnyRef]];
        val jobdata = dataMap.getOrElse("data", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]];
        val jobName = jobdata.getOrElse("model", "").asInstanceOf[String];
        val date = jobdata.getOrElse("date", "").asInstanceOf[String];
        s"*Job*: `$jobName` | Status: `Started` | Date: `$date`";
    }
    
    def jobEndMsg(event: V3DerivedEvent) : String =  {
        val dataMap = event.edata.asInstanceOf[Map[String, AnyRef]];
        val status = dataMap.getOrElse("status", "FAILED").asInstanceOf[String];
        val jobdata = dataMap.getOrElse("data", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]];
        val jobName = jobdata.getOrElse("model", "").asInstanceOf[String];
        val date = jobdata.getOrElse("date", "").asInstanceOf[String];
        val inputEvents = jobdata.getOrElse("inputEvents", 0).asInstanceOf[Number].toString();
        if("SUCCESS".equals(status)) {
            val outputEvents = jobdata.getOrElse("outputEvents", 0).asInstanceOf[Number].toString();
            val timeTaken = jobdata.getOrElse("timeTaken", 0).asInstanceOf[Number].toString();
            s"*Job*: `$jobName` | Status: `$status` | Date: `$date` | inputEvents: `$inputEvents` | outputEvents: `$outputEvents` | timeTaken: `$timeTaken`";
        } else {
            val statusMsg = jobdata.getOrElse("statusMsg", "NA").asInstanceOf[String];
            s"*Job*: `$jobName` | Status: `$status` | Date: `$date` | inputEvents: `$inputEvents` | statusMsg: `$statusMsg`";
        }
    }
  
}

class JobEventListener(channel: String, userName: String) {
    
    private val dispatcher = Dispatcher("slack", Map("channel" -> channel, "userName" -> userName));
    implicit val fc = new FrameworkContext();
    
    @Subscribe def onMessage(event: String) {
        val meEvent = JSONUtils.deserialize[V3DerivedEvent](event);
        meEvent.eid match {
            case "JOB_START" =>
                OutputDispatcher.dispatch(dispatcher, Array(JobMonitor.jobStartMsg(meEvent)))
            case "JOB_END" =>
                OutputDispatcher.dispatch(dispatcher, Array(JobMonitor.jobEndMsg(meEvent)))
        }
    }
    
}