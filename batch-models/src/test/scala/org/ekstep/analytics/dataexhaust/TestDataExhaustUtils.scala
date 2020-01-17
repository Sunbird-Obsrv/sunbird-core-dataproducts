
package org.ekstep.analytics.dataexhaust

import org.ekstep.analytics.framework.EventId
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.DerivedEvent

class TestDataExhaustUtils extends SparkSpec {
    "DataExhaustUtils" should "pass all test cases" in {

       val eventConfig = "{ \"searchType\": \"local\", \"fetchConfig\": { \"params\": { \"bucket\": \"'$bucket'\", \"prefix\": \"ss/\" } }, \"filterMapping\": { \"tags\": { \"name\": \"genieTag\", \"operator\": \"IN\" }, \"channel\": { \"name\": \"channel\", \"operator\": \"EQ\" }, \"app_id\": { \"name\": \"dimensions.pdata.id\", \"operator\": \"EQ\" } }, \"csvConfig\": { \"auto_extract_column_names\": true, \"columnMappings\": { \"context.granularity\": { \"hidden\": true }, \"edata.eks.timeSpent\": { \"to\": \"Time spent on the content\" }, \"edata.eks.screenSummary\": { \"hidden\": true }, \"edata.eks.currentLevel\": { \"to\": \"Current Domain Level\" }, \"edata.eks.syncDate\": { \"mapFunc\": \"timestampToDateTime\", \"to\": \"Sync Date\" }, \"edata.eks.timeDiff\": { \"hidden\": true }, \"context.date_range.from\": { \"hidden\": true }, \"edata.eks.start_time\": { \"mapFunc\": \"timestampToDateTime\", \"to\": \"Content Start Time\" }, \"uid\": { \"to\": \"Genie User ID\" }, \"ets\": { \"hidden\": true }, \"dimensions.group_user\": { \"to\": \"Is Group User\" }, \"edata.eks.activitySummary\": { \"to\": \"Activity Summary\" }, \"edata.eks.end_time\": { \"hidden\": true }, \"edata.eks.eventsSummary\": { \"hidden\": true }, \"eid\": { \"hidden\": true }, \"ver\": { \"hidden\": true }, \"edata.eks.levels\": { \"hidden\": true }, \"context.pdata.id\": { \"hidden\": true }, \"edata.eks.itemResponses\": { \"to\": \"edata.eks.itemResponses\" }, \"dimensions.gdata.id\": { \"to\": \"Content ID\" }, \"edata.eks.noOfLevelTransitions\": { \"to\": \"Number of Level Transitions\" }, \"edata.eks.noOfAttempts\": { \"to\": \"Number of Attempts\" }, \"tags\": { \"to\": \"Genie Tags\" }, \"edata.eks.interruptTime\": { \"to\": \"Total interrupt time\" }, \"syncts\": { \"mapFunc\": \"timestampToDateTime\", \"to\": \"Sync time stamp\" }, \"edata.eks.mimeType\": { \"hidden\": true }, \"edata.eks.telemetryVersion\": { \"to\": \"Telemetry Version\" }, \"context.pdata.ver\": { \"hidden\": true }, \"dimensions.gdata.ver\": { \"hidden\": true }, \"edata.eks.interactEventsPerMin\": { \"to\": \"Number of interactions per minute\" }, \"edata.eks.contentType\": { \"to\": \"Content Type\" }, \"context.pdata.model\": { \"hidden\": true }, \"dimensions.anonymous_user\": { \"to\": \"Logged in User\" }, \"dimensions.did\": { \"to\": \"Device ID\" }, \"dimensions.loc\": { \"to\": \"Lat / Long\" }, \"mid\": { \"to\": \"Session ID\" }, \"edata.eks.noOfInteractEvents\": { \"to\": \"Number of Interact events in the content session\" }, \"context.date_range.to\": { \"hidden\": true } } } }" 
       val config = JSONUtils.deserialize[EventId](eventConfig)
       val data = loadFile[DerivedEvent]("src/test/resources/workflow-summary/test-data1.log");
       
       val d = data.map(f => JSONUtils.serialize(f))
       val finalData = DataExhaustUtils.toCSV(d, config)(sc)
        
       finalData.count() should be (92)
       
    }
  
}