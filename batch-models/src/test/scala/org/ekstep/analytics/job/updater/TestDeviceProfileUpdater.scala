package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.{Constants, EmbeddedPostgresql}

class TestDeviceProfileUpdater extends SparkSpec(null) {

    val deviceTable =  Constants.DEVICE_PROFILE_TABLE

    override def beforeAll(){
        super.beforeAll()
        EmbeddedPostgresql.start()
        EmbeddedPostgresql.createDeviceProfileTable()
    }
    "DeviceProfileUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/device-profile/test-data1.log"))))), None, None, "org.ekstep.analytics.updater.DeviceProfileUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDeviceProfileUpdater"), Option(false))
        DeviceProfileUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
    override def afterAll() {
        super.afterAll()
        EmbeddedPostgresql.close()
    }
}