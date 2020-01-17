package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.{Constants, EmbeddedPostgresql}

class TestDeviceSummaryModel extends SparkSpec(null) {

  import org.ekstep.analytics.framework.FrameworkContext
  implicit val fc = new FrameworkContext()

  val deviceTable =  Constants.DEVICE_PROFILE_TABLE

  override def beforeAll(){
    super.beforeAll()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createDeviceProfileTable()
  }

  "DeviceSummaryModel" should "generate DeviceSummary events from a sample file and pass all positive test cases" in {

    val rdd = loadFile[String]("src/test/resources/device-summary/test_data1.log");
    val me = DeviceSummaryModel.execute(rdd, None);
    me.count() should be(3)

    val event1 = me.filter(f => "B6672B9EFDF620EDD8F43CFDF1101B14".equals(f.mid)).first

    event1.eid should be("ME_DEVICE_SUMMARY");
    event1.context.pdata.model.get should be("DeviceSummary");
    event1.context.pdata.ver should be("1.0");
    event1.context.granularity should be("DAY");
    event1.context.date_range should not be null;
    event1.dimensions.did.get should be("88edda82418a1e916e9906a2fd7942cb");
    event1.dimensions.channel.get should be("b00bc992ef25f1a9a8d63291e20efc8d")

    val summary1 = JSONUtils.deserialize[DeviceSummary](JSONUtils.serialize(event1.edata.eks));
    summary1.content_downloads should be(1)
    summary1.contents_played should be(2)
    summary1.total_ts should be(50.0)
    summary1.total_launches should be(1)
    summary1.unique_contents_played should be(2)
    summary1.dial_stats.total_count should be(0)
    summary1.dial_stats.success_count should be(0)
    summary1.dial_stats.failure_count should be(0)

    val event2 = me.filter(f => "8B6DEB9BCDE92D8830C5BAB6DEB8F14F".equals(f.mid)).first

    event2.eid should be("ME_DEVICE_SUMMARY");
    event2.context.pdata.model.get should be("DeviceSummary");
    event2.context.pdata.ver should be("1.0");
    event2.context.granularity should be("DAY");
    event2.context.date_range should not be null;
    event2.dimensions.did.get should be("49edda82418a1e916e9906a2fd7942cb");
    event2.dimensions.channel.get should be("b00bc992ef25f1a9a8d63291e20efc8d")

    val summary2 = JSONUtils.deserialize[DeviceSummary](JSONUtils.serialize(event2.edata.eks));
    summary2.content_downloads should be(0)
    summary2.contents_played should be(0)
    summary2.total_ts should be(8.0)
    summary2.total_launches should be(1)
    summary2.unique_contents_played should be(0)
    summary2.dial_stats.total_count should be(3)
    summary2.dial_stats.success_count should be(3)
    summary2.dial_stats.failure_count should be(0)

    val event3 = me.filter(f => "42B5E0E32ADD7F8DF58666165AFFB6AB".equals(f.mid)).first

    event3.eid should be("ME_DEVICE_SUMMARY");
    event3.context.pdata.model.get should be("DeviceSummary");
    event3.context.pdata.ver should be("1.0");
    event3.context.granularity should be("DAY");
    event3.context.date_range should not be null;
    event3.dimensions.did.get should be("48edda82418a1e916e9906a2fd7942cb");
    event3.dimensions.channel.get should be("b00bc992ef25f1a9a8d63291e20efc8d")

    val summary3 = JSONUtils.deserialize[DeviceSummary](JSONUtils.serialize(event3.edata.eks));
    summary3.content_downloads should be(0)
    summary3.contents_played should be(0)
    summary3.total_ts should be(18.0)
    summary3.total_launches should be(2)
    summary3.unique_contents_played should be(0)
    summary3.dial_stats.total_count should be(4)
    summary3.dial_stats.success_count should be(2)
    summary3.dial_stats.failure_count should be(2)
  }



  it should "update the value of first_access according to the value from postgres else update with dt_range.from" in {

      EmbeddedPostgresql.execute(s"TRUNCATE $deviceTable")
      EmbeddedPostgresql.execute(s"INSERT INTO $deviceTable (device_id, first_access)" +
        "VALUES('49edda82418a1e916e9906a2fd7942cb', '2018-09-24 22:49:15.883')")
      EmbeddedPostgresql.execute(s"INSERT INTO $deviceTable (device_id, first_access)" +
        "VALUES('88edda82418a1e916e9906a2fd7942cb', '2018-09-20 22:49:15.883')")


    val rdd = loadFile[String]("src/test/resources/device-summary/test_data1.log")
    val measuredEvent = DeviceSummaryModel.execute(rdd, None)

    measuredEvent.collect().foreach{ x =>
      val summary = JSONUtils.deserialize[DeviceSummary](JSONUtils.serialize(x.edata.eks))
      if(x.dimensions.did.get.equals("49edda82418a1e916e9906a2fd7942cb"))
        summary.firstAccess.toString should be("2018-09-21 17:19:15.883")
      else if(x.dimensions.did.get.equals("88edda82418a1e916e9906a2fd7942cb"))
        summary.firstAccess.toString should be("2018-09-20 22:49:15.883")
      else
        summary.firstAccess.toString should be("2018-09-21 17:19:15.883")
    }
  }

  it should "update the value of first_access with dt_range.from if null is returned from postgres" in {
      EmbeddedPostgresql.execute(s"TRUNCATE $deviceTable")
      EmbeddedPostgresql.execute(s"INSERT INTO $deviceTable (device_id) VALUES('49edda82418a1e916e9906a2fd7942cb')")

      EmbeddedPostgresql.execute(s"INSERT INTO $deviceTable (device_id, first_access) VALUES('88edda82418a1e916e9906a2fd7942cb', '2018-09-14 07:10:35.883')")

    val rdd = loadFile[String]("src/test/resources/device-summary/test_data1.log")
    val measuredEvent = DeviceSummaryModel.execute(rdd, None)

    measuredEvent.collect().foreach{ x =>
      val summary = JSONUtils.deserialize[DeviceSummary](JSONUtils.serialize(x.edata.eks))
      if(x.dimensions.did.get.equals("49edda82418a1e916e9906a2fd7942cb"))
        {
          summary.firstAccess.toString should be("2018-09-21 17:19:15.883")
        }
      else if(x.dimensions.did.get.equals("88edda82418a1e916e9906a2fd7942cb"))
        {
          summary.firstAccess.toString should be("2018-09-14 07:10:35.883")
        }
      else {
        summary.firstAccess.toString should be("2018-09-21 17:19:15.883")
      }

    }
  }

  override def afterAll(): Unit ={
    super.afterAll()
    EmbeddedPostgresql.close
  }
}
