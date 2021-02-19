package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.EmbeddedCassandra
import org.scalatest.Matchers

class TestCassandraMigratorJob extends SparkSpec with Matchers {

  val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.job.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"table\":\"user_enrolment_test\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
  override def beforeAll: Unit = {
    EmbeddedCassandra.loadData("src/test/resources/cassandra-migrator/data.cql") // Load test data in embedded cassandra server
  }
  override def afterAll() {
    EmbeddedCassandra.close()
  }

  "CassandraMigratorJob" should "Migrate cassandra data" in {
    CassandraMigratorJob.main(config)(Option(sc))
  }

  "CassandraMigratorJob" should "Migrate cassandra data with repartition" in {
    val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.job.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"table\":\"user_enrolment_test\",\"repartitionColumns\":\"batchid\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
    CassandraMigratorJob.main(config)(Option(sc))

  }
}
