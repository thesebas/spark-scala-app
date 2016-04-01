package thesebas.spark

import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

/**
  * Created by thesebas on 2016-04-01.
  */
object CopyAll {
  val AppName = "CopyAll App"

  def main(args: Array[String]): Unit = {

    val master = None // Some("spark://127.0.0.1:7077"),

    val jars = List(SparkContext.jarOfObject(this).get)

    val sc = {
      val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "10.0.40.42")
        .setAppName(AppName)

      if (master.isDefined) conf.setMaster(master.get)
      new SparkContext(conf)

    }

    val readTable = if (args.isDefinedAt(0)) args.head else "cockpit2_testTogether"
    val saveTable = if (args.isDefinedAt(1)) args(1) else readTable

    sc
      .cassandraTable("el_test", readTable)
      .saveToCassandra("el_test", saveTable)

    sc.stop()

  }
}
