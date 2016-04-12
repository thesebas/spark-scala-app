package thesebas.spark

import java.sql.Date

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.log4j.Logger


object ScenarioWithDateRange {
  val AppName = "ScenarioWithDateRange"

  val logger = Logger.getLogger(AppName)

  def time[R](label: String, block: => R): R = {
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    logger.info(s"TIMER: [$label] elapsed time ${(end - start) / 1e6}")
    result
  }

  def main(args: Array[String]): Unit = {

    ScenarioWithDateRange.execute(
      master = None, // Some("spark://127.0.0.1:7077"),
      args = args.toList,
      jars = List(SparkContext.jarOfObject(this).get)
    )
  }

  def execute(master: Option[String], args: List[String], jars: Seq[String] = Nil): Unit = {


    val sc = {
      val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "10.0.40.42")
        .setAppName(AppName)

      if (master.isDefined) conf.setMaster(master.get)
      new SparkContext(conf)

    }

    val readTable = args.lift(0).getOrElse("cockpit2_testTogether")
    val saveTable = args.lift(1).getOrElse(readTable)

    logger.info(s"reading from $readTable and writing back to $saveTable")


    val campaigns = List(
      Map(
        "name" -> "androidN",
        "tag" -> "chanel:android",
        "budget" -> 100f
      )
      ,
      Map(
        "name" -> "kaspersky",
        "tag" -> "chanel:software",
        "budget" -> 150f
      )
    )

    logger.info("campaigns definitions ready");

    val processedDay = "2015-10-01"
    val processedToDay = "2015-10-10"

    val readTableRDD: CassandraTableScanRDD[UrlRow] = sc.cassandraTable[UrlRow]("el_test", readTable)

    def mapTagsToPis(date: Date, tags: Set[String]): List[((Date, String), Int)] = {
      if (tags.isEmpty) {
        return List(((date, "invalid"), 1))
      }
      (for (tag <- tags) yield ((date, tag), 1)).toList

    }

    val pisPerTagRDD = sc.cassandraTable[(Date, Set[String])]("el_test", readTable)
      .select("date", "tags")
      .where("date > ? AND date < ?", processedDay, processedToDay)
      .flatMap[((Date, String), Int)]({ case (d: Date, s: Set[String]) => mapTagsToPis(d, s) })
      .reduceByKey(_ + _)
      .persist()

    pisPerTagRDD
      .map { case ((date: Date, tag: String), pi: Int) => (tag, date, pi) }
      .saveToCassandra("el_test", "pisPerTagPerDate", SomeColumns("tag", "date", "pi"))


    val pisPerTag = time("collect as map", {
      pisPerTagRDD.collectAsMap()
    })

    Console.println(pisPerTag)

    //    val pisPerTag = Map(
    //      "chanel:android" -> 1500,
    //      "chanel:software" -> 2500
    //    )

    val data = readTableRDD
      .select("url", "date", "tags", "revs", "rev", "pic", "pif", "pi")
      .where("date >= ? AND date <= ? ", processedDay, processedToDay)



    def calculateCampaign(row: UrlRow): UrlRow = {
      val pic = row.pi * row.pif

      for (campaign <- campaigns) {
        val campaignTag = campaign("tag") match {
          case x: String => x
          case _ => ""
        }
        if (row.tags.contains(campaignTag)) {
          val campaignBudget = campaign("budget") match {
            case x: Float => x
          }
          val rev = pic / pisPerTag((row.Date, campaignTag)) * campaignBudget
          val campaignName = campaign("name") match {
            case x: String => x
          }
          row.revs = row.revs ++ Map(("cp_" + campaignName) -> rev)
        }
      }

      row
    }

    def sumRevenues(row: UrlRow): UrlRow = {
      val rev = row.revs.values.sum
      row.rev = rev
      row
    }


    val calculated = data
      .map(calculateCampaign)
      .map(sumRevenues)

    time("save to cassandra", {
      calculated.saveToCassandra("el_test", saveTable, SomeColumns("url", "date", "revs", "rev"))
    })


    sc.stop()
  }
}
