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

    val processedDay = "2015-10-01"
    val processedToDay = "2015-10-10"

    logger.info(s"reading from $readTable and writing back to $saveTable, date range:[$processedDay:$processedToDay]")


    val bcampaigns = time("broadcast campaign data", {
      sc.broadcast(List(
        Map(
          "name" -> "androidN",
          "tag" -> "channel:android",
          "budget" -> 100f
        )
        ,
        Map(
          "name" -> "kaspersky",
          "tag" -> "channel:software",
          "budget" -> 150f
        )
      ))
    })

    logger.info("campaigns definitions ready");


    val readTableRDD: CassandraTableScanRDD[UrlRow] = sc.cassandraTable[UrlRow]("el_test", readTable)

    def mapTagsToPis(date: Date, tags: Set[String], pic: Double): List[((Date, String), Double)] = {
      if (tags.isEmpty) {
        return List(((date, "invalid"), 1d))
      }
      //tags.toList.map(tag => ((date, tag), 1))
      for (tag <- tags.toList) yield ((date, tag), pic)
    }

    val pisPerTagRDD = sc.cassandraTable[(Date, Set[String], Int, Double)]("el_test", readTable)
      .select("date", "tags", "pi", "pif")
      .where("date >= ? AND date < ?", processedDay, processedToDay)
      .flatMap[((Date, String), Double)] { case (d: Date, s: Set[String], pi: Int, pif: Double) => mapTagsToPis(d, s, pi * pif) }
      .reduceByKey(_ + _)
      .persist()

//    time("save pis per tag per day to cassandra", {
//      pisPerTagRDD
//        .map { case ((date: Date, tag: String), pi: Double) => (tag, date, pi) }
//        .saveToCassandra("el_test", "pisPerTagPerDate", SomeColumns("tag", "date", "pi"))
//    })


    val bpisPerTag = time("broadcast pisPerTag", {
      sc.broadcast({
        time("collect as map", {
          pisPerTagRDD.collectAsMap()
        })
      })
    })

    //    val pisPerTag = Map(
    //      "chanel:android" -> 1500,
    //      "chanel:software" -> 2500
    //    )

    val data = readTableRDD
      .select("url", "date", "tags", "revs", "rev", "pic", "pif", "pi")
      .where("date >= ? AND date < ? ", processedDay, processedToDay)


    def calculateCampaign(row: UrlRow): UrlRow = {
      val pic = row.pi * row.pif

      val campaigns = bcampaigns.value
      val pisPerTag = bpisPerTag.value

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

    def sumRevenues(row: UrlRow): (String, Date, Map[String, Double], Int) = {
      (row.Url, row.Date, row.revs, (row.revs.values.sum * 1e6).round.toInt)
    }


    val calculated = data
      .map(calculateCampaign)
      .map(sumRevenues)

    time("process and save to cassandra", {
      calculated.saveToCassandra("el_test", saveTable, SomeColumns("url", "date", "revs", "rev"))
    })


    sc.stop()
  }

}
