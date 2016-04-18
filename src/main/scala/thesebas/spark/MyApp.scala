package thesebas.spark

import java.sql.Date

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.log4j.Logger


/**
  * Created by thesebas on 2016-03-30.
  */
object MyApp {
  val AppName = "MyApp"
  val logger = Logger.getLogger(AppName)

  def time[R](label: String, block: => R): R = {
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    logger.info(s"TIMER: [$label] elapsed time ${(end - start) / 1e6}")
    result
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

    val processedDay = "2015-10-08"

    val readTableRDD: CassandraTableScanRDD[UrlRow] = sc.cassandraTable[UrlRow]("el_test", readTable)

    /*

    def map_tags_to_pis(row):
        if row['tags'] is None:
            return [('invalid', 1)]
        ret = [(tag, row['pi'] * row['pif']) for tag in row['tags']]
        ret.append(('processed', 1))
        return ret


    pisPerTag = rdd.select("url", "date", "counts", "cnt", "pi", "tags", "pif") \
        .where('"date" = ? ', processedDate) \
        .flatMap(map_tags_to_pis) \
        .reduceByKey(lambda a, b: a + b) \
        .collectAsMap()

     */

    def mapRowTagsToPis(row: UrlRow): List[(String, Double)] = {
      if (row.tags.isEmpty) {
        return List(("invalid", 1d))
      }

      for (tag <- row.tags.toList) yield (tag, row.pi * row.pif)
    }

    def mapTagsToPis(date: Date, tags: Set[String], pic: Double): List[((Date, String), Double)] = {
      if (tags.isEmpty) {
        return List(((date, "invalid"), 1d))
      }

      for (tag <- tags.toList) yield ((date, tag), pic)
    }


    val pisPerTagRDD = sc.cassandraTable[(Date, Set[String], Int, Double)]("el_test", readTable)
      .select("date", "tags", "pi", "pif")
      .where("date = ?", processedDay)
      .flatMap { case (date: Date, tags: Set[String], pi: Int, pif: Double) => mapTagsToPis(date, tags, pi * pif) }
      .reduceByKey(_ + _)
      .persist()

//    time("save pis per tag per day", {
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
    //    Console.println(pisPerTag)

    //    val pisPerTag = Map(
    //      ("2015-10-10", "chanel:android") -> 1500,
    //      ("2015-10-10","chanel:software") -> 2500
    //    )


    val data = readTableRDD
      .select("url", "date", "tags", "revs", "rev", "pic", "pif", "pi")
      .where("date = ?", processedDay)



    def calculateCampaign(row: UrlRow): UrlRow = {
      val pic = row.pi * row.pif

      val campaigns = bcampaigns.value
      val pisPerTag = bpisPerTag.value

      for (campaign <- campaigns) {
        val campaignTag = campaign("tag").asInstanceOf[String]
        if (row.tags.contains(campaignTag)) {
          val rev = pic / pisPerTag((row.Date, campaignTag)) * campaign("budget").asInstanceOf[Float]
          row.revs = row.revs ++ Map("cp_" + campaign("name").asInstanceOf[String] -> rev)
        }
      }

      row
    }

    def sumRevenues(row: UrlRow): UrlRow = {
      val rev = row.revs.values.sum
      //CalculateRevenueRow(row.Url, row.Date, row.tags, row.revs, rev, row.pic, row.pif, row.pi)
      row.rev = rev
      row
    }


    val calculated = data
      .map(calculateCampaign)
      .map(sumRevenues)

    time("calculate and save", {
      calculated.saveToCassandra("el_test", saveTable, SomeColumns("url", "date", "revs", "rev"))
    })

    sc.stop()
  }
}


/*

campaigns = [
    {
        'name': 'androidN',
        'tag': "channel:android",
        'budget': 100
    },
    {
        'name': 'caspersky',
        'tag': "channel:software",
        'budget': 150
    }
]

def calculate_campaign(row):
    pic = row['pi'] * row['pif']

    for campaign in campaigns:
        if campaign['tag'] in row['tags']:
            row['revs']['cp_%s' % (campaign['name'],)] = pic / pisPerTag[campaign['tag']] * campaign['budget']

    return row


def sum_revenues(row):
    row['rev'] = reduce(lambda s, i: s+i, row['revs'].itervalues())
    return row


*/