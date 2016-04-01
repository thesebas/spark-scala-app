package thesebas.spark

import java.sql.Date

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD

//import SparkContext._
case class UrlRow(
                   Url: String,
                   Date: Date,
                   tags: Set[String],
                   var revs: Map[String, Float],
                   var rev: Float,
                   pic: Float,
                   pif: Float,
                   pi: Int
                 )

/**
  * Created by thesebas on 2016-03-30.
  */
object MyApp {
  val AppName = "MyApp"

  def execute(master: Option[String], args: List[String], jars: Seq[String] = Nil): Unit = {


    val sc = {
      val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "10.0.40.42")
        .setAppName(AppName)

      if (master.isDefined) conf.setMaster(master.get)
      new SparkContext(conf)

    }

    val readTable = if (args.isDefinedAt(0)) args.head else "cockpit2_testTogether"
    val saveTable = if (args.isDefinedAt(1)) args(1) else readTable

    Console.println(s"reading from $readTable and writing back to $saveTable")


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

    def mapRowTagsToPis(row: UrlRow): List[(String, Int)] = {
      if (row.tags.isEmpty) {
        return List(("invalid", 1))
      }
      (for (tag <- row.tags) yield (tag, 1)).toList

    }
    def mapTagsToPis(tags: Set[String]): List[(String, Int)] = {
      if (tags.isEmpty) {
        return List(("invalid", 1))
      }
      (for (tag <- tags) yield (tag, 1)).toList

    }


    val pisPerTagRDD = sc.cassandraTable[(Set[String])]("el_test", readTable)
      .select("tags")
      .where("date = ?", "2015-10-01")
      .flatMap(mapTagsToPis)
      .reduceByKey(_ + _)

    //    pisPerTagRDD.saveAsTextFile("/www/ssz/pisperTag")

    val pisPerTag = pisPerTagRDD.collectAsMap()

    Console.println(pisPerTag)

    //    val pisPerTag = Map(
    //      "chanel:android" -> 1500,
    //      "chanel:software" -> 2500
    //    )


    val data = readTableRDD
      .select("url", "date", "tags", "revs", "rev", "pic", "pif", "pi")
      .where("date = ?", "2015-10-01")



    def calculateCampaign(row: UrlRow): UrlRow = {
      val pic = row.pi * row.pif

      for (campaign <- campaigns) {
        val campaignTag = campaign("tag").asInstanceOf[String]
        if (row.tags.contains(campaignTag)) {
          val rev = pic / pisPerTag(campaignTag) * campaign("budget").asInstanceOf[Float]
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

    calculated.saveToCassandra("el_test", saveTable, SomeColumns("url", "date", "revs", "rev"))

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