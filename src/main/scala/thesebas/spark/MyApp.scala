package thesebas.spark

import java.sql.Date

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

//import SparkContext._
case class MyRow(
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

    val readTable = ""
    val saveTable = ""




    val campaigns = List(
      Map(
        "name" -> "androidN",
        "tag" -> "chanel:android",
        "budget" -> 100f)
      ,
      Map(
        "name" -> "kaspersky",
        "tag" -> "chanel:software",
        "budget" -> 150f
      )
    )

    val pisPerTag = Map(
      "chanel:android" -> 1500,
      "chanel:software" -> 2500
    )

    val data = sc.cassandraTable[MyRow]("el_test", readTable)
      .select("Url", "Date", "tags", "revs", "rev", "pic", "pif", "pi")
      .where("Date = 2015-10-01")



    def calculateCampaign(row: MyRow): MyRow = {
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

    def sumRevenues(row: MyRow): MyRow = {
      val rev = row.revs.values.sum
      //MyRow(row.Url, row.Date, row.tags, row.revs, rev, row.pic, row.pif, row.pi)
      row.rev = rev
      row
    }


    val calculated = data
      .map(calculateCampaign)
      .map(sumRevenues)

    calculated.saveToCassandra("el_test", saveTable)

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