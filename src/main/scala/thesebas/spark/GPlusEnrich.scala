package thesebas.spark

import java.sql.Date

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import dispatch._
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import scala.concurrent.duration._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

case class Config(input: Option[String] = None, output: Option[String] = None, gpKey: Option[String] = None)

/**
  * Created by thesebas on 2016-04-14.
  */
object GPlusEnrich {

  val AppName = "GPlusEnrich"

  val logger = Logger.getLogger(AppName)


  def main(args: Array[String]): Unit = {

    GPlusEnrich.execute(
      master = None, // Some("spark://127.0.0.1:7077"),
      args = args.toList,
      jars = List(SparkContext.jarOfObject(this).get)
    )
  }

  def time[R](label: String, block: => R): R = {
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    logger.info(s"TIMER: [$label] elapsed time ${(end - start) / 1e6}")
    result
  }


  def getSharesForUrl(url: String, key: String): Option[Double] = {

    val doc = List(
      ("method" -> "pos.plusones.get") ~
        ("id" -> "p") ~
        ("params" ->
          ("nolog" -> true) ~
            ("id" -> url) ~
            ("source" -> "widget") ~
            ("userId" -> "@viewer") ~
            ("groupId" -> "@self")

          ) ~
        ("jsonrpc" -> "2.0") ~
        ("key" -> "p") ~
        ("apiVersion" -> "v1")
    )

    implicit val formats: Formats = org.json4s.DefaultFormats

    val jsonbody = pretty(render(doc))

    //    val svc = dispatch.url("https://clients6.google.com/rpc")
    //      .addQueryParameter("key", key)
    //      .setMethod("POST")
    //      .setBody(jsonbody)
    //      .setContentType("application/json", "utf-8")

    val svc = dispatch.host("clients6.google.com").secure / "rpc" <<? Map("key" -> key) << jsonbody setContentType("application/json", "utf-8")

    logger.info(svc.url)

    val counts = dispatch.Http(svc.POST OK as.String)

    //    counts.value match {
    //      case Some(x) => x match {
    //        case Success(response) => {
    //          List()
    //        }
    //        case Failure(exception) => List()
    //      }
    //      case None => List()
    //    }

    val timeout = 10.seconds
    try {
      val sBody = Await.result(counts, timeout)
      logger.info(sBody)
      val response: JValue = parse(StringInput(sBody))
      //    $body[0]['result']['metadata']['globalCounts']['count']

      try {
        (response(0) \ "result" \ "metadata" \ "globalCounts" \ "count").extractOpt[Double]
      } catch {
        case nse: NoSuchElementException => None
      }
    } catch {
      case timeout: TimeoutException => throw timeout
    }
  }


  def execute(master: Option[String], args: List[String], jars: Seq[String] = Nil): Unit = {

    val sc = {
      val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "10.0.40.42")
        .setAppName(AppName)

      new SparkContext(conf)

    }

    val parser = new scopt.OptionParser[Config]("gplusenrich") {
      head("gplusenrich")
      opt[String]('i', "input") action { (x, c) => c.copy(input = Some(x)) }
      opt[String]('o', "output") action { (x, c) => c.copy(output = Some(x)) }
      opt[String]('k', "gpkey") required() action { (x, c) => c.copy(gpKey = Some(x)) }
    }

    val opts = parser.parse(args, Config()) match {
      case Some(config) => config
      case None => throw new Exception("wrong params")
    }

    val readTable = opts.input.getOrElse("cockpit2_testTogether")
    val saveTable = opts.output.getOrElse(readTable)

    val GPLUSKEY = opts.gpKey.get

    val processedDay = "2015-10-01"
    val processedToDay = "2015-10-10"

    logger.info(s"reading from $readTable and writing back to $saveTable, date range:[$processedDay:$processedToDay], gpkey: $GPLUSKEY")

    val readTableRDD: CassandraTableScanRDD[(String, Date, Map[String, Int], Set[String])] = sc.cassandraTable[(String, Date, Map[String, Int], Set[String])]("el_test", readTable)

    val data = readTableRDD
      .select("url", "date", "counts", "tags")
      .where("date = ?", processedToDay)
      .limit(10)

    logger.info(data.count())

    data.flatMap {
      case (url: String, date: Date, counts: Map[String, Int], tags: Set[String]) =>
        val result = time(s"getSharesForUrl($url)", {
          getSharesForUrl(url, GPLUSKEY) match {
            case Some(count: Double) =>
              val updatedCounts = counts ++ Map("gp_shares" -> count)
              List((url, date, updatedCounts, tags))
            case None =>
              List()
          }
        })
        logger.info(result)
        result
    }
      .map {
        case (url: String, date: Date, counts: Map[String, Int], tags: Set[String]) => {
          val updatedTags = tags ++ Set("gpenriched:v3")
          logger.info(s"($url, $date, $counts, $updatedTags)")
          (url, date, counts, updatedTags)
        }

      }
    //    data.take(5).foreach { case (url: String, date: Date, counts: Map[String, Int], tags: Set[String]) => println(url, date, counts("gp_shares"), tags) }

    data.saveToCassandra("el_test", saveTable, SomeColumns("url", "date", "counts", "tags"))
  }
}
