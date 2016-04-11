package thesebas.spark

import java.sql.Date

/**
  * Created by thesebas on 2016-04-08.
  */
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
