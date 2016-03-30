package thesebas.spark

import org.apache.spark._

object MyAppJob {
  def main(args: Array[String]): Unit = {

    MyApp.execute(
      master = Some("spark://127.0.0.1:7077"),
      args = args.toList,
      jars = List(SparkContext.jarOfObject(this).get)
    )
  }
}