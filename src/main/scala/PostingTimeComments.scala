import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object PostingTimeComments {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Driver").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/reddit.csv")

    var minComments = Integer.MAX_VALUE
    var maxComments = 0

    lines.map(
      line =>
        line.split(",").map(
        s =>
          s.trim()
      )
    ).map(
      spl =>
        (Integer.parseInt(spl(4)), Integer.parseInt(spl(5)))
    ).map{ case (com, time) =>

        if (com > maxComments) {
          maxComments = com
        }
        if (com < minComments) {
          minComments = com
        }

//      println(new Date(time))
      (com, time)
    }.foreach(println)
  }

}
