import java.time.Instant

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object PostingTimeComments {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Driver").setMaster("local[4]")
    val reddit = RedditUtil.getRedditRDD(conf)

    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/reddit.csv")

    lines.map(
      line =>
        line.split(",").map(
          s =>
            s.trim()
        )
    ).map(
      spl =>
        (spl(4).toFloat, spl(5).toInt)
    ).map { case (k, v) =>
      val hr = Instant.ofEpochSecond(k.toInt).toString.split("T")(1).split(":")(0).toInt
      (hr, (v, 1))
    }.reduceByKey{ case
      ((x1, x2), (y1, y2)) => (x1 + y1, x2 + y2)
    }.sortByKey().collect().foreach { case (hr, (a, b)) =>
      println(hr, a / b.toFloat)
    }
  }

}
