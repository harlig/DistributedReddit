import java.time.Instant

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object Collin1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Collin1").setMaster("local[4]")

    val reddit = RedditUtil.getRedditRDD(conf)

    reddit.map(post => {
      val postHour = Instant.ofEpochSecond(post.timeCreated.longValue()).toString.split("T")(1).split(":")(0).toInt

      (postHour, post.score)
    }).reduceByKey{case (s1, s2) => s1.intValue() + s2.intValue()}
      .sortByKey()
      .collect()
      .foreach(println)
  }
}
