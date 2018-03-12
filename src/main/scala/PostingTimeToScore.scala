import java.time.Instant

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object PostingTimeToScore {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("PostingTimeToScore").setMaster("local[4]")

    val reddit = RedditUtil.getRedditRDD(conf)

    eachSubReddit(reddit)

  }

  def allSubReddits(reddit: RDD[RedditPost]): Unit = {
    reddit
      .map(post => {
        val postHour = Instant.ofEpochSecond(post.timeCreated.longValue()).toString.split("T")(1).split(":")(0).toInt

        (postHour, post.score)
      })
      .reduceByKey({case (s1, s2) => s1.intValue() + s2.intValue()})
      .sortByKey()
      .collect()
      .foreach(println)
  }

  def eachSubReddit(reddit: RDD[RedditPost]): Unit = {
    reddit
      .map(post => {
        val postHour = Instant.ofEpochSecond(post.timeCreated.longValue()).toString.split("T")(1).split(":")(0).toInt

        ((post.subreddit, postHour), post.score)
      })
      .reduceByKey({case (s1, s2) => s1.intValue() + s2.intValue()})
      .takeOrdered(1)(Ordering[Int].reverse.on(_._2.intValue()))
      .foreach(println)

  }
}
