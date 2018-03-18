import java.time.Instant

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object PostingTimeToScore {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("PostingTimeToScore").setMaster("local[1]")

    val reddit = RedditUtil.getRedditRDD(conf)

    allSubReddits(reddit)
    eachSubReddit(reddit)
  }

  def allSubReddits(reddit: RDD[RedditPost]): Unit = {
    reddit
      .map(post => {
        val postHour = Instant.ofEpochSecond(post.timeCreated.longValue()).toString.split("T")(1).split(":")(0).toInt

        (postHour, post.score)
      })
      .mapValues(value => (value, 1))
      .reduceByKey {case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)}
      .mapValues {case (sum, count) => sum / count}
      .reduceByKey({case (s1, s2) => s1 + s2})
      .sortByKey()
      .collect()
      .foreach(println)
  }

  def eachSubReddit(reddit: RDD[RedditPost]): Unit = {
    reddit
      .map(post => {
        val postHour = Instant.ofEpochSecond(post.timeCreated.longValue()).toString.split("T")(1).split(":")(0).toInt

        ((post.subreddit, postHour), 100000 * (1.0 * post.score) / post.subscribers)
      })
      .mapValues(value => (value, 1))
      .reduceByKey {case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)}
      .mapValues {case (sum, count) => sum / count}
      .map({case (nameHour, avgScore) => (nameHour._1, (nameHour._2, avgScore))})
      .reduceByKey {case ((h1, s1), (h2, s2)) => if (s1 > s2) (h1, s1) else (h2, s2)}
      .sortByKey()
      .takeOrdered(5)(Ordering[Double].on(_._2._2))
      .foreach(x => println(x._1))
  }
}
