import java.time.Instant

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object BestTitleLength {
  val before = Instant.now()
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("PostingTimeToScore").setMaster("local[4]")

    val reddit = RedditUtil.getRedditRDD(conf)
    allSubs(reddit)
    println(Instant.now().toEpochMilli - before.toEpochMilli)
  }

  def allSubs(rdd: RDD[RedditPost]): Unit = {
    rdd
      .map(post => {
        (post.title.length, (post.score , 1))
      })
      .reduceByKey {case ((score1, i), (score2, i2)) => (score1+score2, i+i2)}
      .map{ case (length, score) => (score._1/score._2, length)}
      .sortByKey(false)
      .collect()
      .foreach(println)
  }
  def perSubComments(rdd: RDD[RedditPost]): Unit = {
    rdd
      .map { post: RedditPost =>
        ((post.subreddit, post.title.length), post.numComments)
      }
      .reduceByKey{ case ((x1), (x2)) => x1 + x2}
      .map { case ((sub, length), score) => (sub, (score, length))}
      .reduceByKey{ case ((score1, length1), (score2, length2)) =>
        if (score1 > score2)
          (score1, length1)
        else
          (score2, length2)}
      .map { case (sub, (score, length)) => (score, (sub, length))}
      .sortByKey(false)
      .collect()
      .foreach{ case (score, (sub, length)) => println(sub + ", " + length + ": " + score)}
  }
}