import java.time.Instant

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object PostingTimeComments {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Driver").setMaster("local[4]")
    val reddit = RedditUtil.getRedditRDD(conf)

    val rdd = RedditUtil.getRedditRDD(conf)

//    allSubs(rdd)

    perSub(rdd)
  }

  def allSubs(rdd: RDD[RedditPost]): Unit = {
    rdd
      .map { redditPost: RedditPost =>
        val hr = Instant.ofEpochSecond(redditPost.timeCreated.longValue()).toString.split("T")(1).split(":")(0).toInt

        (hr, (redditPost.numComments.intValue(), 1))
      }.reduceByKey{ case
      ((x1, x2), (y1, y2)) => (x1 + y1, x2 + y2)
    }.sortByKey().collect().foreach { case (hr, (a, b)) =>
      println(hr, a / b.toFloat)
    }
  }

  def perSub(rdd: RDD[RedditPost]): Unit = {
    rdd
      .map { redditPost: RedditPost =>
        val hr = Instant.ofEpochSecond(redditPost.timeCreated.longValue()).toString.split("T")(1).split(":")(0).toInt

        (redditPost.subreddit + "," + hr.toString, (redditPost.numComments.intValue(), 1))
      }.reduceByKey{ case
      ((x1, x2), (y1, y2)) => (x1 + y1, x2 + y2)
    }.sortBy(a => (a._1.split(",")(0), a._1.split(",")(1).toInt)).collect().foreach { case (k, (a, b)) =>
      val spl = k.split(",")
      println(spl(0), spl(1), a / b.toFloat)
    }
  }

}
