import java.time.Instant;
import java.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SubUpvotes {
  def main(args: Array[String]): Unit = {
    val before = Instant.now()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Driver").setMaster("local[1]")

    val rdd = RedditUtil.getRedditRDD(conf)

//    allSubs(rdd)

    weighted(rdd)

    println(Instant.now().toEpochMilli - before.toEpochMilli)
  }

  // How many average upvotes you get in each sub
  def allSubs(rdd: RDD[RedditPost]): Unit = {
    rdd.map { redditPost: RedditPost =>
      (redditPost.subreddit, (redditPost.score, 1))
    }.reduceByKey{ case
      ((x1, x2), (y1, y2)) => (x1 + y1, x2 + y2)
    }.map { case (k, (a, b)) =>
      (a / b.toFloat, k)
    }.sortByKey().collect().foreach { case (votes, sub) =>
      println(sub, votes)
    }
  }

  // How many average upvotes you get in each subreddit if it has 100k subs
  def weighted(rdd: RDD[RedditPost]): Unit = {
    rdd.map { redditPost: RedditPost =>
      (redditPost.subreddit , (redditPost.score, 1, redditPost.subscribers))
    }.reduceByKey{ case
      ((x1, x2, x3), (y1, y2, y3)) => (x1 + y1, x2 + y2, x3)
    }.map{ case (k, (v1, v2, v3)) =>
      (100000 * ((v1 / v2.toFloat) / v3.toFloat), k)
    }.sortByKey().collect().foreach(println)

  }
}
