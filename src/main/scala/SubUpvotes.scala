import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SubUpvotes {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Driver").setMaster("local[4]")

    val rdd = RedditUtil.getRedditRDD(conf)

    allSubs(rdd)

    weighted(rdd)
  }

  // How many average upvotes you get in each sub
  def allSubs(rdd: RDD[RedditPost]): Unit = {
    rdd.map { redditPost: RedditPost =>
      (redditPost.subreddit, (redditPost.score, 1))
    }.reduceByKey{ case
      ((x1, x2), (y1, y2)) => (x1 + y1, x2 + y2)
    }.sortByKey().collect().foreach { case (sub, (a, b)) =>
      println(sub, a / b.toFloat)
    }
  }

  // How many average upvotes you get in each subreddit if it has 100k subs
  def weighted(rdd: RDD[RedditPost]): Unit = {
    rdd.map { redditPost: RedditPost =>
      (redditPost.subreddit + "," + redditPost.subscribers, (redditPost.score, 1))
    }.reduceByKey{ case
      ((x1, x2), (y1, y2)) => (x1 + y1, x2 + y2)
    }.map{ case (k, (v1, v2)) =>
        val spl = k.split(",")
      (spl(0), 100000 * ((v1 / v2.toFloat) / spl(1).toFloat))
    }.sortByKey().collect().foreach(println)
  }

}
