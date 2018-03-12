import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object SubUpvotes {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Driver").setMaster("local[4]")

    RedditUtil.getRedditRDD(conf)
      .map { redditPost: RedditPost =>
        (redditPost.subreddit, (redditPost.score.intValue(), 1))
      }.reduceByKey{ case
      ((x1, x2), (y1, y2)) => (x1 + y1, x2 + y2)
    }.sortByKey().collect().foreach { case (sub, (a, b)) =>
      println(sub, a / b.toFloat)
    }
  }

}
