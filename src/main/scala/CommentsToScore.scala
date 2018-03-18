import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object CommentsToScore {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("CommentsToScore").setMaster("local[4]")

    val reddit = RedditUtil.getRedditRDD(conf)

    reddit
      .filter(post => {
        post.numComments > 0
      })
      .map(post => {
        (post.subreddit, (1.0 * post.score) / post.numComments)
      })
      .mapValues(value => (value, 1))
      .reduceByKey {case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)}
      .mapValues {case (sum, count) => sum / count}
      .sortByKey()
      .takeOrdered(3)(Ordering[Double].on(_._2))
      .foreach(println)
  }
}
