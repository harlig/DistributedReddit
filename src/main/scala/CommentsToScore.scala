import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object CommentsToScore {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("PostingTimeToScore").setMaster("local[4]")

    val reddit = RedditUtil.getRedditRDD(conf)

    reddit
      .map(post => {
        (post.subreddit, post.score.intValue() / post.numComments.intValue())
      })
      .mapValues(value => (value, 1))
      .reduceByKey {case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)}
      .mapValues {case (sum, count) => sum / count}
      .sortByKey()
      .collect()
      .foreach(println)
  }
}
