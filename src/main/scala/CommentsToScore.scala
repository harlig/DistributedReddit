import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

import scala.math.round

object CommentsToScore {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val commentBucketSize = 250

    val conf = new SparkConf().setAppName("PostingTimeToScore").setMaster("local[4]")

    val reddit = RedditUtil.getRedditRDD(conf)

    reddit.map(post => {
      (round(post.numComments.intValue() / commentBucketSize * 1.0) * commentBucketSize, post.score)
    }).reduceByKey{case (s1, s2) => s1.intValue() + s2.intValue()}
      .sortByKey()
      .collect()
      .foreach(println)
  }
}
