import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object BestTitleLength {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("PostingTimeToScore").setMaster("local[4]")

    val reddit = RedditUtil.getRedditRDD(conf)
    allSubs(reddit)
  }

  def allSubs(rdd: RDD[RedditPost]): Unit = {
    rdd
      .map(post => {
        (post.title.length, post.score)
      })
      .reduceByKey {case (score1, score2) => score1+score2}
      .map{ case (length, score) => (score, length)}
      .sortByKey(false)
      .collect()
      .take(1)
      .foreach(println)
  }
}