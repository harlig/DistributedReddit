import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class RedditPost(id: String, subreddit: String, score: Number, title: String, timeCreated: Number, numComments: Number) {

}

object RedditUtil {
  def getRedditRDD(conf: SparkConf): RDD[RedditPost] = {
    val sc = new SparkContext(conf)
    sc.textFile("data/reddit.csv")
      .mapPartitionsWithIndex((ndx, iter) => if (ndx == 0) iter.drop(1) else iter)
      .map(line => {
        val lineSplit = line.split(",").map(_.trim)

        val id = lineSplit(0)
        val subreddit = lineSplit(1)
        val score = lineSplit(2).toInt
        val title = lineSplit(3)
        val timeCreated = lineSplit(4).toDouble
        val numComments = lineSplit(5).toInt

        RedditPost(id, subreddit, score, title, timeCreated, numComments)
    })
  }
}
