import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class RedditPost(id: String, subreddit: String, score: Int, title: String, timeCreated: Double, numComments: Int, subscribers: Int) {

}

object RedditUtil {
  def getRedditRDD(conf: SparkConf): RDD[RedditPost] = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.textFile("data/reddit.csv")
      .mapPartitionsWithIndex((ndx, iter) => if (ndx == 0) iter.drop(1) else iter)
      .map(line => {
        val lineSplit = line.split(",").map(_.trim)

        println(lineSplit(0))

        val id = lineSplit(0)
        val subreddit = lineSplit(1)
        val score = lineSplit(2).toInt
        val title = lineSplit(3)
        val timeCreated = lineSplit(4).toDouble
        val numComments = lineSplit(5).toInt
        val subscribers = lineSplit(6).toInt

        RedditPost(id, subreddit, score, title, timeCreated, numComments, subscribers)
    })
  }
}
