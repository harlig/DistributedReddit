import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object Collin1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Collin1").setMaster("local[4]")

    RedditUtil.getRedditRDD(conf).collect().foreach(println)
  }
}
