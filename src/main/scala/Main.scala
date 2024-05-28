import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object App {
  import scala.io.Source
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("WordAnalysis").setMaster("local[4]")
    val sc = new SparkContext(conf)
    println("Hello World!");    //; is optional

    val filePath = "test.txt"

    // Read the file contents as a string
    val fileString = Source.fromFile(filePath).mkString

    val temp = preprocess(fileString)


  }

  def preprocess(fileString: String) = {



  }

  // gets top 500 used words
  def wordCount(tokenizedList: List[List[String]]): List[(String, Int)] = {
    val allWords = tokenizedList.flatten
    val wordCounts = allWords.groupBy(x => x).mapValues(_.size)
    wordCounts.toList.sortBy(-_._2).take(500)
  }
}
