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
    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)
    println("Hello World!");    //; is optional

    val filePath = "AllCombined.txt"

    // Read the file contents as a string
    val fileString = Source.fromFile(filePath).mkString

    val temp = preprocess(fileString)

  }

  def preprocess(fileString: String) = {



  }
}
