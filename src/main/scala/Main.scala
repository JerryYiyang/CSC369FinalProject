package example
import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object App {
  import scala.io.Source
  import java.io._
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("WordAnalysis").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val filePath = "test.txt" //test path

    // Read the file contents as a a list of strings
    val fileList = Source.fromFile(filePath).getLines.toList


    preprocess(fileList)


  }

  def preprocess(fileList: List[String]): List[List[String]] = {
    val punctuation = List(".",",","!","?",":",";","/",'{','}','[',']','(','"',''')
    val pw = new PrintWriter(new File("hello.txt" ))
    var pastFirst = false
    try {
      var totalList = List[List[String]]()
      var indexList = List[String]()
      for (i <- 0 until fileList.length) {
        val element = fileList(i)
        if (element.nonEmpty){
          if((!punctuation.contains(element.takeRight(1)))&& (element.takeRight(1).head != '"' && element.takeRight(1).head != ''') && (element.head >= 'A' && element.head <= 'Z')){
            if(pastFirst) {
              totalList = totalList :+ indexList
              indexList = List()
            }
            indexList = indexList :+ element
            pastFirst = true
          } else {
            indexList = indexList ++ element.split("""\s|\.""").toList
          }

          //println(totalList.toString())
          }
        }
      for (j <- 0 until totalList.length) {
        println("hit")
        println(totalList(j))
      }

      totalList



    }


  }

  // gets top 500 used words
  def wordCount(tokenizedList: List[List[String]]): List[(String, Int)] = {
    val allWords = tokenizedList.flatten
    val wordCounts = allWords.groupBy(x => x).mapValues(_.size)
    wordCounts.toList.sortBy(-_._2).take(500)
  }
}
