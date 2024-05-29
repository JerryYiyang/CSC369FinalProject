package example
import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import scala.collection.immutable.List

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


    val processedArticles = preprocess(fileList)

    val idf = top500(sc, processedArticles)
    val result = processedArticles.map { article =>
      val tf = docTF(sc, article)
      tfidf(sc, tf, idf)
    }
    result.foreach(println)
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
  def top500(sc: SparkContext, tokenizedList: List[List[String]]): List[(String, Double)] = {
    val tokensRDD = sc.parallelize(tokenizedList)
    val allWordsRDD = tokensRDD.flatMap(identity)
    val wordCountsRDD = allWordsRDD.map((_, 1)).reduceByKey(_ + _)
    val top500Words = wordCountsRDD.mapValues(_.toDouble).sortBy(_._2, false).take(500)
    top500Words.toList
  }

  // gets the tf of each doc
  def docTF(sc: SparkContext, article: List[String]): RDD[(String, Double)] = {
    val doc = sc.parallelize(article)
    val wordCountsRDD = doc.map((_, 1)).reduceByKey(_ + _)
    val totalWords = article.length.toDouble
    val tfRDD = wordCountsRDD.mapValues(_.toDouble / totalWords)
    tfRDD
  }

  // calculates the tfidf
  def tfidf(sc: SparkContext, docTF: RDD[(String, Double)], top500: List[(String, Double)]): List[Double] = {
    val idfMap = sc.broadcast(top500.toMap)
    val result = docTF.map { case (word, tf) =>
      val idf = idfMap.value.getOrElse(word, 0.0)
      tf * idf
    }
    result.collect().toList
  }
}
