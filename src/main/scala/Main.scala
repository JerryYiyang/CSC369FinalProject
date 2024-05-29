package example
import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import scala.collection.immutable.List
import scala.util.Random

object App {

  import scala.io.Source
  import java.io._

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("WordAnalysis").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val filePath = "test.txt" //test path

    // Read the file contents as a a list of strings
    val fileList = Source.fromFile(filePath).getLines.toList


    val processedArticles = preprocess(fileList)

    val idf = top500(processedArticles)
    val result = processedArticles.map { article =>
      val tf = docTF(article)
      tfidf(tf, idf)
    }

  }

  def preprocess(fileList: List[String]): List[List[String]] = {
    val punctuation = List(".", ",", "!", "?", ":", ";", "/", '{', '}', '[', ']', '(', '"', ''')
    val pw = new PrintWriter(new File("hello.txt"))
    var pastFirst = false
    try {
      var totalList = List[List[String]]()
      var indexList = List[String]()
      for (i <- 0 until fileList.length) {
        val element = fileList(i)
        if (element.nonEmpty) {
          if ((!punctuation.contains(element.takeRight(1))) && (element.takeRight(1).head != '"' && element.takeRight(1).head != ''') && (element.head >= 'A' && element.head <= 'Z')) {
            if (pastFirst) {
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
//      for (j <- 0 until totalList.length) {
//        println("hit")
//        println(totalList(j))
//      }

      totalList
    }
  }

  // gets top 500 used words
  def top500(tokenizedList: List[List[String]]): List[(String, Double)] = {
    val allWords = tokenizedList.flatten
    val wordCounts = allWords.groupBy(x => x).mapValues(_.size.toDouble)
    wordCounts.toList.sortBy(-_._2).take(500)
  }

  // gets the tf of each doc
  def docTF(article: List[String]): List[(String, Double)] = {
    val counts = article.groupBy(x => x).mapValues(_.size).toList
    val totalWords = article.length.toDouble
    counts.map { case (word, count) => (word, count / totalWords) }
  }

  // calculates the tfidf
  def tfidf(docTF: List[(String, Double)], top500: List[(String, Double)]): List[Double] = {
    val idfMap = top500.toMap
    docTF.map { case (word, tf) =>
      val idf = idfMap.getOrElse(word, 0.0)
      tf * idf
    }
  }

  def dotProduct(v1: List[Double], v2: List[Double]): Double = {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException("Vectors must have the same length")
    }
    (v1 zip v2).map { case (x, y) => x * y }.sum
  }

  def magnitude(v: List[Double]): Double = {
    Math.sqrt(v.map(x => x * x).sum)
  }

  def getCosineDistance(v1: List[Double], v2: List[Double]): Double={
    1.0 - (dotProduct(v1, v2) /
      (magnitude(v1) * magnitude(v2)))
  }

  def greatestCosineDistance(referenceVector: List[Double], listOfVectors: List[List[Double]]): Double = {
    listOfVectors.map(vector => getCosineDistance(referenceVector, vector)).max
  }

  def averageVector(points: List[List[Double]]): List[Double] = {
    val dimensions = points.head.size
    val averages = (0 until dimensions).map(dim => points.map(_(dim)).sum / points.size).toList
    averages
  }

  def kMeansCluster(vectorList: List[List[Double]]): Unit = {
    val k = 20
    var hasClusterChange = true
    var centroids = Random.shuffle(vectorList).take(k)

    while(hasClusterChange){
      hasClusterChange = false
      val assignedClusters: List[Int] = vectorList.zipWithIndex.map { case (point, pointIndex) =>
        val closestCentroidIndex = centroids.zipWithIndex.minBy { case (centroid, _) => getCosineDistance(point, centroid) }._2
        val previousCluster = if (assignedClusters.nonEmpty) assignedClusters(pointIndex) else -1 // Default to -1 if no previous assignment
        if (closestCentroidIndex != previousCluster) {
          hasClusterChange = true
        }
        closestCentroidIndex
      }
      for (cluster <- 0 until k) {
        val assignedPoints = vectorList.filter(point => assignedClusters(vectorList.indexOf(point)) == cluster)
        if (assignedPoints.nonEmpty) {
          centroids = centroids.updated(cluster, averageVector(assignedPoints))
        }
      }

    }



  }
}

