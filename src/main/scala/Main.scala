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
    val k = 20

    // Read the file contents as a a list of strings
    val fileList = Source.fromFile(filePath).getLines.toList


    val processedArticles = preprocess(fileList)

    val idf = top500(sc, processedArticles)
    val result = processedArticles.map { article =>
      val tf = docTF(sc, article)
      tfidf(sc, tf, idf)
    }
    val clusterIndexes = kMeansCluster(sc, k, result)
    //val articleNames = getArticleNames(processedArticles)
    //clusterPrinter(clusterIndexes, articleNames,k)
  }

  def getArticleNames(allData: List[List[String]]): List[String]={
    allData.map(x => x(1))
  }

  def clusterPrinter(clusterIndexes: List[Int], articles: List[String], k: Int): Unit={
    if (clusterIndexes.length != articles.length) {
      throw new IllegalArgumentException("Input lists must have the same length")
    }

    // Use zip to combine elements with the same index
    val zippedData = clusterIndexes.zip(articles)

    // Group data by the integer using groupBy
    val groupedByInt = zippedData.groupBy { case (int, _) => int % k }

    // Map each group to a list of strings
    groupedByInt.map { case (_, group) => group.map(_._2).toList }.toList.foreach { group =>
      println(group.mkString(", "))
    }


  }

  def preprocess(fileList: List[String]): List[List[String]] = {
    val punctuation = List(".", ",", "!", "?", ":", ";", "/", '{', '}', '[', ']', '(', '"', "'")
    var totalList = List[List[String]]()
    var indexList = List[String]()
    var pastFirst = false

    for (element <- fileList) {
      if (element.nonEmpty) {
        if ((!punctuation.contains(element.takeRight(1))) &&
          (element.takeRight(1).head != '"' && element.takeRight(1).head != '\'') &&
          (element.head >= 'A' && element.head <= 'Z')) {
          if (pastFirst) {
            totalList = totalList :+ indexList
            indexList = List()
          }
          indexList = indexList :+ element.toLowerCase
          pastFirst = true
        } else {
          indexList = indexList ++ element.toLowerCase.split("""[\s\.]""").filter(_.nonEmpty).toList
        }
      }
    }
    if (indexList.nonEmpty) {
      totalList = totalList :+ indexList
    }
    totalList
  }

  // gets top 500 used words
  def top500(sc: SparkContext, tokenizedList: List[List[String]]): List[(String, Double)] = {
    val tokensRDD = sc.parallelize(tokenizedList)
    val allWordsRDD = tokensRDD.flatMap(identity)
    val wordCountsRDD = allWordsRDD.map(x => (x, 1)).reduceByKey(_ + _)
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

  def tfidf(sc: SparkContext, docTF: RDD[(String, Double)], top500: List[(String, Double)]): List[Double] = {
    val idfMap = sc.broadcast(top500.toMap)
    val result = docTF.map { case (word, tf) =>
      val idf = idfMap.value.getOrElse(word, 0.0)
      tf * idf
    }.collect().toList
    result.padTo(500, 0.0)
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

  def kMeansCluster(sc: SparkContext, k: Int, vectorList: List[List[Double]]) = {
    var hasClusterChange = true
    val centroids = sc.parallelize(Random.shuffle(vectorList).take(k))
    var assignedClusters = List[Unit]()

    do {
      hasClusterChange = false

      // Collect assignedClusters after the loop for final assignments
      val pointZip = sc.parallelize(vectorList).zipWithIndex().map(x => (x._1, x._2)) //[(Vector, Index)]
      val centroidZip = centroids.zipWithIndex().map(x => (x._1, x._2)) //[(Vector, Index)]
      println(pointZip.collect().toList(0)._1.length)
      println(centroidZip.collect().toList(0)._1.length)

      //pointZip.cartesian(centroidZip).map(x => (x._1._2, (getCosineDistance(x._1._1, x._2._1), x._2._2))).groupByKey().sortBy(x => x._2).foreach(println(_))

//      assignedClusters = sc.parallelize(vectorList).zipWithIndex().map({ case (point, pointIndex) =>  // [article1,
//        val closestCentroidIndex = centroids.zipWithIndex().map({ case (centroid, centroidIndex) => (getCosineDistance(point, centroid), centroidIndex)}).collect().foreach(println(_))
//
////        val previousCluster = if (assignedClusters.nonEmpty) assignedClusters(pointIndex.toInt) else -1 // Default to -1 if no previous assignment
////        if (closestCentroidIndex != previousCluster) {
////          hasClusterChange = true
////        }
//        closestCentroidIndex
//      }).collect().toList

//      for (cluster <- 0 until k) {
//        val assignedPoints = sc.parallelize(vectorList).filter(point => assignedClusters(vectorList.indexOf(point)) == cluster).collect().toList
//        if (assignedPoints.nonEmpty) {
//          centroids = centroids.updated(cluster, averageVector(assignedPoints))
//        }
//      }
    } while (hasClusterChange)

    // Return assignedClusters after the loop completes
    //assignedClusters
  }

}

