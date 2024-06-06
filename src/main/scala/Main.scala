package example
import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import java.io.PrintWriter
import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer
import scala.util.Random

object App {

  import scala.io.Source
  import java.io._

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("WordAnalysis").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val filePath = "AllCombined.txt" //test path
    val k = 100

    // Read the file contents as a a list of strings
    val fileList = Source.fromFile(filePath).getLines.toList


    val processedArticles = preprocess(fileList)
    println("finished preprocess")
    //processedArticles.foreach(println)
    val idf = topWords(sc, processedArticles, 2000)
    println("finished idf")
    val result = processedArticles.map { article =>
      val tf = docTF(sc, article)
      tfidf(sc, tf, idf, 2000)
    } //(Vector, ArticleName)
    println("finished tfidf")
    val clusterIndexes = kMeansCluster(sc, k, result)
    //val articleNames = getArticleNames(processedArticles)
    //clusterPrinter(clusterIndexes, articleNames,k)
  }

  def preprocess(fileList: List[String]): List[List[String]] = {
    val stopWords = List("a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
      "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to",
      "was", "will", "wit")
    var counter = 0
    var totalList = List[List[String]]()
    var currentArticle = ListBuffer[String]()
    for (element <- fileList) {
      if (element.trim.nonEmpty) {
        val isTitle = element.head.isUpper && element.split("\\s+").length <= 5 && !element.exists(
          ch => ch == '-' || ch == ':' || ch == '>' || ch == '<' || ch == "." || ch == "_" || ch == "(" || ch == ")"
          || ch == "!" || ch == "?" || ch == "/" || ch == "=" || ch == "," || ch == "^"|| ch == "=" || ch == "*")
        if (isTitle) {
          if (currentArticle.nonEmpty) {
            totalList = totalList :+ currentArticle.toList
            counter += 1
            if(counter >= 5000){return totalList}
            currentArticle.clear()
          }
          currentArticle += element.toLowerCase
        } else {
          val processedWords = element.toLowerCase.split("\\s+")
            .flatMap(word => word.split("""[\.\,\!\?\:\;\/\{\}\[\]\(\)"']"""))
            .filter(word => word.nonEmpty && !stopWords.contains(word))
          currentArticle ++= processedWords
        }
      }
    }
    if (currentArticle.nonEmpty) {
      totalList = totalList :+ currentArticle.toList
    }
    totalList = totalList.filter(_.nonEmpty)
    totalList
  }




  // gets top n used words
  def topWords(sc: SparkContext, tokenizedList: List[List[String]], n: Int): List[(String, Double)] = {
    val tokensRDD = sc.parallelize(tokenizedList)
    val allWordsRDD = tokensRDD.flatMap(identity)
    val wordCountsRDD = allWordsRDD.map(x => (x, 1)).reduceByKey(_ + _)
    val top500Words = wordCountsRDD.mapValues(_.toDouble).sortBy(_._2, false).take(n)
    top500Words.toList
  }

  // gets the tf of an article
  def docTF(sc: SparkContext, article: List[String]): (String, List[(String, Double)]) = {
    val doc = sc.parallelize(article)
    val wordCountsRDD = doc.map((_, 1)).reduceByKey(_ + _)
    val totalWords = article.length.toDouble
    val tfRDD = wordCountsRDD.mapValues(_.toDouble / totalWords).collect.toList
    (article(0), tfRDD)
  }

  def tfidf(sc: SparkContext, docTF: (String, List[(String, Double)]), top: List[(String, Double)], n: Int): (List[Double], String) = {
    val idfMap = sc.broadcast(top.toMap)
    val tfVals = docTF._2
    val articleName = docTF._1
    val TF = sc.parallelize(tfVals)
    val result = TF.map { case (word, tf) =>
      val idf = idfMap.value.getOrElse(word, 0.0)
      tf * idf
    }.collect().toList
    val res = result.padTo(n, 0.0).take(n)
    (res, articleName)
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



  def kMeansCluster(sc: SparkContext, k: Int, vectors: List[(List[Double], String)]) = {
    var hasClusterChange = true
    val vectorNames = sc.parallelize(vectors) //(Vector, Name)
    val vectorList = vectors.map { case (v, _) => v }
    val centroids = Random.shuffle(vectorList).take(k).toBuffer
    val pointZip = sc.parallelize(vectorList).zipWithIndex().map(x => (x._1, x._2)) //[(Vector, Index)]
    var i = 0
    do {
      if(i % 5 == 0){println(i)}
      i= i+ 1
      hasClusterChange = false
      val centroidsRDD = sc.parallelize(centroids)

      // Collect assignedClusters after the loop for final assignments

      val centroidZip = centroidsRDD.zipWithIndex().map(x => (x._1, x._2)) //[(Vector, Index)]
      //println(pointZip.collect().toList.length)
      //println(centroidZip.collect().toList(0)._1.length)

      val assignedClusters = pointZip.cartesian(centroidZip).map(x => (x._1._2, (getCosineDistance(x._1._1, x._2._1), x._2._2, x._1._1))).groupByKey().map({ case (key, buffer) =>
        //(pointIndex, (cosineDistance, clusterIndex))
        val (closestCluster, pointVector) = (buffer.toList.sortBy(_._1).head._2, buffer.toList.sortBy(_._1).head._3) // Convert to list and sort by cosine distance
        (closestCluster, pointVector)
      }) // (clusterNum, pointVector)

      val newCentroidPoints = assignedClusters.groupByKey().map { case (key, vectors) =>
        // Calculate the average of each element position across all vectors
        val vectorLength = vectors.head.length // Assuming all vectors have the same length
        val averages = (0 until vectorLength).map { i =>
          vectors.map(_(i)).sum / vectors.size.toDouble // Average each element position
        }
        (key.toInt, averages.toList) // Convert averages to list
      }.collect().toList
      newCentroidPoints.foreach({ case (key, vector) => if (centroids(key) != vector) {
        hasClusterChange = true
        //println(key)
        //println(getCosineDistance(centroids(key),vector))
        centroids(key) = vector
      }})
    } while (hasClusterChange)

    val centroidsRDD = sc.parallelize(centroids)
    val centroidZip = centroidsRDD.zipWithIndex().map(x => (x._1, x._2)) //[(Vector, Index)]

    val assignedClusters = pointZip.cartesian(centroidZip).map(x => (x._1._2, (getCosineDistance(x._1._1, x._2._1), x._2._2, x._1._2, x._1._1))).groupByKey().map({ case (key, buffer) =>
      //(pointIndex, (cosineDistance, clusterIndex))
      val (closestCluster, pointVector) = (buffer.toList.sortBy(_._1).head._2, buffer.toList.sortBy(_._1).head._4) // Convert to list and sort by cosine distance
      (pointVector, closestCluster)
    })
    val output = assignedClusters.join(vectorNames).map(x=>x._2).reduceByKey(_ + ", " + _).sortByKey().collect()
    val pw = new PrintWriter(new java.io.File("output.txt"))
    output.foreach(line => pw.println(line))
    pw.close()
  }
}

