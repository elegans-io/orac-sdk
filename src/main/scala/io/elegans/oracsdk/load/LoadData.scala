package io.elegans.oracsdk.load

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import io.elegans.orac.entities._
import io.elegans.orac.serializers.OracJsonSupport
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.io.Source
import scala.util.{Failure, Success}
import scala.util.Random

case class LoadDataException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

/** load data from files and serialization of data
  */
object LoadData extends OracJsonSupport with java.io.Serializable {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  /** load Actions into an RDD
    *
    * @param path : path of the file with actions
    * @param sc : spark execution context
    * @return : an RDD of Action objects
    */
  def actions(path: String, sc: SparkContext): RDD[Action] = {
    val rdd: RDD[Action] = sc.textFile(path).map(_.trim).map(item => {
      Await.ready(Unmarshal(item).to[Action], 2.seconds).value
        .getOrElse(Failure(throw LoadDataException("Empty result unmarshalling entity")))
    }).map {
      case Success(value) => value
      case Failure(e) => throw LoadDataException("Error unmarshalling entity", e)
    }
    rdd
  }

  /** load OracUser into an RDD
    *
    * @param path : path of the file with orac users
    * @param sc : spark execution context
    * @return : an RDD of OracUser objects
    */
  def oracUsers(path: String, sc: SparkContext): RDD[OracUser] = {
    val rdd: RDD[OracUser] = sc.textFile(path).map(_.trim).map(item => {
      Await.ready(Unmarshal(item).to[OracUser], 2.seconds).value
        .getOrElse(Failure(throw LoadDataException("Empty result unmarshalling entity")))
    }).map {
      case Success(value) => value
      case Failure(e) => throw LoadDataException("Error unmarshalling entity", e)
    }
    rdd
  }

  /** load Item into an RDD
    *
    * @param path : path of the file with items
    * @param sc : spark execution context
    * @return : an RDD of Item objects
    */
  def items(path: String, sc: SparkContext): RDD[Item] = {
    val rdd: RDD[Item] = sc.textFile(path).map(_.trim).map(item => {
      Await.ready(Unmarshal(item).to[Item], 2.seconds).value
        .getOrElse(Failure(throw LoadDataException("Empty result unmarshalling entity")))
    }).map {
      case Success(value) => value
      case Failure(e) => throw LoadDataException("Error unmarshalling entity", e)
    }
    rdd
  }

  /** load LRR recommendations into an RDD
    *
    * @param recommPath : path of the file with the recommendations
    * @param userIdMappingPath : path of the file with userId => Long mapping
    * @param itemIdMappingPath : path of the file with itemId => Long mapping
    * @param spark : spark session
    * @return : an RDD of Recommendation objects
    */
  def llrRecommendations(recommPath: String, userIdMappingPath: String,
                         itemIdMappingPath: String, spark: SparkSession): RDD[Recommendation] = {

    import spark.implicits._

    spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .load(recommPath)
      .map(entry =>
        (entry(0).asInstanceOf[String].toLong,
          entry(1).asInstanceOf[String].replace("[", "").replace("]", "").split(","))
      ).map(entry => entry._2.map { case(item) =>
      val elements = item.split(":")
      val itemId: Long = elements(0).toLong
      val score: Double = elements(1).toDouble
      (entry._1, itemId, score)
    }).flatMap(x => x).createOrReplaceTempView("recomm")

    spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .load(userIdMappingPath)
      .createOrReplaceTempView("userId")

    spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .load(userIdMappingPath)
      .createOrReplaceTempView("itemId")

    val random: Random.type = scala.util.Random
    val generationTimestamp = Instant.now().toEpochMilli
    val generationBatch: String = generationTimestamp + "_" + math.abs(random.nextLong())

    val recommendations =
      spark.sql("select userId._c0, itemId._c0, recomm._3 from recomm join userId, " +
        "itemId where recomm._1 = userId._c1 AND recomm._2 = itemId._c1")
      .map(entry =>
        (entry(0).asInstanceOf[String], entry(1).asInstanceOf[String], entry(2).asInstanceOf[Double])).rdd
      .map(recomm => {
        Recommendation(
          id = None,
          user_id = recomm._1,
          item_id = recomm._2,
          name = "rate",
          algorithm = "COO-LLR",
          generation_batch = generationBatch,
          generation_timestamp = generationTimestamp,
          score = recomm._3)
      })
    recommendations
  }


  /** load a set of stopwords from file
    *
    * @param path: path of stopwords
    * @param sc : spark context
    * @return : a broadcasted variable with a set of terms
    */
  def stopWords(path: String, sc: SparkContext): Broadcast[Set[String]] = {
    sc.broadcast(Source.fromFile(path).getLines().map(_.trim).toSet)
  }

}
