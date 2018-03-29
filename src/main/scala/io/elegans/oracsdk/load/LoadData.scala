package io.elegans.oracsdk.load

import akka.actor.ActorSystem
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import io.elegans.orac.entities._
import io.elegans.orac.serializers.OracJsonSupport
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.io.Source
import scala.util.{Failure, Success}

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
