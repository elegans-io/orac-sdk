package io.elegans.oracsdk.extract

/**
  * Created by angelo on 13/02/18.
  */

import java.io.File
import java.nio.file.StandardOpenOption._
import java.util.Base64

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{FileIO, Flow, Framing}
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.ByteString
import io.elegans.orac.entities.{DeleteDocumentsResult, IndexDocumentResult, Recommendation}
import io.elegans.orac.serializers.OracJsonSupport
import org.apache.spark.rdd.RDD

import scala.collection.immutable
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

case class OracConnectionParameters(
                                     host: String,
                                     indexName: String,
                                     username: String,
                                     password: String
                                   )

/** orac-api interactions functions
  */
object OracHttpClient extends OracJsonSupport {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  /** Generate an orac-uri
    *
    * @param httpParameters: the http parameters
    * @param path: the path of the orac service
    * @return : orac uri
    */
  private[this] def uri(httpParameters: OracConnectionParameters, path: String): String = {
    httpParameters.host + "/" + httpParameters.indexName + path
  }

  /** Create a the header elements for the http request
    *
    * @param headerValues: map of key values header elements
    * @param default: default header value, by default application/json
    * @return : a sequence of RawHeader elements
    */
  private[this] def httpJsonHeader(
                                    headerValues: Map[String, String] = Map.empty[String, String],
                                    default: (String, String) = ("application", "json")):
  immutable.Seq[HttpHeader] = {
    val headers = headerValues.map { case (key, value) =>
      RawHeader(key, value)
    } ++ Seq(RawHeader(default._1, default._2))
    headers.to[immutable.Seq]
  }

  /** execute an http stream call and write the result on file
    *
    * @param path: the path of the orac endpoint
    * @param parameters: the connection parameters
    * @param httpMethod: the http method (GET, POST, PUT ...)
    * @param filePath: the path of the output file
    * @return : a future with an IOResult
    */
  private[this] def streamToFile(path: String, parameters: OracConnectionParameters,
                                 httpMethod: HttpMethod,
                                 filePath: String): Future[IOResult] = {
    val http = Http()
    val entity = Future(HttpEntity.Empty)
    val url = uri(httpParameters = parameters, path = path)
    val credentials =
      "Basic " + Base64.getEncoder.encodeToString((parameters.username + ":" + parameters.password).getBytes)
    val headers = httpJsonHeader(headerValues = Map[String, String]("Authorization" -> credentials))

    val response = entity.flatMap { ent =>
      http.singleRequest(HttpRequest(
        method = httpMethod,
        uri = url,
        headers = headers,
        entity = ent))
    }

    response flatMap { response =>
      response.entity.withoutSizeLimit.getDataBytes
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024 * 1024, allowTruncation = true))
        .via(Flow[ByteString].intersperse(ByteString(""), ByteString("\n"), ByteString("")))
        .runWith(FileIO.toPath(new File(filePath).toPath,
          options = Set(CREATE, WRITE, TRUNCATE_EXISTING)), materializer)
    }
  }

  /** fetch and write the actions on file
    *
    * @param parameters: the connection parameters
    * @param filePath: the path of the output file
    * @return : a future with an IOResult
    */
  def downloadActions(parameters: OracConnectionParameters, filePath: String): Future[IOResult] = {
    streamToFile(path = "/stream/action", parameters = parameters, HttpMethods.GET, filePath = filePath)
  }

  /** upload recommendations on orac-api
    *
    * @param parameters http parameters (OracConnectionParameters)
    * @param recommendations the RDD with the recommendations to upload
    * @return an RDD with the result of the upload for each item
    */
  def uploadRecommendation(parameters: OracConnectionParameters,
                           recommendations: RDD[Recommendation]): RDD[Option[IndexDocumentResult]] = {
    recommendations.map { case (rec) =>
      val http = Http()
      val entity = Marshal(rec).to[MessageEntity]
      val url = uri(httpParameters = parameters, path = "/recommendation")
      val credentials =
        "Basic " + Base64.getEncoder.encodeToString((parameters.username + ":" + parameters.password).getBytes)
      val headers = httpJsonHeader(headerValues = Map[String, String]("Authorization" -> credentials))
      val response = entity.flatMap { ent =>
        http.singleRequest(
          HttpRequest(
            method = HttpMethods.POST,
            uri = url,
            headers = headers,
            entity = ent
          )
        )
      }

      val result = Await.result(response, Duration.Inf)
      result.status match {
        case StatusCodes.Created | StatusCodes.OK =>
          Try(Await.result(Unmarshal(result.entity).to[IndexDocumentResult], 5.second)) match {
            case Success(resEntity) =>
              Some(resEntity)
            case Failure(e) =>
              println("Error unmarshalling response(" + result + "): " + e.getMessage)
              None
          }
        case _ =>
          println("Error indexing entry(" + rec + ") Message(" + result.toString() + ")")
          None
      }
    }.filter(_.nonEmpty)
  }

  /** delete all the recommendation within a time range in milliseconds from epoc
    *
    * @param parameters http parameters (OracConnectionParameters)
    * @param from a start range
    * @param to an end range
    * @return the data structure with informations about deleted items
    */
  def deleteRecommendations(parameters: OracConnectionParameters, from: Option[Long],
                            to: Option[Long]): Option[DeleteDocumentsResult] = {
      val http = Http()
      val entity = Future(HttpEntity.Empty)

      val queryString = if(from.isEmpty && to.isEmpty) {
        ""
      } else if(from.isEmpty) {
        "&to=" + to
      } else if(to.isEmpty) {
        "&from=" + from
      } else {
        "&from=" + from + "?to=" + to
      }

      val url = uri(httpParameters = parameters, path = "/recommendation/query" + queryString)
      val credentials =
        "Basic " + Base64.getEncoder.encodeToString((parameters.username + ":" + parameters.password).getBytes)
      val headers = httpJsonHeader(headerValues = Map[String, String]("Authorization" -> credentials))
      val response = entity.flatMap { ent =>
        http.singleRequest(
          HttpRequest(
            method = HttpMethods.DELETE,
            uri = url,
            headers = headers,
            entity = ent
          )
        )
      }

    val result = Await.result(response, Duration.Inf)
    result.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        Try(Await.result(Unmarshal(result.entity).to[DeleteDocumentsResult], 5.second)) match {
          case Success(resEntity) =>
            Some(resEntity)
          case Failure(e) =>
            println("Error unmarshalling response(" + result + "): " + e.getMessage)
            None
        }
      case _ =>
        println("failed deleting old recommendations Message(" + result.toString() + ")")
        None
    }
  }


  /** fetch and write the oracUsers on file
    *
    * @param parameters: the connection parameters
    * @param filePath: the path of the output file
    * @return : a future with an IOResult
    */
  def downloadOracUsers(parameters: OracConnectionParameters, filePath: String): Future[IOResult] = {
    streamToFile(path = "/stream/orac_user", parameters = parameters, HttpMethods.GET, filePath = filePath)
  }

  /** fetch and write the items on file
    *
    * @param parameters: the connection parameters
    * @param filePath: the path of the output file
    * @return : a future with an IOResult
    */
  def downloadItems(parameters: OracConnectionParameters, filePath: String): Future[IOResult] = {
    streamToFile(path = "/stream/item", parameters = parameters, HttpMethods.GET, filePath = filePath)
  }
}
