package io.elegans.oracsdk.commands

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.elegans.oracsdk.extract._
import io.elegans.oracsdk.load.LoadData
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import scala.concurrent.ExecutionContextExecutor

object UploadLLRRecommendations {
  private case class Params(
                             host: String = "http://localhost:8888",
                             recommPath: String = "RECOMMENDATIONS",
                             userIdMappingPath: String = "USERIDMAPPINGS",
                             itemIdMappingPath: String = "ITEMIDMAPPINGS",
                             indexName: String = "index_english_0",
                             username: String = "admin",
                             password: String = "adminp4ssw0rd",
                             output: String = "USER_ACTIONS")

  private def executeTask(params: Params): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val spark = SparkSession.builder().appName("UploadLLRRecommendations").getOrCreate()
    val sc = spark.sparkContext

    val parameters = OracConnectionParameters(host=params.host,
      indexName = params.indexName, username = params.username, password = params.password)

    val recommendations = LoadData.llrRecommendations(
      recommPath = params.recommPath,
      userIdMappingPath = params.userIdMappingPath,
      itemIdMappingPath = params.itemIdMappingPath,
      spark
    )

    OracHttpClient.uploadRecommendation(parameters = parameters, recommendations = recommendations)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Download actions from orac") {
      head("download the actions from orac into a file with json entities")
      help("help").text("prints this usage text")
      opt[String]("host")
        .text(s"full hostname string: with protocol and port" +
          s"  default: ${defaultParams.host}")
        .action((x, c) => c.copy(host = x))
      opt[String]("indexName")
        .text(s"the index name" +
          s"  default: ${defaultParams.indexName}")
        .action((x, c) => c.copy(indexName = x))
      opt[String]("recommPath")
        .text(s"folder with recommendations" +
          s"  default: ${defaultParams.recommPath}")
        .action((x, c) => c.copy(recommPath = x))
      opt[String]("userIdMappingPath")
        .text(s"folder with userId => long mapping" +
          s"  default: ${defaultParams.userIdMappingPath}")
        .action((x, c) => c.copy(userIdMappingPath = x))
      opt[String]("itemIdMappingPath")
        .text(s"folder with itemId => long mapping" +
          s"  default: ${defaultParams.itemIdMappingPath}")
        .action((x, c) => c.copy(itemIdMappingPath = x))
      opt[String]("username")
        .text(s"the orac user name" +
          s"  default: ${defaultParams.username}")
        .action((x, c) => c.copy(username = x))
      opt[String]("password")
        .text(s"the orac password" +
          s"  default: ${defaultParams.password}")
        .action((x, c) => c.copy(password = x))
      opt[String]("output")
        .text(s"the destination directory for the output" +
          s"  default: ${defaultParams.output}")
        .action((x, c) => c.copy(output = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        executeTask(params)
        sys.exit(0)
      case _ =>
        sys.exit(1)
    }
  }
}
