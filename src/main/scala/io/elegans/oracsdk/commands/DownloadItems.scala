package io.elegans.oracsdk.commands

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.elegans.oracsdk.extract._
import scopt.OptionParser

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor}

object DownloadItems {
  private case class Params(
                             host: String = "http://localhost:8888",
                             indexName: String = "index_english_0",
                             from: Long = 0,
                             to: Long = 0,
                             username: String = "admin",
                             password: String = "adminp4ssw0rd",
                             output: String = "USER_ACTIONS")

  private def executeTask(params: Params): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val parameters = OracConnectionParameters(host=params.host,
      indexName = params.indexName, username = params.username, password = params.password)

    val res = OracHttpClient.downloadItems(parameters = parameters, filePath = params.output,
      from = Some(params.from), to = Some(params.to))
    Await.result(res, Duration.Inf)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Download items from orac") {
      head("download the items from orac into a file with json entities")
      help("help").text("prints this usage text")
      opt[String]("host")
        .text(s"full hostname string: with protocol and port" +
          s"  default: ${defaultParams.host}")
        .action((x, c) => c.copy(host = x))
      opt[String]("indexName")
        .text(s"the index name" +
          s"  default: ${defaultParams.indexName}")
        .action((x, c) => c.copy(indexName = x))
      opt[Long]("from")
        .text(s"from timestamp" +
          s"  default: ${defaultParams.from}")
        .action((x, c) => c.copy(from = x))
      opt[Long]("to")
        .text(s"to timestamp" +
          s"  default: ${defaultParams.to}")
        .action((x, c) => c.copy(to = x))
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
