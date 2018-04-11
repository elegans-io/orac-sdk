package io.elegans.oracsdk.commands

import io.elegans.oracsdk.extract.{OracConnectionParameters, OracHttpClient}
import io.elegans.oracsdk.load.LoadData
import io.elegans.oracsdk.transform.Transformer
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.duration._

object ActionsItemsToCoOccurrenceInput {
  private case class Params(
                             actions: String = "",
                             items: String = "",
                             output: String = "CO_OCCURRENCE_INPUT",
                             defPref: Double = 2.5,
                             host: String = "http://localhost:8888",
                             indexName: String = "index_english_0",
                             username: String = "admin",
                             password: String = "adminp4ssw0rd"
                           )

  private def executeTask(params: Params): Unit = {
    val appName = "ActionsItemsToCoOccurrenceInput"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext

    try {
      val parameters = OracConnectionParameters(host = params.host,
        indexName = params.indexName, username = params.username, password = params.password)

      /* downloading actions and return the path */
      val actionsFolder = params.actions match {
        case "" =>
          val folder = params.output + "/ACTIONS"
          val response = OracHttpClient.downloadActions(parameters = parameters, filePath = folder)
          val result = Await.result(response, Duration.Inf)
          if (result.wasSuccessful) {
            println("INFO: downloaded actions into " + folder)
          } else {
            println("ERROR: downloading action" + result.getError.getMessage)
            sys.exit(10)
          }
          folder
        case _ => params.actions
      }

      /* downloading item and return the path */
      val itemsFolder = params.actions match {
        case "" =>
          val folder = params.output + "/ITEMS"
          val response = OracHttpClient.downloadItems(parameters = parameters, filePath = folder)
          val result = Await.result(response, Duration.Inf)
          if (result.wasSuccessful) {
            println("INFO: downloaded items into " + folder)
          } else {
            println("ERROR: downloading items" + result.getError.getMessage)
            sys.exit(11)
          }
          folder
        case _ => params.items
      }

      /* load actions and items */
      println("INFO: loading actions")
      val actionsEntities = LoadData.actions(path = actionsFolder, sc = sc)

      println("INFO: loading items")
      val itemsEntities = LoadData.items(path = itemsFolder, sc = sc)

      /* joinedEntries = RDD[(userId, numericalUserId, itemId, itemRankId, score)] */
      println("INFO: join actions with items")
      val joinedEntries = Transformer.joinActionEntityForCoOccurrence(actionsEntities = actionsEntities,
        itemsEntities = itemsEntities, spark = spark, defPref = params.defPref)

      /* save mapping: userId -> numericalUserId */
      println("INFO: saving userId -> numericalUserId mapping")
      joinedEntries.map(item => item._1 + "," + item._2).distinct.saveAsTextFile(params.output + "/USER_ID_TO_LONG")

      /* save mapping: itemId -> rankId */
      println("INFO: saving itemId -> rankId mapping")
      joinedEntries.map(item => item._3 + "," + item._4).distinct.saveAsTextFile(params.output + "/ITEM_ID_TO_LONG")

      /* save actions for co-occurrence  */
      println("INFO: saving actions joined with items")
      joinedEntries.map(item => item._2 + "," + item._4 + "," + item._5)
        .saveAsTextFile(params.output + "/CO_OCCURRENCE_ACTIONS")

      println("Info: successfully terminated task : " + appName)
    } catch {
      case e: Exception =>
        println("Error: failed task : " + appName + " : " + e.getMessage)
    } finally {
      println("Info: Stop spark context: " + appName)
      spark.stop()
    }
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Actions and Items to co-occurrence input") {
      head("create an input dataset suitable for the co-occurrence algorithm")
      help("help").text("prints this usage text")
      opt[String]("host")
        .text(s"full hostname string: with protocol and port" +
          s"  default: ${defaultParams.host}")
        .action((x, c) => c.copy(host = x))
      opt[String]("indexName")
        .text(s"the index name" +
          s"  default: ${defaultParams.indexName}")
        .action((x, c) => c.copy(indexName = x))
      opt[String]("username")
        .text(s"the orac user name" +
          s"  default: ${defaultParams.username}")
        .action((x, c) => c.copy(username = x))
      opt[String]("password")
        .text(s"the orac password" +
          s"  default: ${defaultParams.password}")
        .action((x, c) => c.copy(password = x))
      opt[String]("actions")
        .text(s"the action's file or directory, if empty the data will be downloaded from orac" +
          s"  default: ${defaultParams.actions}")
        .action((x, c) => c.copy(actions = x))
      opt[String]("items")
        .text(s"the item's file or directory, if empty the data will be downloaded from orac" +
          s"  default: ${defaultParams.items}")
        .action((x, c) => c.copy(actions = x))
      opt[Double]("defPref")
        .text(s"default preference value if 0.0" +
          s"  default: ${defaultParams.defPref}")
        .action((x, c) => c.copy(defPref = x))
      opt[String]("output")
        .text(s"the destination directory for the output: tree subfolders will be created: " +
          s" CO_OCCURRENCE_ACTIONS, USER_ID_TO_LONG, ITEM_ID_TO_LONG" +
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
