package io.elegans.oracsdk.commands

import io.elegans.oracsdk.load.LoadData
import io.elegans.oracsdk.store.SaveToCsv
import io.elegans.oracsdk.transform.Transformer
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object ActionsToCoOccurrenceInput {
  private case class Params(
                             input: String = "USER_ACTIONS",
                             output: String = "CO_OCCURRENCE_INPUT")

  private def executeTask(params: Params): Unit = {
    val spark = SparkSession.builder().appName("ActionsToCoOccurrenceInput").getOrCreate()
    val sc = spark.sparkContext

    val actionsEntities = LoadData.actions(path = params.input, sc = sc)

    val coOccurrenceInputData = Transformer.actionsToCoOccurrenceInput(input = actionsEntities, spark = spark)
    SaveToCsv.saveCoOccurrenceInput(input = coOccurrenceInputData._3, outputFolder = params.output + "/ACTIONS")
    SaveToCsv.saveStringToLongMapping(input = coOccurrenceInputData._1,
      outputFolder = params.output + "/USER_ID_TO_LONG")
    SaveToCsv.saveStringToLongMapping(input = coOccurrenceInputData._2,
      outputFolder = params.output + "/ITEM_ID_TO_LONG")
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Actions to co-occurrence input") {
      head("create an input dataset suitable for the co-occurrence algorithm")
      help("help").text("prints this usage text")
      opt[String]("input")
        .text(s"the input file or directory" +
          s"  default: ${defaultParams.input}")
        .action((x, c) => c.copy(input = x))
      opt[String]("output")
        .text(s"the destination directory for the output: tree subfolders will be created: " +
          s" ACTIONS, USER_ID_TO_LONG, ITEM_ID_TO_LONG" +
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
