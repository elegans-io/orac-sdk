package io.elegans.oracsdk.commands

import io.elegans.oracsdk.load.{LoadData, SaveToCsv}
import io.elegans.oracsdk.transform.Transformer
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object ActionsToCoOccurrenceInput {
  private case class Params(
                             input: String = "USER_ACTIONS",
                             output: String = "CO_OCCURRENCE_INPUT",
                             defPref: Double = 2.5
                           )

  private def executeTask(params: Params): Unit = {
    val appName = "ActionsToCoOccurrenceInput"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext

    try {
      val actionsEntities = LoadData.actions(path = params.input, sc = sc)

      val coOccurrenceInputData = Transformer.actionsToCoOccurrenceInput(input = actionsEntities, spark = spark,
        defPref = params.defPref)
      SaveToCsv.saveCoOccurrenceInput(input = coOccurrenceInputData._3, outputFolder = params.output + "/ACTIONS")
      SaveToCsv.saveStringToLongMapping(input = coOccurrenceInputData._1,
        outputFolder = params.output + "/USER_ID_TO_LONG")
      SaveToCsv.saveStringToLongMapping(input = coOccurrenceInputData._2,
        outputFolder = params.output + "/ITEM_ID_TO_LONG")

      println("INFO: successfully terminated task : " + appName)
    } catch {
      case e: Exception =>
        println("Error: failed task : " + appName + " : " + e.getMessage)
    } finally {
      println("INFO: Stop spark context: " + appName)
      spark.stop()
    }
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Actions to co-occurrence input") {
      head("create an input dataset suitable for the co-occurrence algorithm, " +
        "takes the items id directly from actions")
      help("help").text("prints this usage text")
      opt[String]("input")
        .text(s"the input file or directory" +
          s"  default: ${defaultParams.input}")
        .action((x, c) => c.copy(input = x))
      opt[Double]("defPref")
        .text(s"default preference value if 0.0" +
          s"  default: ${defaultParams.defPref}")
        .action((x, c) => c.copy(defPref = x))
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
