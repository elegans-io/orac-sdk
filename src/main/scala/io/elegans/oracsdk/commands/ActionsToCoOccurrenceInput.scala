package io.elegans.oracsdk.commands

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import io.elegans.oracsdk.load.LoadData
import io.elegans.oracsdk.transform.Transformer
import io.elegans.oracsdk.store.SaveToCsv
import scopt.OptionParser

object ActionsToCoOccurrenceInput {
  private case class Params(
                             input: String = "USER_ACTIONS",
                             output: String = "CO_OCCURRENCE_INPUT")

  private def executeTask(params: Params): Unit = {
    val conf = new SparkConf().setAppName("ActionsToCoOccurrenceInput")
    val sc = new SparkContext(conf)
    val actionsEntities = LoadData.actions(path = params.input, sc = sc)
    val coOccurrenceInput = Transformer.actionsToCoOccurrenceInput(input = actionsEntities)
    SaveToCsv.saveCoOccurrenceInput(input = coOccurrenceInput, outputFolder = params.output)
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
