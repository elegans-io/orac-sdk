package io.elegans.oracsdk.commands

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import scopt.OptionParser

object ItemSimilarityToSkipGram {
  private case class Params(itemSimilarity: String = "",
                            output: String = "ItemSimilarityToSkipGram"
                           )

  private def executeTask(params: Params): Unit = {
    val appName = "ItemSimilarityToSkipGram"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext

    try {
      /* load actions and items */
      println("INFO: loading actions: " + params.itemSimilarity)

      val inputData: RDD[(Long, List[(Long, Double)])] = spark.read.format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .load(params.itemSimilarity).map {
        case (row: Row) =>
          (row.get(0).asInstanceOf[Long], List((row.get(1).asInstanceOf[Long], row.get(2).asInstanceOf[Double])))
      }.rdd

      inputData.reduceByKey((a, b) => a ++ b).flatMap { case(e0, similar) =>
        similar.flatMap {
          case (s) =>
            List((e0, s._1, s._2), (e0, s._1, s._2))
        }
      }.map(x => x._1 + "," + x._2).saveAsTextFile(params.output + "/CO_OCCURRENCE_TO_SKIPGRAM")

      println("INFO: successfully terminated task : " + appName)
    } catch {
      case e: Exception =>
        println("ERROR: failed task : " + appName + " : " + e.getMessage)
        sys.exit(12)
    } finally {
      println("INFO: Stop spark context: " + appName)
      spark.stop()
    }
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Actions and Items to co-occurrence input") {
      head("create an input dataset suitable for the co-occurrence algorithm")
      help("help").text("prints this usage text")
      opt[String]("itemSimilarity")
        .text(s"the itemSimilarity encoded as " +
          s"  default: ${defaultParams.itemSimilarity}")
        .action((x, c) => c.copy(itemSimilarity = x))
      opt[String]("output")
        .text(s"the destination directory for the output: 2 sub folders will be created: " +
          s" ITEM_TO_RANKID, ACTIONS" +
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

