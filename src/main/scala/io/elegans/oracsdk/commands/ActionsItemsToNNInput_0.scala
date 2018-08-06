package io.elegans.oracsdk.commands

import io.elegans.oracsdk.transform.Transformer
import io.elegans.oracsdk.transform.Transformer.{lshClustering0, lshClustering1}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import scopt.OptionParser

import scala.annotation.tailrec
import scala.reflect.ClassTag

object ActionsItemsToNNInput_0 {
  private case class Params(
                             actions: String = "",
                             output: String = "NN_INPUT_0",
                             simThreshold: Double = 0.4,
                             sliding: Int = 3,
                             numHashTables: Int = 10,
                             defPref: Double = 2.5,
                             clustering: Int = 0,
                             windowSize: Int = 3
                           )

  private def executeTask(params: Params): Unit = {
    val appName = "ActionsItemsToNNInput_0"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext

    try {
      /* load actions and items */
      println("INFO: loading actions: " + params.actions)

      val df = spark.read.format("com.databricks.spark.xml")
          .option("rowTag", "ROW")
        .load(params.actions)

      val columns = List("INVOICE_ID", "INVOICE_REFERENCE", "INVOICE_LINE_ID", "INVOICE_DATE",
        "PRT_PARTNER_ID", "BARCODE", "DESCRIPTION", "QUANTITY", "PRICE", "TOTAL_AMOUNT",
        "BRAND", "DIVISION")

      val inputData: RDD[Array[String]] = df.rdd.map(x => x(0))
        .map(x => x.asInstanceOf[scala.collection.mutable.WrappedArray[Row]]
          .toList.map(y => (y.getString(0), y.getString(1))))
        .map(x => x.toMap).map(x => columns.map(e => x(e)).toArray)


      val clustering: RDD[(String, String, Double)] => RDD[(String, Long)] = params.clustering match {
        case 1 => lshClustering1
        case _ => lshClustering0
      }

      val rankedIdActions = Transformer.makeRankIdLSH(
        input = inputData,
        columns = Seq(6), // DESCRIPTION
        spark = spark,
        simThreshold = params.simThreshold,
        sliding = params.sliding,
        numHashTables = params.numHashTables,
        clustering = clustering
      )

      // building item to rankid map
      val itemsToRankId = rankedIdActions.map(x => (x(6), x.last)).distinct
      val maxRankId = itemsToRankId.map(x => x._2.toLong).max
      itemsToRankId.map{case(id, rankId) => id + "," + rankId}.saveAsTextFile(params.output + "/ITEM_TO_RANKID")

      // building rankid to item map
      val newColumns = columns ++ List("RANKID")

      val groupedActions = rankedIdActions.map(x => newColumns.zip(x).toMap).map(x => (x("PRT_PARTNER_ID"), List(x)))
        .reduceByKey(
          (a, b) => a ++ b).map { case(partnerID, items0) =>
        (partnerID, items0.groupBy(_("INVOICE_DATE")) // put together the invoices of the same day
          .map{ case(_ /*invoiceID*/, items1) => items1})
      }.map{ case(invoiceID, items2)=>
        (invoiceID, items2.toList.sortWith(_.head("INVOICE_DATE") < _.head("INVOICE_DATE")))
      } /* each line is (partnerID, Baskets[Basket[Item[key, value]]]) baskets are sorted in asc. order by date */
      .map{ case(_, baskets) => baskets.map{case(basket) => basket} } // foreach basket


      /* recursilvely apply the cartesian product of an array with itself */
      def iterateCartesian[T: ClassTag](a: Array[T], count: Int = 2): Array[Array[T]] = {
        @tailrec
        def iterate(b: Array[Array[T]], countA: Int = 2): Array[Array[T]] =
          if (countA == 1)
            Transformer.partialCartesian(b, a)
          else {
            iterate(Transformer.partialCartesian(b, a), countA -1)
          }
        iterate(a.map(x => Array(x)), count)
      }

      // preparing skip-gram with permutations,
      //    e.g. for a window = 3 the result is the permutations without repetitions:
      //  <w1> <w2> <w0>
      //  <w1> <w0> <w2>
      //  <w2> <w1> <w0>
      //  <w2> <w0> <w1>
      //  <w0> <w1> <w2>
      //  <w0> <w2> <w1>
      val skipNGram = groupedActions.flatMap{ case(customerBaskets) =>
        customerBaskets.filter(_.length >= params.windowSize)
          .flatMap { case (basket) =>
            val basketItemsRankId = basket.map(x => x("RANKID")).toArray
            iterateCartesian(basketItemsRankId, params.windowSize -1)
              .filter(x => x.length == x.toSet.size).map(x => x.toList)
          }
      }.filter(x => x.nonEmpty)
      skipNGram.map(x => x.mkString(",")).saveAsTextFile(params.output + "/ACTIONS")
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
      opt[String]("actions")
        .text(s"the action's file or directory, if empty the data will be downloaded from orac" +
          s"  default: ${defaultParams.actions}")
        .action((x, c) => c.copy(actions = x))
      opt[Double]("defPref")
        .text(s"default preference value if 0.0" +
          s"  default: ${defaultParams.defPref}")
        .action((x, c) => c.copy(defPref = x))
      opt[Double]("simThreshold")
        .text(s"LSH similarity threshold the lower the value the stricter is the match" +
          s"  default: ${defaultParams.simThreshold}")
        .action((x, c) => c.copy(simThreshold = x))
      opt[Int]("sliding")
        .text(s"sliding window for shingles" +
          s"  default: ${defaultParams.sliding}")
        .action((x, c) => c.copy(sliding = x))
      opt[Int]("numHashTables")
        .text(s"number of buckets for LSH, high values will slow down the LSH process" +
          s"  default: ${defaultParams.numHashTables}")
        .action((x, c) => c.copy(numHashTables = x))
      opt[Int]("clustering")
        .text(s"clustering algoritm: 1 for the greedy, 0 for the faster and more accurate" +
          s"  default: ${defaultParams.clustering}")
        .action((x, c) => c.copy(clustering = x))
      opt[Int]("windowSize")
        .text(s"word2vec window size" +
          s"  default: ${defaultParams.windowSize}")
        .action((x, c) => c.copy(windowSize = x))
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

