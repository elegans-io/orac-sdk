package io.elegans.oracsdk.commands

import io.elegans.oracsdk.transform.Transformer
import io.elegans.oracsdk.transform.Transformer.{lshClustering0, lshClustering1}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import scopt.OptionParser

import scala.util.Random

object ActionsItemsToSkipGram_0 {
  private case class Params(actions: String = "",
                            output: String = "ActionsItemsToSkipGram_0",
                            simThreshold: Double = 0.4,
                            sliding: Int = 3,
                            numHashTables: Int = 10,
                            defPref: Double = 2.5,
                            clustering: Int = 0,
                            windowSize: Int = 3,
                            pairs: Boolean = false,
                            shuffle: Boolean = false,
                            genCoOccurrence: Boolean = false
                           )

  private def executeTask(params: Params): Unit = {
    val appName = "ActionsItemsToSkipGram_0"
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
      val maxItemRankId = itemsToRankId.map(x => x._2.toLong).max
      itemsToRankId.map{case(id, rankId) => rankId + "," + id}.saveAsTextFile(params.output + "/ITEM_TO_RANKID")

      if(params.genCoOccurrence) {
        // import spark implicits to create views for the join
        import spark.implicits._

        // build and save rankid map for users
        val usersToRankId = rankedIdActions.map(x => (x(4), 1))
          .reduceByKey((a, b) => a + b)
          .sortBy(_._2, ascending = true).map(_._1)
          .zipWithIndex // parnterId, partnerRankId
        usersToRankId.map { case (id, rankId) => rankId + "," + id }.saveAsTextFile(params.output + "/USER_TO_RANKID")

        // build and save input for the co-occurrence algorithm
        rankedIdActions.map(x => (x(4), x.last.toLong)).toDS
          .createOrReplaceTempView("partnerAction") // partnerId, itemRankId
        usersToRankId.toDS.createOrReplaceTempView("userRankId")
        spark.sql("select userRankId._2, partnerAction._2 from userRankId, partnerAction " +
            "WHERE userRankId._1 = partnerAction._1").rdd.map{
          case(entry) =>
            entry(0).asInstanceOf[Long] + "," + entry(1).asInstanceOf[Long]
        }.saveAsTextFile(params.output + "/CO_OCCURRENCE_INPUT")
      }

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

      // preparing skip-gram with permutations,
      //    e.g. for a window = 3 the result is the permutations without repetitions:
      //  <w1> <w2> <w0>
      //  <w1> <w0> <w2>
      //  <w2> <w1> <w0>
      //  <w2> <w0> <w1>
      //  <w0> <w1> <w2>
      //  <w0> <w2> <w1>
      val skipNGram = groupedActions.flatMap { case(customerBaskets) =>
        customerBaskets.flatMap { case (basket) =>
          val basketItemsRankId = basket.map(x => x("RANKID")).toArray
          basketItemsRankId.combinations(params.windowSize).flatMap(x => x.permutations)
        }
      }.filter(x => x.nonEmpty)

      val skipGramItems = if(params.pairs) {
        skipNGram.flatMap {
          /* pairs */
          case (l) =>
            val head = l.head
            l.tail.map(y => Array(head, y))
        }
      } else {
        skipNGram
      }

      val out = if(params.shuffle) {
        val rand = Random
        skipGramItems.map(x => (x, rand.nextInt())).sortBy(_._2).map(_._1)
      } else {
        skipGramItems
      }

      val numOfOutEntries = out.count()
      out.map(x => x.mkString(","))
        .saveAsTextFile(params.output + "/ACTIONS_" + maxItemRankId + "_" + numOfOutEntries)

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
      opt[Unit]("pairs")
        .text(s"arrange the labels and targets in pairs" +
          s"  default: ${defaultParams.pairs}")
        .action((_, c) => c.copy(pairs = true))
      opt[Unit]("shuffle")
        .text(s"shuffle pairs, requires a big amount of heap space" +
          s"  default: ${defaultParams.pairs}")
        .action((_, c) => c.copy(shuffle = true))
      opt[Unit]("genCoOccurrence")
        .text(s"generate input for the co-occurrence algorithm" +
          s"  default: ${defaultParams.genCoOccurrence}")
        .action((_, c) => c.copy(genCoOccurrence = true))
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

