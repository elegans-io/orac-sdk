package io.elegans.oracsdk.commands

import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import io.elegans.oracsdk.tools.TextProcessingUtils

/** Read a list of CVs and JobDescriptions, clean and prepare the dataset */
object CleanTextPairs {

  lazy val textProcessingUtils = new TextProcessingUtils /* lazy initialization of TextProcessingUtils class */

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  /** Case class for command line variables with default values
    *
    * @param cvfile the users data
    * @param jdfile the items data
    * @param dictionary the word dictionary used to split words
    * @param output the path for output data
    */
  private case class Params(
                             cvfile: String = "CVs.tsv",
                             jdfile: String = "JDs.tsv",
                             dictionary: String = "dictionary.txt",
                             output: String = "CVs_DATASET"
                           )

  /** Do all the spark initialization, the logic of the program is here
    *
    * @param params the command line parameters
    */
  private def doEtl(params: Params) {
    val spark = SparkSession.builder().appName("CleanTextPairs").getOrCreate()

    val cvData = spark.read.format("csv").
      option("header", "true").
      option("delimiter", "\t").
      load(params.cvfile)
    cvData.createOrReplaceTempView("CVs")

    val jdData = spark.read.format("csv").
      option("header", "true").
      option("delimiter", "\t").
      load(params.jdfile)
    jdData.createOrReplaceTempView("JDs")

    val dictionary = spark.sparkContext.textFile(params.dictionary).map{
      case (w) => (w, 0)
    }.collectAsMap()

    cvData.col("")


    val merged_data = spark.sql(
      "SELECT user, gender, age, job, item, rating, time, genres, title" +
        " FROM transactions t JOIN items i ON i.id = t.item JOIN users u ON u.id = t.user")

/*
    val extracted_data_sentences: RDD[List[String]] =
      merged_data.rdd
        .map(x => {
          val title = if (x.isNullAt(8)) { "" } else x.getString(8)
          List(x.getString(0), x.getString(1),
            x.getString(2), x.getString(3),
            x.getString(4), x.getString(5),
            x.getString(6), x.getString(7), title
          )
        })

    val initialSet = List.empty[List[String]]
    val addToSet = (s: List[List[String]],
                    v: List[String]) => s ++ List(v)
    val mergePartitionSet = (s: List[List[String]],
                             v: List[List[String]]) => s ++ v

    val extracted_data = extracted_data_sentences
      .map(x => (x(0), x)).aggregateByKey(initialSet)(addToSet, mergePartitionSet).map(x => {
      (x._1, x._2.sortBy(z => z(6).toInt))
    })

    val extractedSentences = extracted_data.flatMap(x => x._2)
      .map(x => { List("user_" + x(0), "gender_" + x(1),
        "age_" + x(2), "job_" + x(3), "itemid_" + x(4),
        "rating_" + x(5), x(7).split(",").mkString("_"), x(8).split(",").mkString(" ") ) })
      .map(x => x.mkString(" "))
    extractedSentences.saveAsTextFile(params.output + "/" + params.format) /* write the output in plain text format */
*/
    spark.stop()
  }

  /** The main function, this function must be given in order to run a stand alone program
    *
    * @param args will contain the command line arguments
    */
  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("CleanTextPairs") { /* initialization of the parser */
      head("create sentences from user iteractions list of sentences with spark")
      help("help").text("prints this usage text")
      opt[String]("cvfile")
        .text(s"the CVs data file" +
          s"  default: ${defaultParams.cvfile}")
        .action((x, c) => c.copy(cvfile = x))
      opt[String]("jdfile")
        .text(s"the JDs data file" +
          s"  default: ${defaultParams.jdfile}")
        .action((x, c) => c.copy(jdfile = x))
      opt[String]("dictionary")
        .text(s"the words dictionary file" +
          s"  default: ${defaultParams.dictionary}")
        .action((x, c) => c.copy(dictionary = x))
      opt[String]("output")
        .text(s"the destination directory for the output" +
          s"  default: ${defaultParams.output}")
        .action((x, c) => c.copy(output = x))
    }

    parser.parse(args, defaultParams) match { /* parsing of the command line options */
      case Some(params) =>
        doEtl(params)
      case _ =>
        sys.exit(1)
    }
  }
}
