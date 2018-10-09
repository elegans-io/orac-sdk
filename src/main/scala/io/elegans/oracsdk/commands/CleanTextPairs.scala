package io.elegans.oracsdk.commands

import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import io.elegans.oracsdk.tools.TextProcessingUtils
import org.apache.spark.broadcast.Broadcast

/** Read a list of CVs and JobDescriptions, clean and prepare the dataset */
object CleanTextPairs {

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
                             languages: List[String] = List("en", "it"),
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

    val mergedData = spark.sql(
      //firstname, lastname, email, jd, cv
      "SELECT cv.`User Firstname`, cv.`User Lastname`, cv.`User E-mail Address`, " +
        "jd.`Req #`, jd.`Requisition Title Translated`, jd.`Requisition Description Translated`, cv.`Job Seeker Resume Body`" +
        " FROM CVs cv JOIN JDs jd ON cv.`Req #` = jd.`Req #`")

    val stopWords = spark.sparkContext.broadcast(Set[String]())

    val textPairs = mergedData.rdd.map { case (row) =>
      (row.getString(6), row.getString(4) + " " + row.getString(5))
    }

    val tokenizedTextRaw = textPairs.map { case (cv, jd) =>
      val cvTokens = TextProcessingUtils.tokenizeSentence(text = cv, stopWords = stopWords)
      val jdTokens = TextProcessingUtils.tokenizeSentence(text = jd, stopWords = stopWords)
      (cv, jd, cvTokens, jdTokens)
    }

    val filterLang = params.languages.toSet
    val tokenizedText = tokenizedTextRaw

    val mergedCvJdTokens = tokenizedText.flatMap{case (_, _, cvTok, jdTok) => cvTok ++ jdTok}

    val mergedCvJdTokensWordCount = mergedCvJdTokens.map(token => (token, 1)).reduceByKey(_ + _).collectAsMap()

    val outOfDictionaryTokens = mergedCvJdTokens.map { case(word) =>
      dictionary.contains(word) match {
        case false =>
          val candidates = (1 to word.length).map{ case(i) => word.slice(0, i) }.map { case(firstChunk) =>
            val secondChunk = word.replaceAll("^" + firstChunk, "")
            if(dictionary.contains(firstChunk) && dictionary.contains(secondChunk)) {
              (word, (firstChunk, secondChunk))
            } else {
              (word, ("", ""))
            }
          }
          val replacement = candidates.map{
            case((origToken,(firstPart, secondPart))) =>
              val count = mergedCvJdTokensWordCount.getOrElse(firstPart, 0) +
                mergedCvJdTokensWordCount.getOrElse(secondPart, 0)
              (origToken,(firstPart, secondPart, count))
          }.maxBy(_._2._3)
          (replacement._1, (replacement._2._1, replacement._2._2))
        case true =>
          (word, (word, ""))
      }
    }.filter(word => !(word._2._1 != "" && word._2._2 == "")).distinct

    outOfDictionaryTokens.saveAsTextFile(params.output + "/OUT_OF_DICT_TOKENS")

    val outOfDictionaryTokensMap = outOfDictionaryTokens.collectAsMap

    val tokenizedTextReplacedTokens = tokenizedText.map{
      case (cv, jd, cvTokens, jdTokens) =>
        val newCvTokens = cvTokens.map(t => List(t)).flatMap { case(token) =>
          outOfDictionaryTokensMap.get(token.head) match {
            case Some(t) =>
              List(t._1, t._2)
            case _ =>
              token
          }
        }
        (cv, jd,
          newCvTokens.filter(_ != ""),
          jdTokens.filter(_ != ""))
    }

    tokenizedTextReplacedTokens.map {
      case((_, _, newCvTokens, jdTokens)) =>
        newCvTokens.mkString(",") + "\t" + jdTokens.mkString(",")
    }.saveAsTextFile(params.output + "/DATASET_CV_JD")

    tokenizedTextReplacedTokens.map {
      case((_, _, newCvTokens, jdTokens)) =>
        newCvTokens ++ jdTokens
    }.flatMap(list => list).map(token => (token, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending=false)
      .map(pair => pair._1)
      .zipWithIndex
      .map(term => term._1 + "," + term._2)
      .saveAsTextFile(params.output + "/DICTIONARY")

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
