package io.elegans.oracsdk.commands

import io.elegans.oracsdk.tools.TextProcessingUtils
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/** Read a list of Text1 and Text2, clean and prepare the dataset */
object CleanTextPairs {

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  /** Case class for command line variables with default values
    *
    * @param text1file the users data
    * @param text2file the items data
    * @param dictionary the word dictionary used to split words
    * @param output the path for output data
    */
  private case class Params(
                             text1file: String = "text1.tsv",
                             text2file: String = "text2.tsv",
                             dictionary: String = "dictionary.txt",
                             languages: List[String] = List("en", "it"),
                             output: String = "T1T2_DATASET"
                           )

  /** Do all the spark initialization, the logic of the program is here
    *
    * @param params the command line parameters
    */
  private def doEtl(params: Params) {
    val spark = SparkSession.builder().appName("CleanTextPairs").getOrCreate()

    val t1Data = spark.read.format("csv").
      option("header", "true").
      option("delimiter", "\t").
      load(params.text1file)
    t1Data.createOrReplaceTempView("Text1")

    val t2Data = spark.read.format("csv").
      option("header", "true").
      option("delimiter", "\t").
      load(params.text2file)
    t2Data.createOrReplaceTempView("Text2")

    val dictionary = spark.sparkContext.textFile(params.dictionary).map{
      case (w) => (w, 0)
    }.collectAsMap()

    val mergedData = spark.sql(
      "SELECT t1.`User Firstname`, t1.`User Lastname`, t1.`User E-mail Address`, " +
        "t2.`Req #`, t2.`Requisition Title Translated`, t2.`Requisition Description Translated`, t1.`Job Seeker Resume Body`" +
        " FROM Text1 t1 JOIN Text2 jd ON t1.`Req #` = t2.`Req #`")

    val stopWords = spark.sparkContext.broadcast(Set[String]())

    val textPairs = mergedData.rdd.map { case (row) =>
      (row.getString(6), row.getString(4) + " " + row.getString(5))
    }

    val tokenizedTextRaw = textPairs.map { case (t1, t2) =>
      val T1Tokens = TextProcessingUtils.tokenizeSentence(text = t1, stopWords = stopWords)
      val T2Tokens = TextProcessingUtils.tokenizeSentence(text = t2, stopWords = stopWords)
      (t1, t2, T1Tokens, T2Tokens)
    }

    val tokenizedText = tokenizedTextRaw

    val mergedT1T2Tokens = tokenizedText.flatMap{case (_, _, t1Tok, t2Tok) => t1Tok ++ t2Tok}

    val mergedT1T2TokensWordCount = mergedT1T2Tokens.map(token => (token, 1)).reduceByKey(_ + _).collectAsMap()

    val outOfDictionaryTokens = mergedT1T2Tokens.map { case(word) =>
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
              val count = mergedT1T2TokensWordCount.getOrElse(firstPart, 0) +
                mergedT1T2TokensWordCount.getOrElse(secondPart, 0)
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
      case (t1, t2, t1Tokens, t2Tokens) =>
        val newT1Tokens = t1Tokens.map(t => List(t)).flatMap { case(token) =>
          outOfDictionaryTokensMap.get(token.head) match {
            case Some(t) =>
              List(t._1, t._2)
            case _ =>
              token
          }
        }
        (t1, t2,
          newT1Tokens.filter(_ != ""),
          t2Tokens.filter(_ != ""))
    }

    tokenizedTextReplacedTokens.map {
      case((_, _, newT1Tokens, t2Tokens)) =>
        newT1Tokens.mkString(",") + "\t" + t2Tokens.mkString(",")
    }.saveAsTextFile(params.output + "/DATASET_T1_T2")

    tokenizedTextReplacedTokens.map {
      case((_, _, newT1Tokens, t2Tokens)) =>
        newT1Tokens ++ t2Tokens
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
      opt[String]("text1file")
        .text(s"the first text data file" +
          s"  default: ${defaultParams.text1file}")
        .action((x, c) => c.copy(text1file = x))
      opt[String]("text2file")
        .text(s"the second text data file" +
          s"  default: ${defaultParams.text2file}")
        .action((x, c) => c.copy(text2file = x))
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
