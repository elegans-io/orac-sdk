package io.elegans.oracsdk.transform

import org.apache.spark.rdd.RDD
import io.elegans.orac.entities._

object Transformer extends java.io.Serializable {

  val informationCleaning = true

  /** Tokenize a sentence
    *
    * @param s: a string to tokenize
    * @return : A single string which contains all tokens space separated
    */
  def tokenizer(s: String): String = s.split("\\s+")
    .filter(_.matches("""[a-zA-Z_]\w*"""))
    .map(_.toLowerCase).mkString(" ")

  def tokenizeToRankID(input:  RDD[List[String]], stopWords: Set[String]):   //It would be easier with an Array, but we cannot
  RDD[(String, String)]  = {

    val word_occ_map_all = input.flatMap(identity).filter(x => ! stopWords.contains(x)).map((_, 1))
      .reduceByKey((a, b) => a + b)

    if (!this.informationCleaning)
      word_occ_map_all.sortBy(- _._2).map(x => x._1).zipWithIndex.map(x => (x._1, x._2.toString))
    else {
      val one_byte = 5.55  // nat

      val total_items = input.count

      val total_words = word_occ_map_all.count
      // (occurrence, count with such occurrence)
      val occ_count_map = word_occ_map_all.map(x => (x._2, 1)).reduceByKey((a, b) => a + b)

      // lowest occurrence to be considered
      val lowest_occ: Int = occ_count_map.filter(x => math.log(total_words / x._2) > one_byte).sortBy(_._1).first._1

      // Makes cut in high-frequency
      val word_occ_map = word_occ_map_all.filter(x => math.log(total_items / x._2) >= one_byte).filter(_._2 > lowest_occ)

      word_occ_map.sortBy(- _._2).map(x => x._1).zipWithIndex.map(x => (x._1, x._2.toString))
    }
  }

  /**
    * Substitute elements in one or more columns with a numerical ID inversely
    * proportional to occurrence. Eg if RDD contains rows of books,
    * and the 3rd column is "author" and the 4th "title", `makeRankID(input, Seq(3,4), false)`
    * will add a column Int.toString where "0" is the most popular book author+title
    * "1" the second most popular etc.
    * (different book editions with same author+title will have same RankID)
    *
    * @param input arrays of fields (all strings)
    * @param columns identifiers for the rank
    * @param tokenize tokenize the resulting string and substitute RankIDs to tokens (words)
    * @return same RDD with one more column with the RankIDs
    *
    */
  def makeRankId(input: RDD[Array[String]], columns: Seq[Int],
                 tokenize: Boolean,
                 replace:Option[Int]=None): RDD[Array[String]] = {

    assert(input.first.length >= columns.max + 1)
    val new_input = input.map{x => x :+ columns.map(y => x(y)).mkString(" ")}

    if (!tokenize) {
      // token => rankid
      val rankid_map = new_input.map(x => (x.last, 1)).reduceByKey((a, b) => a + b)
        .sortBy(- _._2).map(x => x._1).zipWithIndex
        .map(x => (x._1, x._2.toString)).collect.toMap
      replace match {
        case Some(v) => new_input.map(x => x.dropRight(1) :+ rankid_map(x.last)).map(x => x.updated(v, x.last).dropRight(1))
        case _ => new_input.map(x => x.dropRight(1) :+ rankid_map(x.last))
      }
    } else {
      // from String to clean List[String]
      val input_with_tokenized_column = new_input.map(x => x :+ this.tokenizer(x.last))

      val tokenized_column = new_input.map(x => x.last)
      // makes the map (Word, rankid)
      //      val rankid_map = this.tokenizeToRankID(...)

      val rankid_map = tokenized_column.flatMap(line => line.split(" "))
        .map(x => (x, 1)).reduceByKey((a, b) => a + b)
        .sortBy(- _._2).map(x => x._1)
        .zipWithIndex.map(x => (x._1, x._2.toString)).collect.toMap

      replace match {
        case Some(v) => input_with_tokenized_column.map(x => x.dropRight(1) :+ x.last.split(" ")
          .map(y => rankid_map.getOrElse(y, ""))
          .mkString(" ")
        ).map(x => x.updated(v, x.last).dropRight(1))
        case _ => input_with_tokenized_column.map(x => x.dropRight(1) :+ x.last.split(" ")
          .map(y => rankid_map.getOrElse(y, ""))
          .mkString(" ")
        )
      }
    }
  }

  /** Takes an RDD with a RankId column and provides a Map with rankID -> RDD(value)
    * (or the opposite if reverse)
    *
    * @param input the RDD with fields, of which one is rankId
    * @param rankIdColumn where is the rankId
    * @param value column to be used as value
    * @param mode "random", "popularity": gets the most popular of the values for each rankID (eg most popular ISBN for books)
    * @param reverse true produces a map(value -> rankId)
    */
  def rankIdValueMap(input: RDD[Array[String]], rankIdColumn: Int, value: Int,
                     mode: String, reverse: Boolean=false): Map[String, String] = {

    if (!reverse) {
      if (mode == "random") {
        input.map(x => (x(rankIdColumn), x(value))).groupByKey.map(x => (x._1, x._2.toList(0))).collect.toMap
      } else if (mode == "popularity") {
        input.map(x => (x(rankIdColumn), x(value))).groupByKey.map(x => (x._1, x._2))
          .collect.map(x => (x._1, x._2.toList.groupBy(identity)
          .mapValues(_.size).toSeq.sortBy(-_._2).toList(0)._1)).toMap
      } else throw new IllegalArgumentException("mode can only be 'random' or 'popularity")

    } else {
      input.map(x => (x(value), x(rankIdColumn))).collect.toMap
    }


  }

  /** Join two RDD[Array[String]]
    * input = [username, action_date, ...]
    * table = [username, address, user_id]
    * val rdd3 = Join(input=input, input_column=0, table=table, key_column=0, value_column=2, replace=true)
    * rdd3: [user_id, action_date, ...]
    *
    * If !replace append the result
    */

  def join(input: RDD[Array[String]], input_column: Int,
           table: RDD[Array[String]], key_column: Int, value_column: Int,
           replace: Boolean
          ): RDD[Array[String]] = {
    if (replace)
      input.map(x => (x(input_column), x))
        .join(
          table.map(x => Tuple2(x(key_column), x(value_column)))
        )
        .map(x => x._2._1.updated(input_column, x._2._2))
    else
      input.map(x => (x(input_column), x))
        .join(table.map(x => Tuple2(x(key_column), x(value_column))))
        .map(x => x._2._1 :+ x._2._2)
  }

  /** extract only the fields needed by the co-occurrence algorithm
    *
    * @param input an RDD of Action entities
    * @return an RDD with tuple (user_id, item_id, score)
    */
  def actionsToCoOccurrenceInput(input: RDD[Action]): RDD[(String, String, Double)] = {
    input.map(entry => {
      val score: Double = entry.score match {
        case Some(s) => s
        case _ => 0.0d
      }
      (entry.item_id, entry.user_id, score)
    })
  }
}

