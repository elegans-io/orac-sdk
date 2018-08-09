package io.elegans.oracsdk.transform

import io.elegans.orac.entities._
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.{Vector => sparkMlVector, Vectors => sparkMlVectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, _}
import scalaz.Scalaz._

import scala.annotation.tailrec
import scala.reflect.ClassTag

object Transformer extends java.io.Serializable {

  /** cartesian product */
  def partialCartesian[T: ClassTag](a: Array[Array[T]], b: Array[T]): Array[Array[T]] = {
    a.flatMap(xs => {
      b.map(y => {
        xs ++ Array(y)
      })
    })
  }

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

  /** Tokenize a sentence
    *
    * @param s: a string to tokenize
    * @return : A single string which contains all tokens space separated
    */
  def tokenizer(s: String): String = s.split("\\s+")
    .filter(_.matches("""[a-zA-Z_]\w*"""))
    .map(_.toLowerCase).mkString(" ")

  /** Get a one-hot encoded vector from id and index
    *
    * @param index vector index from 0 to N-1
    * @param vectorSize the size N of the vector
    * @return a spark ML sparse vector with one-hot encoded value
    */
  def indexToOneHot(index: Int, vectorSize: Int): sparkMlVector = {
    sparkMlVectors.sparse(size = vectorSize, indices = Array(index), values = Array(1.0d))
  }

  def tokenizeToRankID(input:  RDD[List[String]], stopWords: Set[String],
                       informationCleaning: Boolean = true):  //It would be easier with an Array, but we cannot
  RDD[(String, String)] = {

    val word_occ_map_all = input.flatMap(identity).filter(x => ! stopWords.contains(x)).map((_, 1))
      .reduceByKey((a, b) => a + b)

    if (! informationCleaning)
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

  def shingles(text: String, sliding: Int = 5): List[String] = {
    text.replaceAll("\\s+", "")
      .replaceAll("[\\P{IsAlphabetic}]", "")
      .sliding(sliding).toList
  }


  /** clustering by nearest elements, it is accurate, a bit faster and returns more clusters
    *
    * @param similarity the distance between items
    * @return the items annotated with cluster id
    */
  def lshClustering0(similarity: RDD[(String, String, Double)]): RDD[(String, Long)] = {
    similarity
      .map {
        case (x) => (x._2, List((x._1, x._3)))
      }.reduceByKey((a, b) => a ++ b)
      .map { case (x) =>
        val h = x._2.minBy(_._2)
        (h._1, Set((x._1, h._2)))
      }.reduceByKey((a, b) => a ++ b)
      .map(x => (x._1, x._2))
      .zipWithIndex
      .flatMap(x => x._1._2.map(y => (y._1, x._2)))
  }

  /** clustering by nearest elements, it is greedy and returns less clusters
    *
    * @param similarity the distance between items
    * @return the items annotated with cluster id
    */
  def lshClustering1(similarity: RDD[(String, String, Double)]): RDD[(String, Long)] = {
    similarity
      .map{
        case(x) => (x._2, List((x._1, x._3)))
      }.reduceByKey((a, b) => a ++ b).zipWithIndex
      .flatMap{ case(x) => // produce (element, group_id)
        val l = List(x._1._1) ++ x._1._2.map(y => y._1)
        l.map(e => (e, List(x._2)))
      }
      .reduceByKey((a, b) => a ++ b) // produce symbol -> (1,2, .., <group_id>)
      .map(x => (x._2.min, Set(x._1))) // take the smallest group_id for each symbol (<group_id>, symbol)
      .reduceByKey((a, b) => a ++ b) // produce symbol -> (<group_id>, (symbol0, .., symbolN))
      .map(x => (x._1, x._2))
      .zipWithIndex
      .flatMap(x => x._1._2.map(y => (y, x._2)))
  }

  /** Calculate the LSH to cluster similar items and returns the rank id for each cluster
    *
    * @param input arrays of fields (all strings)
    * @param columns identifiers for the rank
    * @param spark the spark session
    * @param simThreshold the min similarity threshold, the lower the more similar 0.0 means equal
    * @param sliding the sliding window for shingles
    * @param numHashTables the number of hash table for the LSH algorithm
    * @param clustering the clustering function which groups similar items together
    * @return same RDD with one more column with the RankIDs starting from 0
    */
  def makeRankIdLSH(input: RDD[Array[String]],
                    columns: Seq[Int],
                    spark: SparkSession,
                    simThreshold: Double = 0.4,
                    sliding: Int = 3,
                    numHashTables: Int = 100,
                    clustering: RDD[(String, String, Double)] => RDD[(String, Long)] = lshClustering1)
  : RDD[Array[String]] = {

    assert(input.first.length >= columns.max + 1)
    val new_input = input.map{ x => x :+ columns.map(y => x(y)).mkString(" ") }

    val docShingles = new_input.map(x => x.last).distinct.map{ case(x) =>
      (x, shingles(x.toLowerCase, sliding).toSet)
    }

    val shingleDictionary = docShingles.map(x => x).flatMap(x => x._2)
      .distinct.zipWithIndex.collect.toMap

    val shingleDictionaryLength = shingleDictionary.size

    val shinglesFeatures = docShingles.map { case(x) =>
      val docShingleSeq = x._2.map(sh => (sh, shingleDictionary.get(sh)))
        .filter(_._2.nonEmpty).map { case(sh) =>
        (sh._2.get.toInt, 1.0)
      }.toSeq.sortWith(_._1 < _._1)
      (x._1, sparkMlVectors.sparse(shingleDictionaryLength, docShingleSeq))
    }

    val shinglesDataFrame = spark.createDataFrame(shinglesFeatures).toDF("id", "features")

    val mh = new MinHashLSH()
      .setNumHashTables(numHashTables)
      .setInputCol("features")
      .setOutputCol("hashValues")

    val model = mh.fit(shinglesDataFrame)
    val similarity = model.approxSimilarityJoin(shinglesDataFrame, shinglesDataFrame, simThreshold)
      .select(
        col("datasetA.id").alias("idA"),
        col("datasetB.id").alias("idB"),
        col("distCol").alias("dist"))
      .rdd.map(x =>
      (x(0).asInstanceOf[String],
        x(1).asInstanceOf[String],
        x(2).asInstanceOf[Double])).filter(x => x._1 =/= x._2)

    // calculate clusters
    val clusteredItems = clustering(similarity)
    val newInputNumOfColumn: Int = if (new_input.isEmpty) 0 else new_input.first.length

    // creating views for join
    import spark.implicits._

    val columnNames = List.range(0, newInputNumOfColumn, 1).map(x => "e_" + x)
    val schema = StructType(columnNames.map(x => StructField(x, StringType, nullable = false)))
    val newInputDataFrame = spark.createDataFrame(new_input.map(x => Row.fromSeq(x.toSeq)), schema)

    newInputDataFrame.createOrReplaceTempView("input")
    clusteredItems.toDS.createOrReplaceTempView("clustered")
    val maxClusterIndex = clusteredItems.sortBy(_._2.toLong).map(x => x._2).max

    val allButOneColumns = columnNames.reverse.tail.reverse.map(x => "input." + x).mkString(",")

    val lastNewInputColumnId = "input.e_" + (newInputNumOfColumn - 1)
    val unclusteredStartIndex = maxClusterIndex + 1

    // taking all elements which does not belong to a cluster and assign a unique index
    val notClusteredItems = spark.sql("SELECT " + lastNewInputColumnId +
      " FROM input LEFT JOIN clustered ON " + lastNewInputColumnId +
      " = clustered._1 WHERE clustered._2 IS NULL").map(x => x(0).toString)
      .distinct.rdd.zipWithIndex.map(x => (x._1, x._2 + unclusteredStartIndex))

    notClusteredItems.union(clusteredItems).toDS.createOrReplaceTempView("clustered")
    val annotatedActions = spark.sql("SELECT " + allButOneColumns +
      ", clustered._2 FROM input INNER JOIN clustered ON " + lastNewInputColumnId + " = clustered._1").rdd
      .map(x => x.toSeq.toArray.map(y => y.toString))

    // rank id: high frequency items get lower id
    annotatedActions.map(x => (x.last, List(x)))
      .reduceByKey((a, b) => a ++ b).sortBy(_._2.length, ascending=false)
      .zipWithIndex
      .flatMap(x => x._1._2.map(y => (Array(x._2.toString) ++ y.reverse.tail).reverse))
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
  def makeRankIdSimpleMatch(input: RDD[Array[String]], columns: Seq[Int],
                            tokenize: Boolean,
                            replace:Option[Int]=None): RDD[Array[String]] = {

    assert(input.first.length >= columns.max + 1)
    val new_input = input.map{ x => x :+ columns.map(y => x(y)).mkString(" ") }

    if (!tokenize) {
      // token => rankid
      val rankid_map = new_input.map(x => (x.last, 1)).reduceByKey((a, b) => a + b)
        .sortBy(- _._2).map(x => x._1).zipWithIndex
        .map(x => (x._1, x._2.toString)).collect.toMap
      replace match {
        case Some(v) => new_input
          .map(x => x.dropRight(1) :+ rankid_map(x.last))
          .map(x => x.updated(v, x.last).dropRight(1))
        case _ => new_input.map(x => x.dropRight(1) :+ rankid_map(x.last))
      }
    } else {
      // from String to clean List[String]
      val input_with_tokenized_column = new_input.map(x => x :+ this.tokenizer(x.last))

      val tokenized_column = new_input.map(x => x.last) // x.last is the union of the selected columns
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
    * @param mode "random", "popularity": gets the most popular of the values for
    *             each rankID (eg most popular ISBN for books)
    * @param reverse true produces a map(value -> rankId)
    */
  def rankIdValueMap(input: RDD[Array[String]], rankIdColumn: Int, value: Int,
                     mode: String, reverse: Boolean=false): Map[String, String] = {
    if(reverse) {
      input.map(x => (x(value), x(rankIdColumn))).collect.toMap
    } else {
      if (mode == "random") {
        input.map(x => (x(rankIdColumn), x(value))).groupByKey.map(x => (x._1, x._2.toList.head)).collect.toMap
      } else if (mode == "popularity") {
        input.map(x => (x(rankIdColumn), x(value))).groupByKey.map(x => (x._1, x._2))
          .collect.map(x => (x._1, x._2.toList.groupBy(identity)
          .mapValues(_.size).toSeq.sortBy(-_._2).toList.head._1)).toMap
      } else throw new IllegalArgumentException("mode can only be 'random' or 'popularity")
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
    * @return an RDD with 3 tuples
    *         ((str_user_id, long_user_id), (str_item_id, long_item_id), (long_user_id, long_item_id, score))
    */
  def actionsToCoOccurrenceInput(input: RDD[Action], spark: SparkSession, defPref: Double = 2.5d):
  (RDD[(String, Long)], RDD[(String, Long)], RDD[(Long, Long, Double)]) = {
    val entries = input.map(entry => {
      val score: Double = entry.score match {
        case Some(s) => if(s === 0.0d) defPref else s
        case _ => defPref
      }
      (entry.user_id, entry.item_id, score)
    })

    import spark.implicits._

    val userIdColumn = entries.map(x => x._1).distinct.zipWithIndex
    val itemIdColumn = entries.map(x => x._2).distinct.zipWithIndex

    userIdColumn.toDS.createOrReplaceTempView("userId")
    itemIdColumn.toDS.createOrReplaceTempView("itemId")
    entries.toDS.createOrReplaceTempView("entries")
    val convertedEntries = spark.sql("SELECT userId._2, itemId._2, entries._3 FROM entries INNER JOIN userId, " +
      "itemId WHERE entries._1 = userId._1 AND entries._2 = itemId._1").rdd
      .map(entry => (entry(0).asInstanceOf[Long], entry(1).asInstanceOf[Long], entry(2).asInstanceOf[Double]))
      .map(entry => (entry._1, entry._2, if(entry._3 == 0) defPref else entry._3))

    (userIdColumn, itemIdColumn, convertedEntries)
  }

  /** join actions and items and produce a tuple with the rankId
    *
    * @param actionsEntities RDD of Actions
    * @param itemsEntities RDD of Items
    * @param spark spark session
    * @param rankIdFunction the function which calculate the rankId
    * @param defPref default score value
    * @return an RDD : (userId, numericalUserId, itemId, itemRankId, score)
    */
  private[this] def joinActionEntityForCoOccurrenceFormat0(actionsEntities: RDD[Action],
                                                           itemsEntities: RDD[Item],
                                                           spark: SparkSession,
                                                           rankIdFunction:
                                                           RDD[Array[String]] => RDD[(String, String, String, String)],
                                                           defPref: Double = 2.5d):
  RDD[(String, String, String, String, String)] = {
    import spark.implicits._

    actionsEntities.map{case(record) =>
      val score = record.score match {
        case Some(s) => if(s === 0.0d) defPref else s
        case _ => defPref
      }
      (record.user_id, record.item_id, score)
    }.toDS.createOrReplaceTempView("actions")

    itemsEntities.map { case (record) =>
      val stringProperties = record.props match {
        case Some(p) =>
          p.string match {
            case Some(stringProps) =>
              stringProps.map(x => (x.key, x.value)).toMap
            case _ => Map.empty[String, String]
          }
        case _ => Map.empty[String, String]
      }
      (
        record.id,
        stringProperties.getOrElse("title", record.name),
        stringProperties.getOrElse("author", "unknown")
      )
    }.toDS.createOrReplaceTempView("items")

    val joinedItemActions = spark.sql("SELECT actions._1, actions._2, actions._3, " +
      "items._1, items._2, items._3 FROM actions INNER JOIN items " +
      "WHERE actions._2 = items._1").rdd
      .map { case (entry) =>
        Array(
          entry(0).asInstanceOf[String], // userId
          entry(1).asInstanceOf[String], // itemId
          entry(2).asInstanceOf[Double].toString, // score
          entry(3).asInstanceOf[String], // title
          entry(4).asInstanceOf[String] // author
        )
      }

    println("INFO: preparing items joined with rankID")
    /* joinedWithItemRankId = RDD[(userId, itemId, score, rankId)] */
    val joinedWithItemRankId = rankIdFunction(joinedItemActions)
      .map(item => (item._1, item._2, item._3, item._4))

    println("INFO: preparing userId -> numericalUserId")
    /* user_id => numerical_id */
    val userIdColumn = joinedWithItemRankId.map(x => x._1).distinct.zipWithIndex

    /* create joinedWithItemRankId view */
    joinedWithItemRankId.toDS.createOrReplaceTempView("rankedIdItems")

    /* userId => numericalUserId */
    userIdColumn.toDS.createOrReplaceTempView("userId")

    println("INFO: preparing (userId, numericalUserId, itemId, itemRankId, score)")
    /* join itemsWithRankId with numericalUserId*/
    spark.sql("SELECT rankedIdItems._1, userId._2, rankedIdItems._2, rankedIdItems._4, rankedIdItems._3 " +
      "FROM rankedIdItems INNER JOIN userId " +
      "WHERE rankedIdItems._1 = userId._1").rdd
      .map { case(entry) =>
        (
          entry(0).asInstanceOf[String], // userId
          entry(1).asInstanceOf[Long].toString, // numericalUserId
          entry(2).asInstanceOf[String], // itemId
          entry(3).asInstanceOf[String], // itemRankId
          entry(4).asInstanceOf[String] // score
        )
      }
  }

  /** join actions and items and produce a tuple with the rankId calculated used LSH
    *
    * @param actionsEntities RDD of Actions
    * @param itemsEntities RDD of Items
    * @param spark spark session
    * @param simThreshold similarity threshold for LSH, lower is the value the stricter is the comparison
    * @param sliding sliding window to calculate shingles for LSH
    * @param numHashTables number of LSH buckets
    * @param defPref default score value
    * @return an RDD : (userId, numericalUserId, itemId, itemRankId, score)
    */
  def joinActionEntityForCoOccurrenceLSHFormat0(actionsEntities: RDD[Action], itemsEntities: RDD[Item],
                                                spark: SparkSession,
                                                simThreshold: Double = 0.4,
                                                sliding: Int = 3,
                                                numHashTables: Int = 100,
                                                defPref: Double = 2.5d): RDD[(String, String, String, String, String)] = {
    /* joinedWithItemRankId = RDD[(userId, itemId, score, rankId)] */
    def rankIdFunction(joinedItemActions: RDD[Array[String]]): RDD[(String, String, String, String)] =
      Transformer.makeRankIdLSH(
        input = joinedItemActions,
        columns = Seq(3,4),
        spark = spark,
        simThreshold = simThreshold,
        sliding = sliding,
        numHashTables = numHashTables,
        clustering = lshClustering1
      ).map(item => (item(0), item(1), item(2), item(5)))

    joinActionEntityForCoOccurrenceFormat0(
      actionsEntities = actionsEntities,
      itemsEntities = itemsEntities,
      spark = spark,
      rankIdFunction = rankIdFunction,
      defPref = defPref)
  }

  /** join actions and items and produce a tuple with the rankId calculated using the last to fields
    *
    * @param actionsEntities RDD of Actions
    * @param itemsEntities RDD of Items
    * @param spark spark session
    * @param defPref default score value
    * @return an RDD : (userId, numericalUserId, itemId, itemRankId, score)
    */
  def joinActionEntityForCoOccurrenceSimpleMatchFormat0(actionsEntities: RDD[Action],
                                                        itemsEntities: RDD[Item],
                                                        spark: SparkSession,
                                                        defPref: Double = 2.5d):
  RDD[(String, String, String, String, String)] = {

    /* joinedWithItemRankId = RDD[(userId, itemId, score, rankId)] */
    def rankIdFunction(joinedItemActions: RDD[Array[String]]): RDD[(String, String, String, String)] =
      Transformer.makeRankIdSimpleMatch(input = joinedItemActions, columns = Seq(3,4),
        tokenize = false, replace = None).map(item => (item(0), item(1), item(2), item(5)))

    joinActionEntityForCoOccurrenceFormat0(
      actionsEntities = actionsEntities,
      itemsEntities = itemsEntities,
      spark = spark,
      rankIdFunction = rankIdFunction,
      defPref = defPref)
  }

}
