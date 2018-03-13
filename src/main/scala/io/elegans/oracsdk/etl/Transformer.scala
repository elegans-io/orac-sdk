package io.elegans.oracsdk.etl

import org.apache.spark.rdd.RDD

class Transformer  extends java.io.Serializable {

  // togo into file
  val stopwords: List[String] = List("cd", "c", "libro", "audio", "ediz", "edizione", "italiana", "spagnola", "portoghese", "edizione", "prima", "seconda", "terza", "quarta", "quinta", "the", "a", "adesso", "ai", "al", "alla", "allo", "allora", "altre", "altri", "altro", "anche", "ancora", "avere", "aveva", "avevano", "ben", "buono", "che", "chi", "cinque", "comprare", "con", "consecutivi", "consecutivo", "cosa", "cui", "da", "del", "della", "dello", "dentro", "deve", "devo", "di", "doppio", "due", "e", "ecco", "fare", "fine", "fino", "fra", "gente", "giu", "ha", "hai", "hanno", "ho", "il", "indietro", "invece", "io", "la", "lavoro", "le", "lei", "lo", "loro", "lui", "lungo", "ma", "me", "meglio", "molta", "molti", "molto", "nei", "nella", "no", "noi", "nome", "nostro", "nove", "nuovi", "nuovo", "o", "oltre", "ora", "otto", "peggio", "pero", "persone", "piu", "poco", "primo", "promesso", "qua", "quarto", "quasi", "quattro", "quello", "questo", "qui", "quindi", "quinto", "rispetto", "sara", "secondo", "sei", "sembra", "sembrava", "senza", "sette", "sia", "siamo", "siete", "solo", "sono", "sopra", "soprattutto", "sotto", "stati", "stato", "stesso", "su", "subito", "sul", "sulla", "tanto", "te", "tempo", "terzo", "tra", "tre", "triplo", "ultimo", "un", "una", "uno", "va", "vai", "voi", "volte", "vostro", "ad", "al", "allo", "ai", "agli", "all", "agl", "alla", "alle", "con", "col", "coi", "da", "dal", "dallo", "dai", "dagli", "dall", "dagl", "dalla", "dalle", "di", "del", "dello", "dei", "degli", "dell", "degl", "della", "delle", "in", "nel", "nello", "nei", "negli", "nell", "negl", "nella", "nelle", "su", "sul", "sullo", "sui", "sugli", "sull", "sugl", "sulla", "sulle", "per", "tra", "contro", "io", "tu", "lui", "lei", "noi", "voi", "loro", "mio", "mia", "miei", "mie", "tuo", "tua", "tuoi", "tue", "suo", "sua", "suoi", "sue", "nostro", "nostra", "nostri", "nostre", "vostro", "vostra", "vostri", "vostre", "mi", "ti", "ci", "vi", "lo", "la", "li", "le", "gli", "ne", "il", "un", "uno", "una", "ma", "ed", "se", "perché", "anche", "come", "dov", "dove", "che", "chi", "cui", "non", "più", "quale", "quanto", "quanti", "quanta", "quante", "quello", "quelli", "quella", "quelle", "questo", "questi", "questa", "queste", "si", "tutto", "tutti", "a", "c", "e", "i", "l", "o", "ho", "hai", "ha", "abbiamo", "avete", "hanno", "abbia", "abbiate", "abbiano", "avrò", "avrai", "avrà", "avremo", "avrete", "avranno", "avrei", "avresti", "avrebbe", "avremmo", "avreste", "avrebbero", "avevo", "avevi", "aveva", "avevamo", "avevate", "avevano", "ebbi", "avesti", "ebbe", "avemmo", "aveste", "ebbero", "avessi", "avesse", "avessimo", "avessero", "avendo", "avuto", "avuta", "avuti", "avute", "sono", "sei", "è", "siamo", "siete", "sia", "siate", "siano", "sarò", "sarai", "sarà", "saremo", "sarete", "saranno", "sarei", "saresti", "sarebbe", "saremmo", "sareste", "sarebbero", "ero", "eri", "era", "eravamo", "eravate", "erano", "fui", "fosti", "fu", "fummo", "foste", "furono", "fossi", "fosse", "fossimo", "fossero", "essendo", "faccio", "fai", "facciamo", "fanno", "faccia", "facciate", "facciano", "farò", "farai", "farà", "faremo", "farete", "faranno", "farei", "faresti", "farebbe", "faremmo", "fareste", "farebbero", "facevo", "facevi", "faceva", "facevamo", "facevate", "facevano", "feci", "facesti", "fece", "facemmo", "faceste", "fecero", "facessi", "facesse", "facessimo", "facessero", "facendo", "sto", "stai", "sta", "stiamo", "stanno", "stia", "stiate", "stiano", "starò", "starai", "starà", "staremo", "starete", "staranno", "starei", "staresti", "starebbe", "staremmo", "stareste", "starebbero", "stavo", "stavi", "stava", "stavamo", "stavate", "stavano", "stetti", "stesti", "stette", "stemmo", "steste", "stettero", "stessi", "stesse", "stessimo", "stessero", "stando", "sul", "sulla", "ti", "dello", "se", "re", "ai", "te", "to")

  val informationCleaning = true

  /**
    *
    * @param s
    * @return A single string which contains all tokens space separated
    */
  def tokenizer(s: String): String = s.split("\\s+")
    .filter(_.matches("""[a-zA-Z_]\w*"""))
    .map(_.toLowerCase).mkString(" ")

  def tokenizeToRankID(input:  RDD[List[String]]):   //It would be easier with an Array, but we cannot
  RDD[(String, String)]  = {

    val word_occ_map_all = input.flatMap(identity).filter(x => !this.stopwords.contains(x)).map((_, 1))
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
    * and the 3rd column is "author" and the 4th "title", it will add a column
    * an Int.toString where "0" is the most popular book author+title
    * "1" the second most popular etc.
    * (different books with same author+title will have same RankID)
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
//      val rankid_map = this.tokenizeToRankID()

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
}


