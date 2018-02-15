package io.elegans.oracsdk.etl

import org.apache.spark.rdd.RDD

class Transformer {

  // togo into file
  val stopwords: List[String] = List("cd", "c", "libro", "audio", "ediz", "edizione", "italiana", "spagnola", "portoghese", "edizione", "prima", "seconda", "terza", "quarta", "quinta", "the", "a", "adesso", "ai", "al", "alla", "allo", "allora", "altre", "altri", "altro", "anche", "ancora", "avere", "aveva", "avevano", "ben", "buono", "che", "chi", "cinque", "comprare", "con", "consecutivi", "consecutivo", "cosa", "cui", "da", "del", "della", "dello", "dentro", "deve", "devo", "di", "doppio", "due", "e", "ecco", "fare", "fine", "fino", "fra", "gente", "giu", "ha", "hai", "hanno", "ho", "il", "indietro", "invece", "io", "la", "lavoro", "le", "lei", "lo", "loro", "lui", "lungo", "ma", "me", "meglio", "molta", "molti", "molto", "nei", "nella", "no", "noi", "nome", "nostro", "nove", "nuovi", "nuovo", "o", "oltre", "ora", "otto", "peggio", "pero", "persone", "piu", "poco", "primo", "promesso", "qua", "quarto", "quasi", "quattro", "quello", "questo", "qui", "quindi", "quinto", "rispetto", "sara", "secondo", "sei", "sembra", "sembrava", "senza", "sette", "sia", "siamo", "siete", "solo", "sono", "sopra", "soprattutto", "sotto", "stati", "stato", "stesso", "su", "subito", "sul", "sulla", "tanto", "te", "tempo", "terzo", "tra", "tre", "triplo", "ultimo", "un", "una", "uno", "va", "vai", "voi", "volte", "vostro", "ad", "al", "allo", "ai", "agli", "all", "agl", "alla", "alle", "con", "col", "coi", "da", "dal", "dallo", "dai", "dagli", "dall", "dagl", "dalla", "dalle", "di", "del", "dello", "dei", "degli", "dell", "degl", "della", "delle", "in", "nel", "nello", "nei", "negli", "nell", "negl", "nella", "nelle", "su", "sul", "sullo", "sui", "sugli", "sull", "sugl", "sulla", "sulle", "per", "tra", "contro", "io", "tu", "lui", "lei", "noi", "voi", "loro", "mio", "mia", "miei", "mie", "tuo", "tua", "tuoi", "tue", "suo", "sua", "suoi", "sue", "nostro", "nostra", "nostri", "nostre", "vostro", "vostra", "vostri", "vostre", "mi", "ti", "ci", "vi", "lo", "la", "li", "le", "gli", "ne", "il", "un", "uno", "una", "ma", "ed", "se", "perché", "anche", "come", "dov", "dove", "che", "chi", "cui", "non", "più", "quale", "quanto", "quanti", "quanta", "quante", "quello", "quelli", "quella", "quelle", "questo", "questi", "questa", "queste", "si", "tutto", "tutti", "a", "c", "e", "i", "l", "o", "ho", "hai", "ha", "abbiamo", "avete", "hanno", "abbia", "abbiate", "abbiano", "avrò", "avrai", "avrà", "avremo", "avrete", "avranno", "avrei", "avresti", "avrebbe", "avremmo", "avreste", "avrebbero", "avevo", "avevi", "aveva", "avevamo", "avevate", "avevano", "ebbi", "avesti", "ebbe", "avemmo", "aveste", "ebbero", "avessi", "avesse", "avessimo", "avessero", "avendo", "avuto", "avuta", "avuti", "avute", "sono", "sei", "è", "siamo", "siete", "sia", "siate", "siano", "sarò", "sarai", "sarà", "saremo", "sarete", "saranno", "sarei", "saresti", "sarebbe", "saremmo", "sareste", "sarebbero", "ero", "eri", "era", "eravamo", "eravate", "erano", "fui", "fosti", "fu", "fummo", "foste", "furono", "fossi", "fosse", "fossimo", "fossero", "essendo", "faccio", "fai", "facciamo", "fanno", "faccia", "facciate", "facciano", "farò", "farai", "farà", "faremo", "farete", "faranno", "farei", "faresti", "farebbe", "faremmo", "fareste", "farebbero", "facevo", "facevi", "faceva", "facevamo", "facevate", "facevano", "feci", "facesti", "fece", "facemmo", "faceste", "fecero", "facessi", "facesse", "facessimo", "facessero", "facendo", "sto", "stai", "sta", "stiamo", "stanno", "stia", "stiate", "stiano", "starò", "starai", "starà", "staremo", "starete", "staranno", "starei", "staresti", "starebbe", "staremmo", "stareste", "starebbero", "stavo", "stavi", "stava", "stavamo", "stavate", "stavano", "stetti", "stesti", "stette", "stemmo", "steste", "stettero", "stessi", "stesse", "stessimo", "stessero", "stando", "sul", "sulla", "ti", "dello", "se", "re", "ai", "te", "to")

  val informationCleaning = true

  def tokenizer(s: String): String = s.split("\\s+").filter(_.matches("""[a-zA-Z_]\w*""")).map(_.toLowerCase).mkString(" ")

  def tokenizeToRankID(input:  RDD[List[String]]):   //It would be easier with an Array, but we cannot
  RDD[(String, String)]  = {

    val word_occ_map_all = input.flatMap(identity).filter(x => !this.stopwords.contains(x)).map((_, 1)).reduceByKey((a, b) => (a + b))

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
    * Substitute elements in a column with a numerical ID inversely
    * proportional to occurrence. Eg if RDD contains rows of books,
    * and the 3rd column is "author", SimpleRankID will substitute any
    * author with a Int.toString where "0" is the most popular author,
    * "1" the second most popular etc.
    * If replace, updates the array, otherwise appends
    *
    */
  def makeRankId(input: RDD[Array[String]], column: Int, replace: Boolean, tokenize: Boolean ): RDD[Array[String]] = {

    if (replace) {
      if (!tokenize) {
        val rankid_map = input.map(x => (x(column), 1)).reduceByKey((a, b) => a + b).sortBy(- _._2).map(x => x._1).zipWithIndex.map(x => (x._1, x._2.toString)).collect.toMap
        input.map(x => x.updated(column, rankid_map(x(column))))
      } else {
        val input_with_tokenized_column = input.map(x => x.updated(column, this.tokenizer(x(column))))
        // from String to clean List[String]
        //val tokenized_column = input.map(x => OracClientTokenizer(x(column)))
        val tokenized_column = input.map(x => x(column))
        // makes the map (Word, rankid)
        val rankid_map = tokenized_column.flatMap(line => line.split(" ")).map(x => (x, 1)).reduceByKey((a, b) => a + b).sortBy(- _._2).map(x => x._1).zipWithIndex.map(x => (x._1, x._2.toString)).collect.toMap
        input_with_tokenized_column.map(x => x.updated(column, x(column).split(" ").map(y => rankid_map.getOrElse(y, "")).mkString(", ")))
      }
    }
    else {
      val rankid_map = input.map(x => (x(column), 1)).reduceByKey((a, b) => a + b).sortBy(- _._2).map(x => x._1).zipWithIndex.map(x => (x._1, x._2.toString)).collect.toMap
      input.map(x => x ++ Array(rankid_map(x(column))))
    }
  }


  /** Join two RDD[Array[String]]
    * rdd1 = [username, action_date, ...]
    * rdd2 = [username, address, user_id]
    * val rdd3 = Join(input=rdd1, input_column=0, table=rdd2, lookup_columns=(0, 2), replace=true)
    * rdd3: [user_id, action_date, ...]
    *
    * If !replace append the result
    */

  def join(input: RDD[Array[String]], input_column: Int,
                     table: RDD[Array[String]], key_column: Int, value_column: Int,
                     replace: Boolean
                    ): RDD[Array[String]] = {
    if (replace)
      input.map(x => (x(input_column), x)).join(table.map(x => (x(key_column), x(value_column)))).map(x => x._2._1.updated(input_column, x._2._2))
    else
      input.map(x => (x(input_column), x)).join(table.map(x => (x(key_column), x(value_column)))).map(x => x._2._1 :+ x._2._2)
  }

}


