package io.elegans.oracsdk.tools

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

/* import core nlp */
import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.pipeline._

/* these are necessary since core nlp is a java library */
import scala.collection.JavaConversions._
import java.util.Properties

class TextProcessingUtils {

  /** Instantiate a StanfordCoreNLP pipeline
    *
    * @return an instance of StanfordCoreNLP
    */
  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
    pipeline
  }

  /** check if a string is only made by letters
    *
    * @param str the input string
    * @return true if the string is only letters, otherwise false
    */
  def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }

  /** tokenize a string, optionally remove stopwords using StanfordCoreNLP
    *
    * @param text the input text
    * @param stopWords the set of stopwords
    * @param pipeline the instance of StanfordCoreNLP
    * @return a List of lowercase token
    */
  def plainTextToLemmas(text: String, stopWords: Broadcast[Set[String]],
                        pipeline: StanfordCoreNLP): List[String] = {
    val doc: Annotation = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences;
         token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.getString(classOf[LemmaAnnotation])
      val lc_lemma = lemma.toLowerCase
      if (!stopWords.value.contains(lc_lemma) && isOnlyLetters(lc_lemma)) {
        lemmas += lc_lemma.toLowerCase
      }
    }
    lemmas.toList
  }

  /** tokenize a sentence
    *
    * @param text the input string
    * @param stopWords the set of stopwords
    * @return a list of tokens
    */
  def tokenizeSentence(text: String, stopWords: Broadcast[Set[String]]): List[String] = {
    try {
      val pipeline = createNLPPipeline() /* get an instance of the NLP pipeline */
      val doc_lemmas : List[String] =  /* call the tokenization function */
        plainTextToLemmas(text, stopWords, pipeline)
      doc_lemmas /* return the original sentence annotated with tokens */
    } catch {
      case e: Exception => List.empty[String]
    }
  }

}
