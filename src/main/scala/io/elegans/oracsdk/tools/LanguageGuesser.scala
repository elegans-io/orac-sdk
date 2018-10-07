package io.elegans.oracsdk.tools

import org.apache.tika.langdetect.OptimaizeLangDetector
import org.apache.tika.language.detect.{LanguageDetector, LanguageResult}

object LanguageGuesser {
  def guessLanguage(inputText: String): (String, Double, String, Boolean) = {
    val detector: LanguageDetector = new OptimaizeLangDetector().loadModels()
    val result: LanguageResult = detector.detect(inputText)
    (result.getLanguage, result.getRawScore, result.getConfidence.name, detector.hasEnoughText)
  }

  def getLanguages(languageCode: String /*ISO 639-1 name for language*/): Boolean = {
    val detector: LanguageDetector = new OptimaizeLangDetector().loadModels()
    detector.hasModel(languageCode)
  }
}
