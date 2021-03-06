/**
 * @author Ernesto Gutiérrez Corona- ernesto.g.corona@gmail.com
 */

package tinga.nlp.texttools

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.Map
import scala.collection.mutable.Buffer
import java.io.InputStreamReader
import java.io.BufferedReader

/** Object for preprocessing text
 *
 *  It can be customized for english: "en", spanish: "es", french: "fr", italian: "it" and german: "de"
 */
object TextPreprocessor{

  val punctuationChars = List('.',',',';',':','¡','!','¿','?','(',')','[',']','{','}','`',
                              '\\','\'','@','#','$','^','&','*','+','-','|','=','_','~','%',
                              '<','>','/', '"')

  var lexiconDir = "lexicon/"

  def readAsStream(path: String): List[String] = {
    val is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)
    val isr = new InputStreamReader(is)
    val br = new BufferedReader(isr)
    var lines = List[String]()
    var line = br.readLine
    while(line != null){
      lines = line :: lines
      line = br.readLine
    }
    br.close
    isr.close
    is.close
    lines
  }

  def readFileToMap(path: String): Map[String,String] = {
    //Lprintln(path)
    val lines = readAsStream(path)
    var map: Map[String, String] = Map[String, String]() //new java.util.HashMap[String, String]
    for(line <- lines) {
      if (line.split(" ").length == 2)
        map += line.split(" ")(0) -> line.split(" ")(1)
    }
    map
  }

  def readFileToStringList(path: String): List[String] = {
    readAsStream(path)
  }

  def readFileToCharList(path: String): List[Char] = {
    readAsStream(path).flatMap(c => c.toCharArray)
  }

  def langSpecificChars(lang: String): List[Char] = {
    readFileToCharList(lexiconDir + f"special-characters/$lang%s-characters.txt")
  }

  def langStopwords(lang: String): List[String] = {
    readFileToStringList(lexiconDir + f"stopwords/$lang%s-stopwords.txt")
  }

  /** Preprocess text customized by language
   *
   * @return String optionally cleaned from punctuation (with exceptions) and stopwords (with exceptions)
   */
  def preprocess(lang: String)(text: String,
                               punctuation: Boolean = false,
                               exceptPunct: List[Char] = List(),
                               stopwords: Boolean = false,
                               exceptStop: List[String] = List()): String = lang match {
    case "en" => cleanText(text, List(),
                           punctuation, exceptPunct,
                           stopwords, langStopwords("en"), exceptStop)
    case "es" => cleanText(text, langSpecificChars("es"),
                           punctuation, exceptPunct,
                           stopwords, langStopwords("es"), exceptStop)
    case "fr" => cleanText(text, langSpecificChars("fr"),
                           punctuation, exceptPunct,
                           stopwords, langStopwords("fr"), exceptStop)
    case "it" => cleanText(text, langSpecificChars("it"),
                           punctuation, exceptPunct,
                           stopwords, langStopwords("it"), exceptStop)
    case "de" => cleanText(text, langSpecificChars("de"),
                           punctuation, exceptPunct,
                           stopwords, langStopwords("de"), exceptStop)
  }

  def cleanText(text: String, langChars: List[Char],
                punctuation: Boolean, exceptPunct: List[Char],
                stopwords: Boolean, langStopwords: List[String], exceptStop: List[String]): String = {
    if (punctuation && !stopwords) {
      text filter { (c: Char) => (isAllowedChar(c, langChars)) &&
                                 (!(punctuationChars contains c) ||
                                 (exceptPunct contains c)) }
    }
    else{
        if (stopwords) {
          val punctDeleted = text filter { (c: Char) => (isAllowedChar(c, langChars)) &&
                                                        (!(punctuationChars contains c) ||
                                                        (exceptPunct contains c)) }
          val wordList = punctDeleted.split(' ').toList map (str => str.toLowerCase.trim)
          val stopwordsRemoved = wordList filter { (str: String) => (!(langStopwords contains str) ||
                                                                    (exceptStop contains str)) }
          stopwordsRemoved.mkString(" ")
        }
        else text filter { (c: Char) => isAllowedChar(c, langChars) }
    }

  }

  def isAllowedChar(c: Char, chars: List[Char]) = c <= '~' || chars.contains(c)

  def removeDiacritics(str: String): String = {
    val diacriticChars = "ÀàÈèÌìÒòÙùÁáÉéÍíÓóÚúÝýÂâÊêÎîÔôÛûŶŷÃãÕõÑñÄäËëÏïÖöÜüŸÿÅåÇçŐőŰű".toCharArray
    val asciiChars     = "AaEeIiOoUuAaEeIiOoUuYyAaEeIiOoUuYyAaOoNnAaEeIiOoUuYyAaCcOoUu".toCharArray
    str map (c => if(diacriticChars contains c) asciiChars(diacriticChars.indexOf(c)) else c)
  }
}
