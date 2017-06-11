import com.cybozu.labs.langdetect.DetectorFactory
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import tingaUtils.Sentiment

import scala.util.Try

/**
  * Created by Thagus on 08/09/16.
  */
object Streaming {

  def main(args : Array[String]) {
    //System.setProperty("hadoop.home.dir", "C:\\winutils"); //Windows only
    val conf = new SparkConf()
      .setAppName("Twitter sentiment analysis")
      .setMaster("local[*]")//Create local instance of master with [*] slaves (usually means as much as possible)

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(2))

    System.setProperty("twitter4j.oauth.consumerKey", "b0MnjcNYQW5P1bhAaraPkqOzq");
    System.setProperty("twitter4j.oauth.consumerSecret", "Yp5vAhNoufS3rViENKKFdfzRuXzLKUxJjPtCWFTKSgKan8o1Hn");
    System.setProperty("twitter4j.oauth.accessToken", "741729530478354432-T2sxs34VHwnSkQIYA4EJ1YvPjhPPchM");
    System.setProperty("twitter4j.oauth.accessTokenSecret", "geN0qPgRvW4NCStv2t5GWE6HmNf2VYqXAvwANOt4HxuI4");

    //Load language profiles folder from resources
    DetectorFactory.loadProfile(this.getClass.getResource("languageProfiles").getFile)

    val keywords = Array( "Puebla" )//filter words
    val tweets = TwitterUtils.createStream(ssc, None, keywords)   //Create the stream with the

    def filterLanguage(map: Map[String, Any]): Boolean = {
      //!map.exists(_ == ("language","unknown"))  //Filters unknown languages
      //map.exists(_ == ("language","en")) || map.exists(_ == ("language","es"))  //Allow only english or spanish
      map.exists(_ == ("language","es"))  //Allow only spanish
    }

    def printTweet(map : Map[String, Any]): Unit = {
      println(map.toString())   //Print the map as a single string
    }

    tweets.foreachRDD{(rdd, time) =>
      rdd.map(t => {
        val text = (t.getText) //The text to evaluate is the tweet text itself. We could use the onlyWords method to extract only words
        Map(
          "user"-> t.getUser.getScreenName,
          "created_at" -> t.getCreatedAt.toInstant.toString,
          "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
          "text" -> text,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          "retweet" -> t.getRetweetCount,
          "language" -> detectLanguage(text),
          "sentiment" -> (tingaUtils.Sentiment.score(tingaUtils.Sentiment.clean(t.getText))._1 match {
                            case -2.0 => "N+"
                            case -1.0 => "N"
                            case 0.0 => "NEU"
                            case 1.0 => "P"
                            case 2.0 => "P+"
                          })
        )
      }).filter(filterLanguage).foreach(printTweet)//.saveAsTextFile("./test")  //saveAsTextFile only logs the current RDD state on a file, is not persistent
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def detectLanguage(text: String) : String = {
    Try {
      val detector = DetectorFactory.create()
      detector.append(text)
      detector.detect()
    }.getOrElse("unknown")
  }

}
