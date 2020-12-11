import com.danielasfregola.twitter4s.TwitterRestClient
import com.danielasfregola.twitter4s.entities.Tweet
import com.danielasfregola.twitter4s.entities.enums.ResultType
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Success

object SearchAndSaveTweets extends App {

  // TODO - Make sure to define your consumer and access tokens!
  val client = TwitterRestClient()

  val res = client.searchTweet("#cyberpunk2077",50,result_type = ResultType.Recent)

  res onComplete{
    case Success(tweet) => for (words <- tweet.data.statuses) { println(words.text)
      println("------------")}
  }
}