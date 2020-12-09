import com.danielasfregola.twitter4s.TwitterRestClient
import com.danielasfregola.twitter4s.TwitterStreamingClient
import com.danielasfregola.twitter4s.entities.Tweet
import com.danielasfregola.twitter4s.entities.enums.ResultType
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future //gonna need to look up how to use futures


//this is gonna be useful https://github.com/DanielaSfregola/twitter4s-demo/blob/master/src/main/scala/rest/SearchAndSaveTweets.scala

object  main extends  App{
  val restClient = TwitterRestClient()
  val streamingClient = TwitterStreamingClient()
  println("hi")//access secret
  streamingClient.


}