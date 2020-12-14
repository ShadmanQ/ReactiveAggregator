package streaming

import com.danielasfregola.twitter4s.TwitterStreamingClient
import com.danielasfregola.twitter4s.entities.Tweet
import com.danielasfregola.twitter4s.processors.TwitterProcessor._
import com.typesafe.scalalogging.LazyLogging
import akka.stream.scaladsl._
import akka.actor.Actor
import akka.{Done, NotUsed}
import akka.actor.ActorRef
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.CompletionStrategy
import akka.actor.ActorSystem
import akka.actor.PoisonPill

import scala.collection.mutable.Map
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Props
import akka.actor.PoisonPill
import akka.event.Logging
import streaming.StreamingWithLogging.{materializer, streamingClient}

import scala.collection.mutable


class Tweeter(hub: Sink[String, NotUsed], trackedWord: String) extends Actor {
  var toConsume = hub
  val streamingClient = TwitterStreamingClient()

  val source: Source[String, ActorRef] = Source.actorRef(
    completionMatcher = {
      case Done =>
        // complete stream immediately if we send it Done
        CompletionStrategy.immediately
    },
    // never fail the stream because of a message
    failureMatcher = PartialFunction.empty,
    bufferSize = 100,
    overflowStrategy = OverflowStrategy.dropHead)

  val receiver = source.to(toConsume).run()
  val tweets = streamingClient.filterStatuses(tracks = Seq(trackedWord)) {
    case tweet: Tweet => receiver ! tweet
  }

  def receive = {
    case i => {
      println("I should not be getting this")
      println("killing self")
      self ! PoisonPill
    }
  }
}

class HashManager(seed1: String, seed2: String,man1: ActorRef, man2: ActorRef) extends Actor{
  var table1: Map[String,Int] = Map(seed1 -> 0).withDefaultValue(0)
  var table2: Map[String,Int] = Map(seed2 -> 0).withDefaultValue(0)
  man1 ! Seq((seed1,0))
  man2 ! Seq((seed2,0))

  def receive = {
    case i: Tweet => {
      println("Testing Stuff +++++++++++++++++++")
      //println(i.extended_tweet)
      val tags =(i.entities.get.hashtags)
     // val text = i.text
    //  val parts = text.toString().split(" ")
      //println(parts.toString)
      var hashtags: List[String] = List()
      for (tag <-tags) {
        hashtags = hashtags.concat(List(tag.text))
      }
//      for (part <- parts) {
//   //     println(part.toString())
//        //hashtags = hashtags.concat(List("#testing"))
//        if (part.size > 0 && part(0) == "#") {
//          println(part)
//          hashtags = hashtags.concat(List(part))
//        }
//      }
      println("Now printing hashtags")
      println(hashtags)
      println("Here is table1")
      println(table1)
      println("Here is table2")
      println(table2)
      for (tag <- hashtags){
        if (table1.contains(tag)) table1(tag)+=1 else table1 +=(tag->1)
      }
//      if (hashtags.contains(seed1)) {
//        for (tag <- hashtags) table1(tag) += 1
//      }
//      if (hashtags.contains(seed2)) {
//        for (tag <- hashtags) table2(tag) += 1
//      }
      if (table1(seed1) % 10 == 0) {
        println(table1)
        println("---------------------------------------------")
        man1 ! top5(table1)
      }
      if (table2(seed2) % 10 == 0) {
        println(table2)
        println("---------------------------------------------")
        man2 ! top5(table2)
      }
    }
  }

  def top5(value: mutable.Map[String, Int]): Unit ={
    if (value.size < 5) {
      println("top 5 less than five")
      val top = value.toSeq.sortBy(_._2).reverse
      println(top)
    }
    else {
      val top = value.toSeq.sortBy(_._2).reverse
      println("Here are the top5")
      println(top.slice(0,5))
    }
  }
}

class TweetManager(hub: Sink[String,NotUsed]) extends Actor{
  val toConsume = hub
  var tweeters: Map[String,ActorRef] = Map()
  //receive method override to hashtags from tweet and add it to a related hashtags table
  def receive = {
    case i: Seq[(String,Int)] => {
      for ((key,value) <- tweeters) {
        if (!(i.map(_._1).contains(key))) {
          tweeters - key
          //value ! PoisonPill
          value ! "kys"
        }
      }
      for (hashtag <- i) {
        if (!(tweeters.contains(hashtag._1))) {
          val newActor = context.actorOf(Props(new Tweeter(toConsume,hashtag._1)))
          tweeters(hashtag._1) = newActor
        }
      }
    }
  }
}

  object StreamingWithLogging extends App with LazyLogging {
    implicit val system = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    // Make sure to define the following env variables:
    // TWITTER_CONSUMER_TOKEN_KEY and TWITTER_CONSUMER_TOKEN_SECRET
    // TWITTER_ACCESS_TOKEN_KEY and TWITTER_ACCESS_TOKEN_SECRE
    val streamingClient = TwitterStreamingClient()
    // change this to whichever words you want to track
    val trackedWords = Seq("#covid")

    // logger is accessible with trait LazyLogging - see https://github.com/typesafehub/scala-logging
    // backend used in project is logback - https://logback.qos.ch/
    // configuration for logback is logback.xml in resources
    logger.info(s"Launching streaming session with tracked keywords: $trackedWords")

    //val tweeter = system.actorOf(Props(new Tweeter(actorRef,Seq("#covid"))),name = "tweeter")
    // A simple consumer that will print to the console for now
    val consumer = Sink.foreach(println)

    // Attach a MergeHub Source to the consumer. This will materialize to a
    // corresponding Sink.
    val (sink, source) = {
    MergeHub.source[String](perProducerBufferSize = 16).toMat(BroadcastHub.sink(bufferSize = 256))(Keep.both).run()
    }
    source.runWith(Sink.ignore)

    // By running/materializing the consumer we get back a Sink, and hence
    // now have access to feed elements into it. This Sink can be materialized
    // any number of times, and every element that enters the Sink will
    // be consumed by our consumer.
    val onErrorMessage = (ex: Throwable) => "error"
    val man1 = system.actorOf(Props(new TweetManager(sink))) //actor object
    val man2 = system.actorOf(Props(new TweetManager(sink))) //actor object
    val hashManager = system.actorOf(Props(new HashManager("#Covid","#SCOTUS",man1,man2)))
    val passer = Sink.actorRef(hashManager,"hello")
    source.runWith(passer)
    //source.runWith(Sink.foreach(println))
  }

  // demo purpose only, do not use logging to serialize tweets :)