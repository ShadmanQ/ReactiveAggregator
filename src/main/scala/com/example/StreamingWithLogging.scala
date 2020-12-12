package streaming

import com.danielasfregola.twitter4s.TwitterStreamingClient
import com.danielasfregola.twitter4s.entities.Tweet
import com.danielasfregola.twitter4s.processors.TwitterProcessor._
import com.typesafe.scalalogging.LazyLogging
import akka.stream.scaladsl._
import akka.actor.Actor
import akka.Done
import akka.actor.ActorRef
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.CompletionStrategy
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Props
import akka.actor.PoisonPill
import akka.event.Logging
import streaming.StreamingWithLogging.{streamingClient, trackedWords}


class Passer(actor: Actor) extends Actor {
  var receiver: Actor = actor

  def receive = {
    case i => {
      receiver.self ! i
    }
  }
}

  object StreamingWithLogging extends App with LazyLogging {
    implicit val system = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    // Make sure to define the following env variables:
    // TWITTER_CONSUMER_TOKEN_KEY and TWITTER_CONSUMER_TOKEN_SECRET
    // TWITTER_ACCESS_TOKEN_KEY and TWITTER_ACCESS_TOKEN_SECRET
    val streamingClient = TwitterStreamingClient()

    // change this to whichever words you want to track
    val trackedWords = Seq("#covid")

    // logger is accessible with trait LazyLogging - see https://github.com/typesafehub/scala-logging
    // backend used in project is logback - https://logback.qos.ch/
    // configuration for logback is logback.xml in resources
    logger.info(s"Launching streaming session with tracked keywords: $trackedWords")

    val source: Source[Any, ActorRef] = Source.actorRef(
      completionMatcher = {
        case Done =>
          // complete stream immediately if we send it Done
          CompletionStrategy.immediately
      },
      // never fail the stream because of a message
      failureMatcher = PartialFunction.empty,
      bufferSize = 100,
      overflowStrategy = OverflowStrategy.dropHead)
    val actorRef: ActorRef = source.to(Sink.foreach(println)).run()

    val tweets = streamingClient.filterStatuses(tracks = trackedWords) {
      case tweet: Tweet => actorRef ! tweet
    }
  }

  // demo purpose only, do not use logging to serialize tweets :)