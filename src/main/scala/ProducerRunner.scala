package com.avelresearch.runners

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future
import scala.util.{Success, Failure}

object ProducerRunner extends App {

  implicit val system = ActorSystem("MyActorSystem")

  implicit val ec = system.dispatcher

  implicit val materializer = ActorMaterializer()

  val config = system.settings.config.getConfig("akka.kafka.producer")

  val producerSettings =
    ProducerSettings(config, new StringSerializer, new StringSerializer)
      .withBootstrapServers("192.168.0.8:9092")

  val done: Future[Done] =
    Source(1 to 10000)
      .map(_.toString)
      .map(value => new ProducerRecord[String, String]("test", s"{ message_id: $value }"))
      .runWith( Producer.plainSink(producerSettings))

  done.onComplete {
      case Failure(e) => system.terminate()
      case Success(_) => system.terminate()
    }

}

