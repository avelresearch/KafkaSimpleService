package com.avelresearch.runners

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ProducerRunner extends App {

  implicit val system = ActorSystem("MyActorSystem")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val config = system.settings.config.getConfig("akka.kafka.producer")

  val producerSettings = ProducerSettings(config, new StringSerializer, new StringSerializer)
      .withBootstrapServers("192.168.0.8:9092")

  val source = Source[String]( (1 to 50000).map(_.toString())  )

  val count: Flow[String, Int, NotUsed] = Flow[String].map(_ ⇒ 1)

  val countString: Sink[Int, Future[Int] ] = Sink.fold[Int, Int](0)( (a,b) =>  a + b )

  val mapFromConsumerRecord = Flow[String]
    .map(value => new ProducerRecord[String, String]("test", s"{ message_id: $value }") )

  val kafkaSink = Producer.plainSink(producerSettings)

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import akka.stream.scaladsl.GraphDSL.Implicits._

    val bcast = b.add(Broadcast[String](2))
    source ~> bcast.in

    bcast.out(0) ~> count  ~> countString
    bcast.out(1) ~> mapFromConsumerRecord  ~> kafkaSink

    ClosedShape
  })
  val res = g.run()


//  val done: Future[Done] =
//       source
//         .map(_.toString)
//      .map(value => new ProducerRecord[String, String]("test", s"{ message_id: $value }"))
//      .runWith( Producer.plainSink(producerSettings))
//
//
//  done.onComplete {
//      case Failure(e) => system.terminate()
//      case Success(_) => system.terminate()
//    }

}

