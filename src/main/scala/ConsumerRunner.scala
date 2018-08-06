package com.avelresearch.runners

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

import akka.actor.Actor.Receive
import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, Status}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.javadsl.Consumer.Control
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer, StringSerializer}

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

object ConsumerRunner extends App {

  implicit val system = ActorSystem("MyActorSystem")

  implicit val ec = system.dispatcher

  implicit val materializer = ActorMaterializer()

  val config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers("192.168.0.8:9092")
      .withGroupId("group1")
      //.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")

  class RebalanceListener extends Actor with ActorLogging {
    def receive: Receive = {
      case TopicPartitionsAssigned(sub, assigned) ⇒
        log.info("Assigned: {}", assigned)

      case TopicPartitionsRevoked(sub, revoked) ⇒
        log.info("Revoked: {}", revoked)
    }
  }

  val rebalanceListener = system.actorOf(Props[RebalanceListener])

  // Offset Storage external to Kafka
  val db = new OffsetStore

  //  val control =
  //    Consumer
  //      .atMostOnceSource(consumerSettings, Subscriptions.topics("test"))
  //      .mapAsync(1)(record => business(record.key, record.value()))
  //      .to(Sink.foreach(it => println(s"Done with $it")))
  //      .run()
  //  // #atMostOnce
  //
  //  def business(key: String, value: Array[Byte]): Future[Done] = {
  //    val recordAsStr = new String(value, "UTF-8")
  //    println(s"Record: $key $recordAsStr")
  //    Future.successful(Done)
  //  }

  //terminateWhenDone(control.shutdown())




  //  val consumerControl = db.loadOffset()
  //    .map { fromOffset =>
  //      Consumer.plainSource(
  //        consumerSettings,
  //        Subscriptions.assignmentWithOffset(new TopicPartition("test", /* partition = */ 0) -> fromOffset)
  //      )
  //        .mapAsync(1)(db.businessLogicAndStoreOffset)
  //        .toMat(Sink.ignore)(Keep.both)
  //        .run()
  //    }

  val subscription = Subscriptions
    .topics(Set("test"))
    // additionally, pass the actor reference:
    .withRebalanceListener(rebalanceListener)

  //val control =
    //Consumer
      //.committableSource(consumerSettings, subscription)
      //.mapAsync(10) { msg =>
       // business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
      //}
      //.mapAsync(5)(offset => offset.commitScaladsl())
      //.toMat( FileIO.toPath( Paths.get("/Users/pavel/Sources/factorials.txt") )  )(Keep.both)
      //.mapMaterializedValue(DrainingControl.apply)
      //.run()

  //.to( FileIO.toPath( Paths.get("/Users/pavel/Sources/factorials.txt") ) )
  //      .mapAsync(5)(offset => offset.commitScaladsl())
  //      .toMat( Sink.ignore )(Keep.both)
  //      .mapMaterializedValue(DrainingControl.apply)
  //      .run()


  val count: Flow[ CommittableMessage[String,Array[Byte] ], Int, NotUsed] = Flow[CommittableMessage[String,Array[Byte] ]].map(_ ⇒ 1)

  val countFlow : Flow[ Int, Int, NotUsed] = Flow[ Int ].fold(0)((a,b) => a + b)

  val countString: Sink[Int, Future[Int] ] = Sink.fold[Int, Int](0)( (a,b) =>  a + b )

  val transformToStringFlow: Flow[ CommittableMessage[String, Array[Byte] ], ByteString, NotUsed] =
    Flow[ CommittableMessage[String, Array[Byte] ] ].map(msg => ByteString( msg.record.value() ) )

  val kafkaSink = Sink.ignore

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import akka.stream.scaladsl.GraphDSL.Implicits._

    val bcast = b.add(Broadcast[ CommittableMessage[String,Array[Byte] ] ](2))
    Consumer.committableSource(consumerSettings, subscription) ~> bcast.in

    bcast.out(0) ~> count  ~> countFlow ~> Flow[Int].map(s => ByteString(s.toByte) ) ~> FileIO.toPath( Paths.get("/Users/pavel/Sources/count.txt") )

    bcast.out(1) ~> transformToStringFlow ~> FileIO.toPath( Paths.get("/Users/pavel/Sources/messages.txt") )

    ClosedShape
  })

  val res = g.run()

  def business(key: String, value: Array[Byte]): Future[Done] = Future {
    println(s"key: $key value: ${value.map(_.toChar).mkString("")}")
    Done
  }

  //control.foreach(c => terminateWhenDone(c.shutdown()))
  class OffsetStore {

    private val offset = new AtomicLong

    def businessLogicAndStoreOffset(record: ConsumerRecord[String, Array[Byte]]): Future[Done] = {

      val recordAsStr = new String(record.value(), "UTF-8")
      println(s"DB.save: ${recordAsStr}")

      offset.set(record.offset)

      Future.successful(Done)

    }

    def loadOffset(): Future[Long] = Future.successful(offset.get)

  }

  def terminateWhenDone(result: Future[Done]): Unit =
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }


}