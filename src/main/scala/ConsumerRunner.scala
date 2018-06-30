package com.avelresearch.runners

import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
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
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


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

  val consumerControl = db.loadOffset()
    .map { fromOffset =>
      Consumer
      .plainSource(
        consumerSettings,
        Subscriptions.assignmentWithOffset(new TopicPartition("test", /* partition = */ 0) -> fromOffset)
      )
      .mapAsync(1)(db.businessLogicAndStoreOffset)
      .toMat(Sink.ignore)(Keep.both)
      .run()

  }

  //control.foreach(c => terminateWhenDone(c.shutdown()))


  class OffsetStore {

    private val offset = new AtomicLong

    def businessLogicAndStoreOffset(record: ConsumerRecord[String, Array[Byte]]): Future[Done] =
    {

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