import java.nio.file.Paths

import ProducerRunner.{done, system}
import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, Status}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import com.avelresearch.runners.ConsumerRunner.{consumerSettings, subscription, system}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer, StringSerializer}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ConsumerRunner2 extends App {

  implicit val system = ActorSystem("MyActorSystem")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val bootstrapServers = "localhost:9092"

  val config = system.settings.config.getConfig("akka.kafka.consumer")

  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId("group1")
      //.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")


  val firstOne: Flow[ CommittableMessage[String,Array[Byte] ], Int, NotUsed] = Flow[CommittableMessage[String,Array[Byte] ]].take(1).map{
    msg =>
      {
        println(msg.record.offset())
        1
      }
  }

//  val countFlow : Flow[ Int, Int, NotUsed] = Flow[ Int ].fold(0)((a,b) => a + b)
//
//  val countString: Sink[Int, Future[Int] ] = Sink.fold[Int, Int](0)( (a,b) =>  a + b )

  val transformToStringFlow: Flow[ CommittableMessage[String, Array[Byte] ], ByteString, NotUsed] =
    Flow[ CommittableMessage[String, Array[Byte] ] ].map(msg => {
      val m = ByteString( msg.record.value() )
      println( s"${m.map(_.toChar).mkString("")}" )
      m
    } )

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import akka.stream.scaladsl.GraphDSL.Implicits._

    val bcast = b.add(Broadcast[ CommittableMessage[String,Array[Byte] ] ](2))

    val source = Consumer
      .committableSource(consumerSettings, Subscriptions.topics("test"))

    source ~> bcast.in

    bcast.out(0) ~> firstOne  ~> Sink.ignore

    bcast.out(1) ~> transformToStringFlow ~> Flow[_].mapAsync(1) ~> Sink.ignore


    ClosedShape
  })

  g.run()

}
