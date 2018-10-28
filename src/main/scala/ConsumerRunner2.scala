import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.actor.FSM.Failure
import akka.actor.Status.Success
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink}
import akka.util.ByteString
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.Future

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

  val firstOne: Flow[CommittableMessage[String, Array[Byte]], Int, NotUsed] =
    Flow[CommittableMessage[String, Array[Byte]]]
      .take(1)
      .map(msg => {
        val m = ByteString(msg.record.value())
        //println(s"${m.map(_.toChar).mkString("")}")
        println(s"Offset: ${msg.record.offset()} ")
        1
      })

  val transformToStringFlow: Flow[CommittableMessage[String, Array[Byte]], ByteString, NotUsed] =
    Flow[CommittableMessage[String, Array[Byte]]]
      .map(msg => {
        val m = ByteString(msg.record.value())
        println(s"${m.map(_.toChar).mkString("")}")
        m
      })
//
//    val source = Consumer
//      .committableSource(consumerSettings, Subscriptions.topics("test"))

//    val res : Future[Int] = source
//      .take(1)
//      .via(firstOne)
//      .runFold(0)(_ + _)
//
//    res.map(r => {
//      println(r)
//    })

  import akka.actor.ActorRef
  import akka.kafka.KafkaConsumerActor
  import akka.kafka.Metadata
  import akka.pattern.ask
  import akka.util.Timeout

  import scala.concurrent.Future
  import scala.concurrent.duration._

  implicit val timeout = Timeout(5.seconds)

  val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

  // ... create source ...


  val topicPartition = new TopicPartition("test", 0)
  val offset: Future[Metadata.CommittedOffset] = ( consumer ? Metadata.GetCommittedOffset( topicPartition ) ).mapTo[Metadata.CommittedOffset]
  offset.map(meta => {
    meta.response.foreach(response => {
      println( s"Response: ${response.offset()} ")
    })
  })

  //  val topicsFuture: Future[Metadata.PartitionsFor] = (consumer ? Metadata.GetPartitionsFor("test") ).mapTo[Metadata.PartitionsFor]
//  topicsFuture.map(metaData => {
//    metaData.response.foreach(response => {
//      response.foreach(p => {
//
//        println(s"Partition: ${p.partition()} - ${p.topic()}")
//
//
//      })
//      //println(s"Result: ${r}")
//    })
//  })

//    val control =
//      Consumer
//        .committableSource(consumerSettings, Subscriptions.topics("test"))
//        .mapAsync(10) { msg =>
//          business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
//        }
//        .mapAsync(5)(offset => offset.commitScaladsl())
//        .toMat(Sink.ignore)(Keep.both)
//        .mapMaterializedValue(DrainingControl.apply)
//        .run()


    def business(key: String, value: Array[Byte]): Future[Done] = Future{
      val m = ByteString(value)
      println(s"${m.map(_.toChar).mkString("")}")
      Done
    }


//  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
//    import akka.stream.scaladsl.GraphDSL.Implicits._
//
//    val bcast = b.add(Broadcast[CommittableMessage[String, Array[Byte]]](2, eagerCancel = false))
//
//    val source = Consumer
//      .committableSource(consumerSettings, Subscriptions.topics("test"))
//
//    val out = Sink.ignore
//
//    source ~> bcast.in
//
//    bcast.out(0) ~> firstOne ~> out
//
//    bcast.out(1) ~> transformToStringFlow ~> out
//
//    ClosedShape
//  })
//
//  g.run()
}
