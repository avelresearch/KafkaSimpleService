import ConsumerRunner2.RebalanceListener.{ExtractLastCommittedOffsets}
import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}


object ConsumerRunner2 extends App {

  implicit val system = ActorSystem("MyActorSystem")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val bootstrapServers = "10.81.19.213:9092"
  val config = system.settings.config.getConfig("akka.kafka.consumer")

  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId("event-tokeniser-aadab")
      //.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")

  import akka.actor.ActorRef
  import akka.kafka.KafkaConsumerActor
  import akka.kafka.Metadata
  import akka.pattern.ask
  import akka.util.Timeout

  import scala.concurrent.Future
  import scala.concurrent.duration._


  val topicName = "raw"


  implicit val timeout = Timeout(5.seconds)

  val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))


//  val topicsFuture: Future[Metadata.Topics] = (consumer ? Metadata.ListTopics).mapTo[Metadata.Topics]
//
//  topicsFuture.map(_.response.foreach { map =>
//    map.foreach {
//      case (topic, partitionInfo) => if (topic.equals("raw"))
//        partitionInfo.foreach { info =>
//          println(s"$topic: $info")
//        }
//    }
//  })

  object RebalanceListener {
    case class ExtractLastCommittedOffsets(partition: Set[TopicPartition])
  }

  class RebalanceListener(client : ActorRef) extends Actor with ActorLogging {
    def receive: Receive = {
      case TopicPartitionsAssigned(sub, assigned) ⇒ {
        println("Extract Partition offsets")

        log.info("Assigned: {}", assigned)

          assigned.foreach(assigned => {
            val partition = assigned.partition()

            val topicPartition = new TopicPartition(topicName, partition)

            val offset: Future[Metadata.CommittedOffset] = ( client ? Metadata.GetCommittedOffset( topicPartition ) ).mapTo[Metadata.CommittedOffset]
            offset.map(meta => {
              meta.response.foreach(response => {
                println( s"Response: ${response.offset()} ")
              })
            })

          })

      }
        //self ! ExtractLastCommittedOffsets(assigned)

      case TopicPartitionsRevoked(sub, revoked) ⇒
        log.info("Revoked: {}", revoked)

    }
  }

  val rebalanceListener = system.actorOf(Props( new RebalanceListener(consumer)) )

  val subscription = Subscriptions
    .topics(Set(topicName))
    // additionally, pass the actor reference:
    .withRebalanceListener(rebalanceListener)


  val control =
    Consumer
      .committableSource(consumerSettings, subscription)
      .mapAsync(10) { msg =>
        business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
      }
      .mapAsync(5)(offset => offset.commitScaladsl())
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()

  def business(key: String, value: Array[Byte]): Future[Done] = Future {
    println(s"Key: $key - Value: ${value.map(_.toChar).mkString("")}")
    Done
  }


//    val partitionsList = new ListBuffer[Int]()
//
//    val clientConsumer = consumerSettings.createKafkaConsumer()
//    clientConsumer.subscribe( util.Arrays.asList(topicName) )
//
//    val record = clientConsumer.poll(10)
//
//    val p = clientConsumer.assignment()
//
//    println("Assigned partitions")
//
//    println( p.toArray.mkString("\n") )
//    println("End")
//    clientConsumer.close()


  //val partitions: Future[Metadata.PartitionsFor] = ( consumer ? Metadata.GetPartitionsFor( topicName ) ).mapTo[Metadata.PartitionsFor]

  /*
  partitions.map(meta => {
    meta.response.foreach(res => {
      println("Response")

      res.foreach(item => {
        partitionsList.append(item.partition())
        println( s"Partition: ${item.partition()} ")
      })



      partitionsList.foreach(partition => {

        val topicPartition = new TopicPartition("raw", partition)

        val offset: Future[Metadata.CommittedOffset] = ( consumer ? Metadata.GetCommittedOffset( topicPartition ) ).mapTo[Metadata.CommittedOffset]

        offset.map(meta => {
          meta.response.foreach(res => {
            println( s"Partition: ${partition} - Offset: ${res.offset()} ")
          })
        })

      })




//      partitions.map(p => {
//        p.response.foreach(response => {
//          response.foreach(p => {
//            println(s"Assigned: ${p.partition()}")
//          })
//        })
//      })

      println( p.toArray.mkString("\n") )
      println("End")
      clientConsumer.close()
    })
  })
  */

}
