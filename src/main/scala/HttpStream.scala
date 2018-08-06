
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source, Tcp}
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.{Failure, Success}

object HttpStream extends App {

  implicit val system = ActorSystem("MyActorSystem")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  import akka.stream.scaladsl.Framing

  val port = 8888
  val host = "127.0.0.1"

  val commandParser = Flow[String].takeWhile(_ != "BYE")

  val welcomeMsg = s"Welcome to: $host, you are: $port !"
  val welcome = Source.single(welcomeMsg)

  val serverLogic = Flow[ByteString]
    .via( Framing.delimiter( ByteString("\n"),  maximumFrameLength = 255,  allowTruncation = true))
    .map(_.utf8String)
    .via(commandParser)
    // merge in the initial banner after parser
    //.merge(welcome)
    .map(s => {
      println(s)
      s + "\n" })
    .map(ByteString(_))

  val handler = Sink.foreach[Tcp.IncomingConnection] { conn =>
    println("Client connected from: " + conn.remoteAddress)

    conn.handleWith( serverLogic )
  }

  val connections = Tcp().bind(host, port)
  val binding = connections.to(handler).run()

  binding.onComplete {
    case Success(b) =>
      println(s"Server started, listening on: ${b.localAddress} ")
    case Failure(e) =>
      println(s"Server could not bind to $host:$port: ${e.getMessage}")
      system.terminate()
  }

//
//  val connections: Source[IncomingConnection, Future[ServerBinding]] =
//    Tcp().bind("127.0.0.1", 8888)
//  connections runForeach { connection ⇒
//
//    println(s"New connection from: ${connection.remoteAddress}")
//
//    val echo = Flow[ByteString]
//      .via(Framing.delimiter(
//        ByteString("\n"),
//        maximumFrameLength = 256,
//        allowTruncation = true))
//      .map(_.utf8String)
//      .map(_ + "!!!\n")
//      .map(ByteString(_))
//
//    connection.handleWith(echo)
//  }


}
