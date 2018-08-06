import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}

object PreMaterializedSource extends App {

  implicit val system = ActorSystem("MyActorSystem")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val matValuePoweredSource =
    Source.actorRef[String](bufferSize = 10, overflowStrategy = OverflowStrategy.fail)

  val (actorRef, source) = matValuePoweredSource.preMaterialize()

  (0 to 10).map(_ => actorRef ! "This is a best new message send" )

  // pass source around for materialization
  source.runWith(Sink.foreach(println))


}
