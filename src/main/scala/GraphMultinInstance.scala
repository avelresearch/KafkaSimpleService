
import scala.util.{Failure, Success}

object GraphMultinInstance extends  App {

  import akka.actor.ActorSystem
  import akka.stream.ActorMaterializer
  import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}

  import scala.concurrent.Future

  implicit val system = ActorSystem("MyActorSystem")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val sink = Sink.fold[Int, Int](0)(_ + _)

  val runnable: RunnableGraph[Future[Int]] =
    Source(1 to 100000).toMat(sink)(Keep.right)

  // get the materialized value of the FoldSink
  val sum1: Future[Int] = runnable.run()
  val sum2: Future[Int] = runnable.run()

  def printFuture( f: Future[Int]) = f.onComplete(res => res match {
    case Success(v) => println(v)
    case Failure(ex) => println(ex.getMessage)
  })

  printFuture(sum1)
  printFuture(sum2)

}
