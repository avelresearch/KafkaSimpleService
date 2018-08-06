import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.scalatest.FlatSpec

import scala.concurrent.Await
import scala.concurrent.duration._


class SourceBaseSpec extends FlatSpec {

    def getFixture = new {
      val sinkUnderTest = Flow[Int].map(_ * 2).toMat(Sink.fold(0)(_ + _))(Keep.right)
    }

  "Source(1 to 4)" should "be return 4" in {
    implicit val system = ActorSystem("MyActorSystem")
    implicit val materializer = ActorMaterializer()
    implicit val ec = system.dispatcher

    val fixture = getFixture

    val future = Source(1 to 4).runWith(fixture.sinkUnderTest)
    val result = Await.result(future, 3.seconds)
    assert(result == 20)
  }

}
