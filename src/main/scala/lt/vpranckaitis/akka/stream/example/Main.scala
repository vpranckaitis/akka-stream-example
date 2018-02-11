package lt.vpranckaitis.akka.stream.example

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Try}

object Main extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withInputBuffer(initialSize = 1, maxSize = 1)
  )
  var PreviousTime = System.currentTimeMillis()

  val flow = Flow.fromGraph(GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._

    val broadcast = b.add(Broadcast[String](2))
    val join = b.add(Zip[String, Unit]())

    val throttle = Flow[String].throttle(1, 200.millis, 0, ThrottleMode.Shaping)

    val buffer1 = Flow[String].buffer(10, OverflowStrategy.backpressure)
    val buffer2 = Flow[Unit].buffer(10, OverflowStrategy.backpressure)

    broadcast ~>                       buffer1 ~> throttle ~> toUppercase ~> join.in0
    broadcast ~> sleepIfLongerThan5 ~> buffer2 ~>                            join.in1

    FlowShape(broadcast.in, join.out)
  })

  val source = FileIO.fromPath(Paths.get("input.txt"))
    .via(Framing.delimiter(ByteString(' '), Int.MaxValue, allowTruncation = true))
    .map(_.utf8String)

  val sink = Sink.foreach[(String, Long)](p => println(f"${p._1}%-10s ${p._2 * 0.001}%4.1f"))

  source.via(flow).map(timeDiff).to(sink).run()

  // -----

  def toUppercase = {
    val connectionPool = Http().cachedHostConnectionPool[Unit]("api.shoutcloud.io")

    def toRequest(s: String) = Marshal(Map("INPUT" -> s).toJson).to[RequestEntity] map { e =>
      (HttpRequest(HttpMethods.POST, "/V1/SHOUT", entity = e), ())
    }

    def parseResponse(resp: (Try[HttpResponse], Unit)) = resp match {
      case (Success(resp), _) => Unmarshal(resp.entity).to[Map[String, String]] map { _("OUTPUT") }
      case _ => Future.successful("failed")
    }

    Flow[String]
      .mapAsync(1)(toRequest)
      .via(connectionPool)
      .mapAsync(1)(parseResponse)
      .async
  }

  def timeDiff(x: (String, _)) = {
    val time = System.currentTimeMillis() - PreviousTime
    PreviousTime = System.currentTimeMillis()
    (x._1, time)
  }

  def sleepIfLongerThan5 = Flow.fromFunction[String, Unit](s => if (s.length > 5) Thread.sleep(1000)).async
}