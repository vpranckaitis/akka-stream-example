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
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withInputBuffer(initialSize = 1, maxSize = 1)
  )

  def fib(n: Long): Long = if (n < 2) 1 else { fib(n - 2) + fib(n - 1) }

  val connectionPool = Http().cachedHostConnectionPool[Unit]("api.shoutcloud.io")

  def toRequest(s: String) = Marshal(Map("INPUT" -> s).toJson).to[RequestEntity] map { e =>
    (HttpRequest(HttpMethods.POST, "/V1/SHOUT", entity = e), ())
  }
  def parseResponse(resp: (Try[HttpResponse], Unit)) = resp match {
    case (Success(HttpResponse(_, _, entity, _)), _) => Unmarshal(entity).to[Map[String, String]] map { _("OUTPUT") }
    case _ => Future.successful("failed")
  }

  val toUppercase = Flow[String].mapAsync(1)(toRequest).via(connectionPool).mapAsync(1)(parseResponse)

  val startTime = System.currentTimeMillis()

  val flow =
    Flow.fromGraph(GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val split = b.add(Partition[String](2, x => Try(x.toInt).fold(_ => 0, _ => 1)).async)
      val join = b.add(Zip[String, String]())

      val stringsFlow = Flow[String]
        .via(toUppercase)
        .async

      val intsFlow = Flow[String]
        .map(_.toLong)
        .map(fib)
        .map(_ => f"${(System.currentTimeMillis() - startTime) * 0.001}%.2f")
        .async

      val buffer = Flow[String].buffer(100, OverflowStrategy.backpressure)
      val throttle = Flow[String].throttle(1, 100.millis, 0, ThrottleMode.Shaping)

      split.out(0) ~>             buffer ~> throttle ~> stringsFlow ~> join.in0
      split.out(1) ~> intsFlow ~> buffer ~>                            join.in1

      FlowShape(split.in, join.out)
    })

  val source = FileIO.fromPath(Paths.get("1.balanced.txt"))
    .via(Framing.delimiter(ByteString('\n'), Int.MaxValue, allowTruncation = true))
    .map(_.utf8String)

  val sink = Sink.foreach(println)

  val runFuture = source.via(flow).to(sink).run()

  /*scala.io.Source.fromFile("input.txt").getLines().zipWithIndex.flatMap { case (s, i) =>
    List(s, (i / 3).toString)
  } foreach println*/

  //future.onComplete(_ => { Thread.sleep(1000); system.terminate() })
  //Source.tick(0.millis, 100.millis, ()).map(_ => Random.nextInt(100) - 50).via(flow).to(Sink.foreach(println)).run()
}
