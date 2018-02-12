package lt.vpranckaitis.akka.stream.example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Source}
import akka.stream.{ActorMaterializer, FlowShape, Materializer, ThrottleMode}
import akka.util.ByteString
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success

object GithubReplay extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()

  sealed trait Response
  case class NextPage(uri: String) extends Response
  case class Message(message: String) extends Response

  val connectionPool = Http().cachedHostConnectionPoolHttps[Unit](host = "api.github.com")

  def toRequest: Flow[String, Response, NotUsed] = Flow[String]
    .map(uri => (HttpRequest(uri = uri, headers = List(Authorization(BasicHttpCredentials("xxx", "xxx")))), ()))
    .throttle(1, 1.second, 0, ThrottleMode.Shaping)
    .via(connectionPool)
    .mapAsync(1) {
      case (Success(resp), _) => Unmarshal(resp.entity).to[List[JsObject]].map(jsArray => (resp.header[Link], jsArray))
      case _ => Future.failed(new RuntimeException())
    }.mapConcat { case (linkOption, jsArray) =>
      val nextPageOption = linkOption.flatMap { _.values.find { _.params.exists(_.value() == "next") } }.map { link => NextPage(link.uri.toString) }
      nextPageOption ++: jsArray.map { o => Message(o.fields("commit").asJsObject.fields("message").toString() + "\n") }
    }

  val traverse = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val merge = builder.add(Merge[String](2))
    val broadcast = builder.add(Broadcast[Response](2, eagerCancel = true))

    val collectUris = Flow[Response].collect[String] { case NextPage(uri) => uri }

    merge ~> Flow[String].map(x => { println(x); x}) ~> toRequest ~> broadcast
    merge <~                                          collectUris <~ broadcast

    val output = broadcast.collect { case Message(m) => m }

    FlowShape(merge.in(1), output.outlet)
  })

  val route = get {
    path("users" / Segment / "repos" / Segment) { (user, repo) =>
      complete {
        HttpResponse(entity = HttpEntity.Chunked.fromData(ContentTypes.`text/plain(UTF-8)`, Source.single("/repos/akka/akka/commits").via(traverse).map(ByteString(_))))
      }
    }
  }

  Http().bindAndHandle(route, interface = "localhost", port = 8080)
}
