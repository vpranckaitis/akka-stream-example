package lt.vpranckaitis.akka.stream.example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.ContentTypes.`text/plain(UTF-8)`
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Source}
import akka.util.ByteString
import org.joda.time.{DateTime, Hours}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

object GithubReplay extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()

  case class NextPage(uri: String)
  case class PullRequest(title: String, state: String, createdAt: String, merged_at: Option[String])

  implicit val pullRequestFormat = jsonFormat(PullRequest, "title", "state", "created_at", "merged_at")

  type Response = (List[PullRequest], Option[NextPage])

  val httpRequest: Flow[String, Response, NotUsed] = {
    val connectionPool = Http().cachedHostConnectionPoolHttps[Unit](host = "api.github.com")
    val authHeader = args.headOption map { token => Authorization(GenericHttpCredentials("token", token)) }
    val acceptEncoding = `Accept-Encoding`(HttpEncodingRange(HttpEncodings.gzip))

    def toRequest(uri: String) = HttpRequest(uri = uri).withHeaders(acceptEncoding :: authHeader.toList)

    def getNext(linkHeader: Link): Option[NextPage] = linkHeader.values.collectFirst {
      case LinkValue(uri, params) if params.exists(_.value == "next") => NextPage(uri.toString)
    }

    def parseResponse(resp: HttpResponse) =
      Unmarshal(Gzip.decodeMessage(resp).entity).to[List[PullRequest]].map { (_, resp.header[Link].flatMap(getNext)) }

    Flow[String]
      .map { x => println(x); x }
      .map { uri => (toRequest(uri), ()) }
      .via(connectionPool)
      .mapAsync(1) { _._1.fold(Future.failed, parseResponse) }
  }

  val traversePages = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val merge = builder.add(Merge[String](2))
    val broadcast = builder.add(Broadcast[Response](2, eagerCancel = true))

    val collectUris = Flow[Response]
      .takeWhile { x => x._2.isDefined }
      .collect[String] { case (_, Some(NextPage(uri))) => uri }

    val collectPrs = builder.add(Flow[Response].mapConcat { x => x._1 })

    merge ~> httpRequest ~> broadcast ~> collectPrs
    merge <~ collectUris <~ broadcast

    FlowShape(merge.in(1), collectPrs.out)
  })

  def flow(user: String, repo: String) =
    Source.single(s"https://api.github.com/repos/$user/$repo/pulls?state=closed&sort=created&direction=asc&per_page=300")
      .via(traversePages)
      .throttle(1, 50.millis, 0, ThrottleMode.Shaping)
      .map { _.title + "\n" }
      //.map { s => (1 to 10).map { _ => s }.mkString }

  val route = get {
    path("users" / Segment / "repos" / Segment) { (user, repo) =>
      complete {
        HttpEntity.Chunked.fromData(`text/plain(UTF-8)`, flow(user, repo).map { ByteString.fromString })
      }
    }
  }

  Http().bindAndHandle(route, interface = "localhost", port = 8080)
}
