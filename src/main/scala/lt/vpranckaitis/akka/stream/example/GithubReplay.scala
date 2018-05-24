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

  def parseMonthAndDuration: PartialFunction[PullRequest, (String, Int)] = {
    case PullRequest(_, "closed", created, Some(merged)) =>
      val createdDate = DateTime.parse(created)
      val hours = Hours.hoursBetween(createdDate, DateTime.parse(merged)).getHours
      (createdDate.toString("YYYY-MM"), hours)
  }

  def differentMonth: (Seq[(String, Any)] => Boolean) = {
    case (month1, _) +: (month2, _) +: _ => month1 != month2
    case _ => false
  }

  def statistics(xs: List[Int]): String = {
    val cnt = xs.size
    val mean = xs.sum.toDouble / cnt
    val median = xs.sortBy(identity).drop(cnt / 2).head
    val max = xs.max
    f"$cnt%5d $mean%8.2f $median%5d $max%6d\n"
  }

  def flow(user: String, repo: String) =
    Source.single(s"https://api.github.com/repos/$user/$repo/pulls?state=closed&sort=created&direction=asc&per_page=300")
      .via(traversePages)
      .collect(parseMonthAndDuration)
      .sliding(2)
      .splitAfter(SubstreamCancelStrategy.propagate) { differentMonth }
      .fold(("", List.empty[Int])) { case ((_, hs), (m, h) +: _) => (m, h :: hs) }
      .map { case (month, hours) => month + statistics(hours) }
      .concatSubstreams

  val route = get {
    path("users" / Segment / "repos" / Segment) { (user, repo) =>
      complete {
        HttpEntity.Chunked.fromData(`text/plain(UTF-8)`, flow(user, repo).map { ByteString.fromString })
      }
    }
  }

  Http().bindAndHandle(route, interface = "localhost", port = 8080)
}
