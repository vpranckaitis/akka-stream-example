package lt.vpranckaitis.akka.stream.example


import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Source}
import akka.util.ByteString
import org.joda.time.{DateTime, Hours}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Success, Try}

object GithubReplay extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()

  sealed trait Response
  case class NextPage(uri: String) extends Response
  case class Commit(message: String) extends Response
  case class PullRequest(title: String, state: String, createdAt: String, merged_at: Option[String]) extends Response

  implicit val pullRequestFormat = jsonFormat(PullRequest, "title", "state", "created_at", "merged_at")

  val token = args(0)

  val connectionPool = Http().cachedHostConnectionPoolHttps[Unit](host = "api.github.com")

  def httpRequest: Flow[String, Response, NotUsed] = {
    def toRequest(uri: String) = {
      val headers = List(Authorization(GenericHttpCredentials("token", token, Map())))
      HttpRequest(uri = uri, headers = headers)
    }
    def getNextPage(linkHeader: Option[Link]): Option[NextPage] = linkHeader
      .flatMap { _.values.find { _.params.exists(_.value() == "next") } }
      .map { link => NextPage(link.uri.toString) }
    def parseResponse(r: (Try[HttpResponse], Unit)) = r match {
      case (Success(resp), _) => Unmarshal(resp.entity).to[List[PullRequest]].map { js => (resp.header[Link], js) }
      case _ => Future.failed(new RuntimeException())
    }
    def createMessages(x: (Option[Link], List[PullRequest])) = getNextPage(x._1) ++: x._2

    Flow[String]
      .map(uri => (toRequest(uri), ()))
      .via(connectionPool)
      .mapAsync(10)(parseResponse)
      .mapConcat(createMessages)
  }

  val traversePages = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val merge = builder.add(Merge[String](2))
    val broadcast = builder.add(Broadcast[Response](2, eagerCancel = true))

    val collectUris = Flow[Response].collect[String] { case NextPage(uri) => uri }
    def log = Flow[String].map { x => println(x); x }

    merge ~> log ~> httpRequest ~> broadcast
    merge <~     collectUris    <~ broadcast

    val output = broadcast.collect { case pr: PullRequest => pr }

    FlowShape(merge.in(1), output.outlet)
  })

  def parseMonthAndDuration(pr: PullRequest): Option[(String, Int)] = pr match {
    case PullRequest(_, "closed", created, Some(merged)) =>
      val createdDate = DateTime.parse(created)
      val hours = Hours.hoursBetween(createdDate, DateTime.parse(merged)).getHours
      Some((createdDate.toString("YYYY-MM"), hours))
    case _ => None
  }

  def flow(user: String, repo: String) =
    Source.single(s"https://api.github.com/repos/$user/$repo/pulls?state=closed&sort=created&direction=asc&per_page=300")
      .via(traversePages)
      //.map(x => {println(x + "\n" + parseMonthAndDuration(x)); x})
      .async
      .map(parseMonthAndDuration)
      .collect { case Some(x) => x }
      .sliding(2)
      .splitAfter(SubstreamCancelStrategy.propagate){
        case (month1, _) +: (month2, _) +: _ => month1 != month2
        case _ => false
      }
      .map { _.head }
      .fold(("", List.empty[Int])) { case (acc, x) => (x._1, x._2 :: acc._2) }
      .concatSubstreams
      .map { case (month, hours) =>
        val cnt = hours.size
        val mean = hours.sum.toDouble / cnt
        val median = hours.sortBy(identity).drop(cnt / 2).head
        val max = hours.max
        (month, cnt, mean, median, max)
      }

  val route = get {
    path("users" / Segment / "repos" / Segment) { (user, repo) =>
      complete {
        val entity = HttpEntity.Chunked.fromData(
          ContentTypes.`text/plain(UTF-8)`,
          flow(user, repo).map { x => ByteString(x + "\n") })
        HttpResponse(entity = entity)
      }
    }
  }

  Http().bindAndHandle(route, interface = "localhost", port = 8080)
}
