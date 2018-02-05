package upload

import akka.stream._
import akka.stream.scaladsl._
import akka.Done
import akka.actor.ActorSystem

import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._

import scala.io.{Source => IOSource}
import scala.util.{Failure, Success}

object Main extends App {
  implicit val system = ActorSystem("codeu")
  import system.dispatcher
  implicit val materializer = ActorMaterializer()
//  val host = "34.244.159.244"
  val Host = "localhost"
  val Port = scala.util.Properties.envOrElse("PORT", "8080").toInt
  val MaxItems = scala.util.Properties.envOrElse("MAX", "100").toInt
  val DataPath = scala.util.Properties.envOrElse("DATA", "")
  val AccessToken = sys.env("ACCESS_TOKEN")
  val QueueSize = 500
  val AsyncRequests = 100
//  val Auth = Authorization(BasicHttpCredentials("user", "pass"))
  val Auth = RawHeader("access-token", AccessToken)
  val ChunkSize = 64 * 1024

  val poolClientFlow = Http().cachedHostConnectionPool[Promise[HttpResponse]](Host, Port)

  val queue =
    Source
      .queue[(HttpRequest, Promise[HttpResponse])](QueueSize, OverflowStrategy.backpressure)
      .via(poolClientFlow)
      .toMat(Sink.foreach({
        case ((Success(resp), p)) => p.success(resp)
        case ((Failure(e), p))    => p.failure(e)
      }))(Keep.left)
      .run()

  Future
    .sequence(
      List(
        processFile("interactions.csv", 322776002),
        processFile("items.csv", 1306054),
        processFile("targetItems.csv", 46559, Seq("item_id")),
        processFile("targetUsers.csv", 74840),
        processFile("users.csv", 1497020)
      ))
    .onComplete(_ => system.terminate())

  def queueRequest(request: HttpRequest): Future[HttpResponse] = {
    val responsePromise = Promise[HttpResponse]()
    queue.offer(request -> responsePromise).flatMap {
      case QueueOfferResult.Enqueued    => responsePromise.future
      case QueueOfferResult.Dropped     => Future.failed(new RuntimeException("Queue overflowed. Try again later."))
      case QueueOfferResult.Failure(ex) => Future.failed(ex)
      case QueueOfferResult.QueueClosed => Future.failed(new RuntimeException("Queue was closed (pool shut down) while running the request. Try again later."))
    }
  }

  def processFile(filename: String, linesNum: Int, columnNames: Seq[String] = Nil): Future[Done] = {
    val entity = "[A-Z]".r.replaceAllIn(filename.replaceAll("s.csv$", ""), m => s"_${m.matched.toLowerCase}")
    val hasHeader = columnNames.isEmpty
    val columns =
      if (!hasHeader) columnNames
      else getColumns(filename)

    val file = Paths.get("data/" + filename)

    FileIO
      .fromPath(file, ChunkSize)
      .mapConcat { bs =>
        bs.utf8String.split("\\n").toList
      }
      .drop(if (hasHeader) 1 else 0)
      .take(MaxItems)
      .map(l => toJson(l, columns, entity))
//      .throttle(10000, 1.seconds, 1, ThrottleMode.Shaping)
      .mapAsyncUnordered(AsyncRequests) { payload =>
        queueRequest(
          HttpRequest(
            uri = endpoint(entity),
            method = HttpMethods.POST,
            entity = payload,
            headers = List(Auth)
          ))
//          headers = Seq(`Content-Type`(ContentTypes.`application/json`)))
      }
//      .mapAsyncUnordered(500) { resp =>
      .map { resp =>
//        Future {
        resp.discardEntityBytes()
        if (resp.status.isSuccess) (1, 0) else (0, 1)
//        }
      }
      .recover {
        case e: RuntimeException =>
          println(e.getMessage)
          (0, 0)
      }
      .fold((0, 0))((memo, item) => (memo._1 + item._1, memo._2 + item._2))
      .map { res =>
        val suc = res._1
        val err = res._2
        s"${entity}s: $suc successful and $err failed request. Total is ${suc + err}. Expected is $linesNum"
      }
      .runForeach(println)(materializer)
  }

  def endpoint(entity: String) = s"/${entity}s"

  def getColumns(filename: String): Seq[String] =
    IOSource
      .fromFile("data/" + filename)
      .getLines
      .next
      .split("\t")
      .toSeq
      .map(l => if (l.contains(".")) l.replaceAll("^[^.]+.", "") else l)

  def toJson(line: String, columns: Seq[String], entity: String): String = {
    val out = new StringBuilder
    out ++= s"""{"""
    out ++= columns
      .zip(line.split("\t"))
      .map {
        case (column, value) =>
          s""""$column":"$value""""
      }
      .mkString(",")
    out ++= "}"
    out.toString()
  }
}
