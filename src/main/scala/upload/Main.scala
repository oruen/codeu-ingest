package upload

import akka.stream._
import akka.stream.scaladsl._
import akka.Done
import akka.actor.ActorSystem

import scala.concurrent._
import java.nio.file.Paths
import scala.io.{Source => IOSource}

object Main extends App {
  implicit val system = ActorSystem("codeu")
  import system.dispatcher
  implicit val materializer = ActorMaterializer()

  Future
    .sequence(
      List(
        processFile("interactions.csv"),
        processFile("items.csv"),
        processFile("targetItems.csv", Seq("item_id")),
        processFile("targetUsers.csv"),
        processFile("users.csv")
      ))
    .onComplete(_ => system.terminate())

  def processFile(filename: String, columnNames: Seq[String] = Nil): Future[Done] = {
    val entity = filename.replaceAll("s.csv$", "")
    val hasHeader = columnNames.isEmpty
    val columns =
      if (!hasHeader) columnNames
      else getColumns(filename)

    val file = Paths.get("data/" + filename)

    FileIO
      .fromPath(file)
      .mapConcat { bs =>
        bs.utf8String.split("\\n").toList
      }
      .drop(if (hasHeader) 1 else 0)
//      .take(3)
      .map(l => toJson(l, columns, entity))
      .runForeach(println)(materializer)
  }

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
    out ++= s"""{"$entity":{"""
    out ++= columns
      .zip(line.split("\t"))
      .map {
        case (column, value) =>
          s""""$column":"$value""""
      }
      .mkString(",")
    out ++= "}}"
    out.toString()
  }
}
