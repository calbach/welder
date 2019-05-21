package org.broadinstitute.dsp.workbench.welder.server

import cats.effect.IO
import io.circe.{Decoder, Encoder}
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import io.circe.parser._
import io.circe.syntax._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import scalacache._

import scala.util.Try

class StorageLinksService(storageLinksCache: Cache[StorageLink]) extends Http4sDsl[IO] {

  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root =>
      Ok(getStorageLinks())
    case req @ DELETE -> Root =>
      for {
        storageLink <- req.as[StorageLink]
        resp <- Ok(deleteStorageLink(storageLink))
      } yield resp
    case req @ POST -> Root =>
      for {
        storageLink <- req.as[StorageLink]
        resp <- Ok(createStorageLink(storageLink))
      } yield resp
  }

  def createStorageLink(storageLink: StorageLink): IO[Unit] = {
    IO.pure(storageLinksCache.put(storageLink))





//    getStorageLinks().map { currentStorageLinks =>
//      val updatedStorageLinks = currentStorageLinks + storageLink
//
//      reflect.io.File("storagelinks.json").writeAll(updatedStorageLinks.asJson.toString)
//    }.map(_ => ())
  }

  def deleteStorageLink(storageLink: StorageLink): IO[Unit] = {
    getStorageLinks().map { currentStorageLinks =>
      val updatedStorageLinks = currentStorageLinks - storageLink
      reflect.io.File("storagelinks.json").writeAll(updatedStorageLinks.asJson.toString)
    }.map(_ => ())
  }

  def getStorageLinks(): IO[Set[StorageLink]] = {
    IO.pure(storageLinksCache.g)


//    //TODO: handle file not existing
//    decode[Set[StorageLink]](loadStorageLinksFile) match {
//      case Left(_) => IO.pure(Set.empty) //TODO: actually handle error here
//      case Right(foo) => IO.pure(foo)
//    }
  }

  private def loadStorageLinksFile(): String = {
    Try(scala.io.Source.fromFile("storagelinks.json").mkString).recover {
      case _ => ""
    }.get
  }

}

final case class LocalDirectory(asString: String) extends AnyVal
final case class GsDirectory(asString: String) extends AnyVal
final case class StorageLink(localBaseDirectory: LocalDirectory, cloudStorageDirectory: GsDirectory, pattern: String, recursive: Boolean)

object StorageLinksService {
  def apply(storageLinksCache: Cache[StorageLink]): StorageLinksService = new StorageLinksService(storageLinksCache)
}

object StorageLink {

  implicit val localDirectoryEncoder: Encoder[LocalDirectory] = Encoder.encodeString.contramap(_.asString)
  implicit val gsDirectoryEncoder: Encoder[GsDirectory] = Encoder.encodeString.contramap(_.asString)

  implicit val localDirectoryDecoder: Decoder[LocalDirectory] = Decoder.decodeString.map(LocalDirectory)
  implicit val gsDirectoryDecoder: Decoder[GsDirectory] = Decoder.decodeString.map(GsDirectory)

  implicit val storageLinkEncoder: Encoder[StorageLink] = Encoder.forProduct4(
    "localBaseDirectory",
    "cloudStorageDirectory",
    "pattern",
    "recursive")(x => StorageLink.unapply(x).get)

  implicit val storageLinkDecoder: Decoder[StorageLink] = Decoder.forProduct4(
    "localBaseDirectory",
    "cloudStorageDirectory",
    "pattern",
    "recursive")(StorageLink.apply)
}
