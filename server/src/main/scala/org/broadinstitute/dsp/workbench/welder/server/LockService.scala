package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path

import cats.effect.IO
import cats.effect.concurrent.Ref
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.server.StorageLinksService._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import LockService._

class LockService(storageLinks: Ref[IO, Map[LocalBasePath, StorageLink]]) extends Http4sDsl[IO] {

  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ POST -> Root => for {
      request <- req.as[AcquireLockRequest]
      res <- acquireLock(request)
      resp <- Ok(res)
    } yield resp
  }

  def acquireLock(req: AcquireLockRequest): IO[Unit] = {
    ???
  }
}

final case class StorageLinks(storageLinks: Set[StorageLink])

object LockService {
  def apply(storageLinks: StorageLinksCache): StorageLinksService = new StorageLinksService(storageLinks)

  implicit val acquireLockRequestDecoder: Decoder[AcquireLockRequest] = Decoder.forProduct1("localPath")(AcquireLockRequest.apply)

  implicit val acquireLockResponseEncoder: Encoder[AcquireLockResponse] = Encoder.forProduct1("result")(x => AcquireLockResponse.unapply(x).get)
}

final case class AcquireLockRequest(localObjectPath: Path)

sealed abstract class AcquireLockResponse {
  def result: String
}
object AcquireLockResponse {
  final case object FileNotFoundInStorageLink extends AcquireLockResponse {
    def result: String = "FileNotFoundInStorageLink"
  }
  final case object Success extends AcquireLockResponse {
    def result: String = "success"
  }
}