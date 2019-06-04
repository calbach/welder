package org.broadinstitute.dsp.workbench.welder

import java.nio.file.{Path, Paths}
import java.time.Instant

import fs2.Stream
import cats.implicits._
import io.circe.{Decoder, Encoder}
import ca.mrvisser.sealerate
import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalBasePath.{LocalBaseDirectoryPath, LocalSafeBaseDirectoryPath}
import org.broadinstitute.dsp.workbench.welder.SourceUri.{DataUri, GsPath}
import org.http4s.Uri

sealed abstract class SyncStatus extends Product with Serializable
object SyncStatus {
  // crc32c match
  final case object Live extends SyncStatus {
    override def toString: String = "LIVE"
  }
  // crc32c mismatch
  final case object Desynchronized extends SyncStatus {
    override def toString: String = "DESYNCHRONIZED"
  }
  // deleted in gcs. (object exists in storagelinks config file but not in in gcs)
  final case object RemoteNotFound extends SyncStatus {
    override def toString: String = "REMOTE_NOT_FOUND"
  }

  val stringToSyncStatus: Set[SyncStatus] = sealerate.values[SyncStatus]
}

sealed abstract class SyncMode extends Product with Serializable
object SyncMode {
  final case object Safe extends SyncMode {
    override def toString: String = "SAFE"
  }
  final case object Edit extends SyncMode {
    override def toString: String = "EDIT"
  }
}

final case class BlobPath(asString: String) extends AnyVal

sealed abstract class SourceUri
object SourceUri {
  final case class DataUri(data: Array[Byte]) extends SourceUri
  final case class GsPath(bucketName: GcsBucketName, blobName: GcsBlobName) extends SourceUri {
    override def toString: String = s"gs://${bucketName.value}/${blobName.value}"
  }
}

sealed abstract class LocalBasePath {
  def path: Path
}
object LocalBasePath {
  final case class LocalBaseDirectoryPath(path: Path) extends LocalBasePath
  final case class LocalSafeBaseDirectoryPath(path: Path) extends LocalBasePath
}

final case class CloudStorageDirectory(bucketName: GcsBucketName, blobPath: BlobPath)

final case class StorageLink(localBaseDirectory: LocalBasePath, localSafeModeBaseDirectory: LocalBasePath, cloudStorageDirectory: CloudStorageDirectory, pattern: String)

final case class GcsMetadata(lastLockedBy: Option[WorkbenchEmail], ExpiresAt: Option[Instant], crc32c: Crc32, generation: Long)

object JsonCodec {
  implicit val pathDecoder: Decoder[Path] = Decoder.decodeString.emap(s => Either.catchNonFatal(Paths.get(s)).leftMap(_.getMessage))
  implicit val pathEncoder: Encoder[Path] = Encoder.encodeString.contramap(_.toString)
  implicit val blobPathDecoder: Decoder[BlobPath] = Decoder.decodeString.emap(s => Either.catchNonFatal(BlobPath(s)).leftMap(_.getMessage))
  implicit val blobPathEncoder: Encoder[BlobPath] = Encoder.encodeString.contramap(_.asString)
  implicit val workbenchEmailEncoder: Encoder[WorkbenchEmail] = Encoder.encodeString.contramap(_.value)
  implicit val uriEncoder: Encoder[Uri] = Encoder.encodeString.contramap(_.renderString)
  implicit val uriDecoder: Decoder[Uri] = Decoder.decodeString.emap(s => Uri.fromString(s).leftMap(_.getMessage()))
  implicit val instanceEncoder: Encoder[Instant] = Encoder.encodeLong.contramap(_.toEpochMilli)
  implicit val syncStatusEncoder: Encoder[SyncStatus] = Encoder.encodeString.contramap(_.toString)
  implicit val gcsBucketNameDecoder: Decoder[GcsBucketName] = Decoder.decodeString.map(GcsBucketName)
  implicit val gcsBucketNameEncoder: Encoder[GcsBucketName] = Encoder.encodeString.contramap(_.value)
  implicit val gcsBlobNameDecoder: Decoder[GcsBlobName] = Decoder.decodeString.map(GcsBlobName)
  implicit val gsPathDecoder: Decoder[GsPath] = Decoder.decodeString.emap(parseGsPath)
  implicit val gsPathEncoder: Encoder[GsPath] = Encoder.encodeString.contramap(_.toString)
  implicit val cloudStorageDirectoryDecoder: Decoder[CloudStorageDirectory] = Decoder.decodeString.emap(s => parseGsPath(s).map(x => CloudStorageDirectory(x.bucketName, BlobPath(x.blobName.value))))
  implicit val cloudStorageDirectoryEncoder: Encoder[CloudStorageDirectory] =  Encoder.encodeString.contramap(x => s"gs://${x.bucketName.value}/${x.blobPath.asString}")
  implicit val sourceUriDecoder: Decoder[SourceUri] = Decoder.decodeString.emap { s =>
    if (s.startsWith("data:application/json;base64,")) {
      val res = for {
        encodedData <- Either.catchNonFatal(s.split(",")(1))
        data <- Either.catchNonFatal(Stream.emit(encodedData).through(base64DecoderPipe[IO]).compile.to[Array].unsafeRunSync())
      } yield DataUri(data)

      res.leftMap(_.getMessage)
    } else parseGsPath(s)
  }
  implicit val localBasePathEncoder: Encoder[LocalBasePath] = pathEncoder.contramap(_.path)

  implicit val storageLinkEncoder: Encoder[StorageLink] =  Encoder.forProduct4("localBaseDirectory", "localSafeModeBaseDirectory", "cloudStorageDirectory", "pattern")(storageLink => StorageLink.unapply(storageLink).get)

  implicit val storageLinkDecoder: Decoder[StorageLink] = Decoder.instance {
    x =>
      for {
        baseDir <- x.downField("localBaseDirectory").as[Path]
        safeBaseDir <- x.downField("localSafeModeBaseDirectory").as[Path]
        cloudStorageDir <- x.downField("cloudStorageDirectory").as[CloudStorageDirectory]
        pattern <- x.downField("pattern").as[String]
      } yield StorageLink(LocalBaseDirectoryPath(baseDir), LocalSafeBaseDirectoryPath(safeBaseDir), cloudStorageDir, pattern)
  }

  implicit val syncModeEncoder: Encoder[SyncMode] = Encoder.encodeString.contramap(_.toString)
}
