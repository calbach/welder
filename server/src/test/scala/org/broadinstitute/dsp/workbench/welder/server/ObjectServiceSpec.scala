package org.broadinstitute.dsp.workbench.welder
package server

import java.io.File
import java.nio.file.{Path, Paths}

import _root_.fs2.Stream
import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonError
import com.google.cloud.Identity
import com.google.cloud.storage.{Acl, BlobId, BucketInfo}
import io.circe.{Json, parser}
import fs2.{io, text}
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName, GetMetadataResponse, GoogleStorageService, RemoveObjectResult, StorageRole}
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GoogleProject}
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

class ObjectServiceSpec extends FlatSpec with WelderTestSuite {
  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]
  val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map.empty)
  val metaCache = Ref.unsafe[IO, Map[Path, GcsMetadata]](Map.empty)
  val objectServiceConfig = ObjectServiceConfig(Paths.get("/tmp"), WorkbenchEmail("me@gmail.com"), 20 minutes)
  val objectService = ObjectService(objectServiceConfig, FakeGoogleStorageInterpreter, global, storageLinksCache, metaCache)

  "ObjectService" should "be able to localize a file" in {
    forAll {(bucketName: GcsBucketName, blobName: GcsBlobName, bodyString: String, localFileDestination: Path) => //Use string here just so it's easier to debug
      val body = bodyString.getBytes()
      // It would be nice to test objects with `/` in its name, but google storage emulator doesn't support it
      val requestBody =
        s"""
           |{
           |  "action": "localize",
           |  "entries": [
           |   {
           |     "sourceUri": "gs://${bucketName.value}/${blobName.value}",
           |     "localDestinationPath": "${localFileDestination}"
           |   }
           |  ]
           |}
      """.stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)

      val res = for {
        _ <- FakeGoogleStorageInterpreter.removeObject(bucketName, blobName)
        _ <- FakeGoogleStorageInterpreter.storeObject(bucketName, blobName, body, "text/plain", Map.empty, None, None).compile.drain
        resp <- objectService.service.run(request).value
        localFileBody <- io.file.readAll[IO](localFileDestination, global, 4096)
          .compile
          .toList
        _ <- IO((new File(localFileDestination.toString)).delete())
      } yield {
        resp.get.status shouldBe (Status.Ok)
        localFileBody should contain theSameElementsAs (body)
      }

      res.unsafeRunSync()
    }
  }

  it should "should be able to localize data uri" in {
    val localFileDestination = arbFilePath.arbitrary.sample.get
    val requestBody =
      s"""
         |{
         |  "action": "localize",
         |  "entries": [
         |   {
         |     "sourceUri": "data:application/json;base64,eyJkZXN0aW5hdGlvbiI6ICJnczovL2J1Y2tldC9ub3RlYm9va3MiLCAicGF0dGVybiI6ICJcLmlweW5iJCJ9",
         |     "localDestinationPath": "${localFileDestination}"
         |   }
         |  ]
         |}
      """.stripMargin

    val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
    val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)

    val expectedBody = """{"destination": "gs://bucket/notebooks", "pattern": "\.ipynb$"}""".stripMargin

    val res = for {
      resp <- objectService.service.run(request).value
      localFileBody <- io.file.readAll[IO](localFileDestination, global, 4096).through(fs2.text.utf8Decode).compile.foldMonoid
      _ <- IO((new File(localFileDestination.toString)).delete())
    } yield {
      resp.get.status shouldBe (Status.Ok)
      localFileBody shouldBe(expectedBody)
    }

    res.unsafeRunSync()
  }

  "/GET metadata" should "return no storage link is found if storagelink isn't found" in {
    forAll {
      (localFileDestination: Path) =>
        val requestBody = s"""
             |{
             |  "localPath": "${localFileDestination.toString}"
             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)

        val res = for {
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
        } yield {
          resp.get.status shouldBe Status.Ok
        }
        res.attempt.unsafeRunSync() shouldBe(Left(StorageLinkNotFoundException(s"No storage link found for ${localFileDestination.toString}")))
    }
  }

  it should "return RemoteNotFound if metadata is not found in GCS" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localBaseDirectory -> storageLink))
        val objectService = ObjectService(objectServiceConfig, FakeGoogleStorageInterpreter, global, storageLinksCache, metaCache)

        val requestBody = s"""
                             |{
                             |  "localPath": "${localBaseDirectory.path.toString}/test.ipynb"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"REMOTE_NOT_FOUND","storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        val res = for {
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return SafeMode if storagelink exists in LocalSafeBaseDirectory" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localSafeDirectory -> storageLink))
        val objectService = ObjectService(objectServiceConfig, FakeGoogleStorageInterpreter, global, storageLinksCache, metaCache)

        val requestBody =
          s"""
             |{
             |  "localPath": "${localSafeDirectory.path.toString}/test.ipynb"
             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"SAFE","storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        val res = for {
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe (expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.LIVE if crc32c matches" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localBaseDirectory -> storageLink))
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 0L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)
        val objectService = ObjectService(objectServiceConfig, storageService, global, storageLinksCache, metaCache)

        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"LIVE","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.LocalChanged if crc32c doesn't match but local cached generation matches remote" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localBaseDirectory -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val metaCache = Ref.unsafe[IO, Map[Path, GcsMetadata]](Map(Paths.get(localPath) -> GcsMetadata(Paths.get(localPath), None, None, Crc32("asdf"), 111L)))
        val bodyBytes = "this is great! Okay".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 111L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)

        val objectService = ObjectService(objectServiceConfig, storageService, global, storageLinksCache, metaCache)
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"LOCAL_CHANGED","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.RemoteChanged if crc32c doesn't match and local cached generation doesn't match remote" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localBaseDirectory -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val metaCache = Ref.unsafe[IO, Map[Path, GcsMetadata]](Map(Paths.get(localPath) -> GcsMetadata(Paths.get(localPath), None, None, Crc32("asdf"), 0L)))
        val bodyBytes = "this is great! Okay".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 1L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)

        val objectService = ObjectService(objectServiceConfig, storageService, global, storageLinksCache, metaCache)
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"REMOTE_CHANGED","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.OutOfSync if crc32c doesn't match and local cached generation doesn't match remote" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localBaseDirectory -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great! Okay".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 1L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)

        val objectService = ObjectService(objectServiceConfig, storageService, global, storageLinksCache, metaCache)
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"DESYNCHRONIZED","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return lastLockedBy if metadata shows it's locked by someone" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localBaseDirectory -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map("lastLockedBy" -> lockedBy.value, "lockExpiresAt" -> Long.MaxValue.toString), 0L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)

        val objectService = ObjectService(objectServiceConfig, storageService, global, storageLinksCache, metaCache)
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"LIVE","lastLockedBy":"${lockedBy.value}","storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          println(body)
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return not return lastLockedBy if lock has expired" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localBaseDirectory -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map("lastLockedBy" -> lockedBy.value, "lockExpiresAt" -> Long.MinValue.toString), 0L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)

        val objectService = ObjectService(objectServiceConfig, storageService, global, storageLinksCache, metaCache)
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"LIVE","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  "/safeDelocalize" should "delocalize a file" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localBaseDirectory -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")

        val objectService = ObjectService(objectServiceConfig, FakeGoogleStorageInterpreter, global, storageLinksCache, metaCache)
        val requestBody = s"""
                             |{
                             |  "action": "safeDelocalize",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          _ <- IO((new File(localPath.toString)).delete())
          remoteFile <- FakeGoogleStorageInterpreter.getObject(cloudStorageDirectory.bucketName, getFullBlobName(Paths.get(localPath), cloudStorageDirectory.blobPath).getOrElse(throw new Exception("fail to get full blob path"))).compile.toList
        } yield {
          resp.get.status shouldBe Status.Ok
          remoteFile contains theSameInstanceAs(bodyBytes)
        }
        res.unsafeRunSync()
    }
  }

  it should "do not delocalize a SAFE mode file" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localSafeDirectory -> storageLink))
        val localPath = s"${localSafeDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")

        val objectService = ObjectService(objectServiceConfig, FakeGoogleStorageInterpreter, global, storageLinksCache, metaCache)
        val requestBody = s"""
                             |{
                             |  "action": "safeDelocalize",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)
        // Create the local base directory
        val directory = new File(s"/tmp/${localSafeDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value.attempt
          _ <- IO((new File(localPath.toString)).delete())
          remoteFile <- FakeGoogleStorageInterpreter.getObject(cloudStorageDirectory.bucketName, getFullBlobName(Paths.get(localPath), cloudStorageDirectory.blobPath).getOrElse(throw new Exception("fail to get full blob path"))).compile.toList
        } yield {
          resp shouldBe Left(SafeDelocalizeSafeModeFile(s"${localPath} can't be delocalized since it's in safe mode"))
          remoteFile.isEmpty shouldBe(true)
        }
        res.unsafeRunSync()
    }
  }

  it should "throw GenerationMismatch exception if remote file has changed" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[LocalDirectory, StorageLink]](Map(localBaseDirectory -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")

        val objectService = ObjectService(objectServiceConfig, GoogleStorageServiceFailToStoreObject, global, storageLinksCache, metaCache)
        val requestBody = s"""
                             |{
                             |  "action": "safeDelocalize",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value.attempt
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp shouldBe Left(GenerationMismatch(s"Remote version has changed for /tmp/${localPath}. Generation mismatch"))
        }
        res.unsafeRunSync()
    }
  }
}

class FakeGoogleStorageService(metadataResponse: GetMetadataResponse) extends GoogleStorageService[IO]{
  override def listObjectsWithPrefix(bucketName: GcsBucketName, objectNamePrefix: String, maxPageSize: Long, traceId: Option[TraceId]): fs2.Stream[IO, GcsObjectName] = ???
  override def storeObject(bucketName: GcsBucketName, objectName: GcsBlobName, objectContents: Array[Byte], objectType: String, metadata: Map[String, String], generation: Option[Long], traceId: Option[TraceId]): fs2.Stream[IO, Unit] = ???
  override def setBucketLifecycle(bucketName: GcsBucketName, lifecycleRules: List[BucketInfo.LifecycleRule], traceId: Option[TraceId]): fs2.Stream[IO, Unit] = ???
  override def unsafeGetObject(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId]): IO[Option[String]] = ???
  override def getObject(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId]): fs2.Stream[IO, Byte] = ???
  override def downloadObject(blobId: BlobId, path: Path, traceId: Option[TraceId]): fs2.Stream[IO, Unit] = ???
  override def getObjectMetadata(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId]): fs2.Stream[IO, GetMetadataResponse] = Stream.emit(metadataResponse).covary[IO]
  override def removeObject(bucketName: GcsBucketName, objectName: GcsBlobName, traceId: Option[TraceId]): IO[RemoveObjectResult] = ???
  override def createBucket(googleProject: GoogleProject, bucketName: GcsBucketName, acl: Option[NonEmptyList[Acl]], traceId: Option[TraceId]): fs2.Stream[IO, Unit] = ???
  override def setIamPolicy(bucketName: GcsBucketName, roles: Map[StorageRole, NonEmptyList[Identity]], traceId: Option[TraceId]): fs2.Stream[IO, Unit] = ???
}

object FakeGoogleStorageService {
  def apply(metadata: GetMetadataResponse): FakeGoogleStorageService = new FakeGoogleStorageService(metadata)
}

object GoogleStorageServiceFailToStoreObject extends GoogleStorageService[IO]{
  override def listObjectsWithPrefix(bucketName: GcsBucketName, objectNamePrefix: String, maxPageSize: Long, traceId: Option[TraceId]): fs2.Stream[IO, GcsObjectName] = ???
  override def storeObject(bucketName: GcsBucketName, objectName: GcsBlobName, objectContents: Array[Byte], objectType: String, metadata: Map[String, String], generation: Option[Long], traceId: Option[TraceId]): fs2.Stream[IO, Unit] = {
    val errors = new GoogleJsonError()
    errors.setCode(412)
    Stream.raiseError[IO](new com.google.cloud.storage.StorageException(errors))
  }
  override def setBucketLifecycle(bucketName: GcsBucketName, lifecycleRules: List[BucketInfo.LifecycleRule], traceId: Option[TraceId]): fs2.Stream[IO, Unit] = ???
  override def unsafeGetObject(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId]): IO[Option[String]] = ???
  override def getObject(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId]): fs2.Stream[IO, Byte] = ???
  override def downloadObject(blobId: BlobId, path: Path, traceId: Option[TraceId]): fs2.Stream[IO, Unit] = ???
  override def getObjectMetadata(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId]): fs2.Stream[IO, GetMetadataResponse] = ???
  override def removeObject(bucketName: GcsBucketName, objectName: GcsBlobName, traceId: Option[TraceId]): IO[RemoveObjectResult] = ???
  override def createBucket(googleProject: GoogleProject, bucketName: GcsBucketName, acl: Option[NonEmptyList[Acl]], traceId: Option[TraceId]): fs2.Stream[IO, Unit] = ???
  override def setIamPolicy(bucketName: GcsBucketName, roles: Map[StorageRole, NonEmptyList[Identity]], traceId: Option[TraceId]): fs2.Stream[IO, Unit] = ???
}