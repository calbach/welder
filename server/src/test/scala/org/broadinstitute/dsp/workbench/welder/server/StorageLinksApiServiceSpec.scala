package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Paths

import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import fs2.text
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalBasePath.{LocalBaseDirectoryPath, LocalSafeBaseDirectoryPath}
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.FlatSpec

class StorageLinksApiServiceSpec extends FlatSpec with WelderTestSuite {
  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]
  val storageLinks = Ref.unsafe[IO, Map[LocalBasePath, StorageLink]](Map.empty)
  val storageLinksService = StorageLinksService(storageLinks)
  val cloudStorageDirectory = CloudStorageDirectory(GcsBucketName("foo"), BlobPath("bar/baz.zip"))
  val baseDir = LocalBaseDirectoryPath(Paths.get("/foo"))
  val baseSafeDir = LocalSafeBaseDirectoryPath(Paths.get("/bar"))

  "GET /storageLinks" should "return 200 and an empty list of no storage links exist" in {
    val request = Request[IO](method = Method.GET, uri = Uri.unsafeFromString("/storageLinks"))

    val expectedBody = """{"storageLinks": []}""".stripMargin

    val res = for {
      resp <- storageLinksService.service.run(request).value
      body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
    } yield {
      resp.get.status shouldBe Status.Ok
      body shouldBe expectedBody
    }

    res.unsafeRunSync()
  }

  it should "return 200 and a list of storage links when they exist" in {
    val request = Request[IO](method = Method.GET, uri = Uri.unsafeFromString("/storageLinks"))

    val expectedBody = """{"storageLinks": [{"localBaseDirectory": "/foo", "localSafeBaseDirectory": "/bar"}]}""".stripMargin

    val linkToAddAndRemove = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")

    storageLinksService.createStorageLink(linkToAddAndRemove).unsafeRunSync()

    val intermediateListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(intermediateListResult.storageLinks equals Set(linkToAddAndRemove))

    val res = for {
      resp <- storageLinksService.service.run(request).value
      body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
    } yield {
      resp.get.status shouldBe Status.Ok
      true shouldBe false
    }

    res.unsafeRunSync()
  }

  "POST /storageLinks" should "return 200 and the storage link created when called with a valid storage link" in {
    val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/storageLinks"))


  }

  it should "return 400 when an invalid storage link is supplied" in {

  }


}