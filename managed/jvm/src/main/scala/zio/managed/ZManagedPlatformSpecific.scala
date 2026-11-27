/*
 * Copyright 2020-2024 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.managed

import zio._
import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.io
import java.io.IOException
import java.net.{URI, URL}
import java.nio.file.Path

private[managed] trait ZManagedPlatformSpecific {

  /**
   * Returns a managed effect that describes shifting to the blocking executor
   * as the `acquire` action and shifting back to the original executor as the
   * `release` action.
   */
  def blocking(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZIO.blockingExecutor.toManaged.flatMap(executor => ZManaged.onExecutor(executor))

  /**
   * Opens the file at `path` for reading and returns a `ZInputStream` over it.
   * Fails with an `IOException` when the file cannot be opened.
   */
  def readFile(path: Path)(implicit trace: Trace): ZManaged[Any, IOException, ZInputStream] =
    readFile(path.toString())

  /**
   * Opens the file at `path` for reading and returns a `ZInputStream` over it.
   * Fails with an `IOException` when the file cannot be opened.
   */
  def readFile(path: String)(implicit trace: Trace): ZManaged[Any, IOException, ZInputStream] =
    ZManaged
      .acquireReleaseWith(
        ZIO.attemptBlockingIO {
          val fis = new io.FileInputStream(path)
          (fis, ZInputStream.fromInputStream(fis))
        }
      )(tuple => ZIO.attemptBlocking(tuple._1.close()).orDie)
      .map(_._2)

  /**
   * Opens the stream of `url` for reading and returns a `ZInputStream` over it.
   * Fails with an `IOException` when the stream cannot be opened.
   */
  def readURL(url: URL)(implicit trace: Trace): ZManaged[Any, IOException, ZInputStream] =
    ZManaged
      .acquireReleaseWith(
        ZIO.attemptBlockingIO {
          val fis = url.openStream()
          (fis, ZInputStream.fromInputStream(fis))
        }
      )(tuple => ZIO.attemptBlocking(tuple._1.close()).orDie)
      .map(_._2)

  /**
   * Parses `url` as a `java.net.URL` and reads from it like `readURL(URL)`. A
   * string that is not a valid URL terminates the effect with a
   * `java.net.MalformedURLException` instead of failing with an `IOException`.
   */
  def readURL(url: String)(implicit trace: Trace): ZManaged[Any, IOException, ZInputStream] =
    ZManaged.fromZIO(ZIO.succeed(new URL(url))).flatMap(readURL)

  /**
   * Reads from `uri`. An absolute URI is converted to a URL and read like
   * `readURL(URL)`; any other URI is read as the file whose path is the URI's
   * string form, like `readFile(String)`.
   */
  def readURI(uri: URI)(implicit trace: Trace): ZManaged[Any, IOException, ZInputStream] =
    for {
      isAbsolute <- ZManaged.fromZIO(ZIO.attemptBlockingIO(uri.isAbsolute()))
      is         <- if (isAbsolute) readURL(uri.toURL()) else readFile(uri.toString())
    } yield is

  /**
   * Opens the file at `path` for writing and returns a `ZOutputStream` over it.
   * An existing file is truncated. Fails with an `IOException` when the file
   * cannot be opened.
   */
  def writeFile(path: String)(implicit trace: Trace): ZManaged[Any, IOException, ZOutputStream] =
    ZManaged
      .acquireReleaseWith(
        ZIO.attemptBlockingIO {
          val fos = new io.FileOutputStream(path)
          (fos, ZOutputStream.fromOutputStream(fos))
        }
      )(tuple => ZIO.attemptBlocking(tuple._1.close()).orDie)
      .map(_._2)

  /**
   * Opens the file at `path` for writing and returns a `ZOutputStream` over it.
   * An existing file is truncated. Fails with an `IOException` when the file
   * cannot be opened.
   */
  def writeFile(path: Path)(implicit trace: Trace): ZManaged[Any, IOException, ZOutputStream] =
    writeFile(path.toString())

}
