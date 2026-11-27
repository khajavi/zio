/*
 * Copyright 2017-2020 John A. De Goes and the ZIO Contributors
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

import zio.internal.macros.LayerMacros

private[managed] trait ZManagedVersionSpecific[-R, +E, +A] { self: ZManaged[R, E, A] =>

  /**
   * Splits the environment into two parts, assembling one part using the
   * layers passed to the returned value's `apply` and leaving the remainder
   * `R0`. The layers are wired at compile time: a missing layer is a compile
   * error, and a layer that is not needed is reported as a compile warning.
   *
   * On Scala 2 the returned value also has `provideLayer` and
   * `provideSomeLayer`; on Scala 3 it has only `apply`.
   *
   * {{{
   * import zio._
   *
   * final case class Config(url: String)
   * final case class Db(config: Config)
   *
   * val configLayer: ULayer[Config] = ZLayer.succeed(Config("db://local"))
   *
   * val managed: ZManaged[Config with Db, Nothing, Int] = ZManaged.succeed(1)
   *
   * val managed2: ZManaged[Db, Nothing, Int] =
   *   managed.provideSome[Db](configLayer)
   * }}}
   */
  def provideSome[R0]: ProvideSomeLayerManagedPartiallyApplied[R0, R, E, A] =
    new ProvideSomeLayerManagedPartiallyApplied[R0, R, E, A](self)

  /**
   * Assembles the layers passed in at compile time and provides them to this
   * `ZManaged`, so that it requires no environment. A missing layer is a
   * compile error, and a layer that is not needed is reported as a compile
   * warning.
   *
   * {{{
   * import zio._
   *
   * final case class Config(url: String)
   * final case class Db(config: Config)
   *
   * val managed: ZManaged[Config with Db, Nothing, Int] = ZManaged.succeed(1)
   *
   * val configLayer: ULayer[Config] = ZLayer.succeed(Config("db://local"))
   * val dbLayer: ULayer[Db]          = ZLayer.succeed(Db(Config("db://other")))
   *
   * val managed2: ZManaged[Any, Nothing, Int] =
   *   managed.provide(configLayer, dbLayer)
   * }}}
   */
  def provide[E1 >: E](layer: ZLayer[_, E1, _]*): ZManaged[Any, E1, A] =
    macro LayerMacros.provideImpl[ZManaged, R, E1, A]

}

/**
 * The value returned by `ZManaged#provideSome` on Scala 2. It provides some of
 * the environment of a `ZManaged` and leaves the remainder `R0`.
 */
final class ProvideSomeLayerManagedPartiallyApplied[R0, -R, +E, +A](
  val self: ZManaged[R, E, A]
) extends AnyVal {

  /**
   * Provides the specified layer, which builds the whole environment `R` from
   * `R0`, to the `ZManaged`. It is `ZManaged#provideLayer`.
   */
  def provideLayer[E1 >: E](
    layer: ZLayer[R0, E1, R]
  )(implicit trace: Trace): ZManaged[R0, E1, A] =
    self.provideLayer(layer)

  /**
   * Splits the environment of the `ZManaged` into two parts. It is
   * `ZManaged#provideSomeLayer`.
   */
  def provideSomeLayer[R0]: ZManaged.ProvideSomeLayer[R0, R, E, A] =
    new ZManaged.ProvideSomeLayer[R0, R, E, A](self)

  /**
   * Assembles the layers passed in at compile time and provides them to the
   * `ZManaged`, leaving the environment `R0`. A missing layer is a compile
   * error, and a layer that is not needed is reported as a compile warning.
   */
  def apply[E1 >: E](layer: ZLayer[_, E1, _]*): ZManaged[R0, E1, A] =
    macro LayerMacros.provideSomeImpl[ZManaged, R0, R, E1, A]
}
