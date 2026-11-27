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
  def provideSome[R0] =
    new ProvideSomeZManagedPartiallyApplied[R0, R, E, A](self)

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
  inline def provide[E1 >: E](inline layer: ZLayer[_, E1, _]*): ZManaged[Any, E1, A] =
    ${ ZManagedMacros.provideImpl[Any, R, E1, A]('self, 'layer) }
}

/**
 * The value returned by `ZManaged#provideSome` on Scala 3. It provides some of
 * the environment of a `ZManaged` and leaves the remainder `R0`.
 */
final class ProvideSomeZManagedPartiallyApplied[R0, -R, +E, +A](val self: ZManaged[R, E, A]) extends AnyVal {
  /**
   * Assembles the layers passed in at compile time and provides them to the
   * `ZManaged`, leaving the environment `R0`. A missing layer is a compile
   * error, and a layer that is not needed is reported as a compile warning.
   */
  inline def apply[E1 >: E](inline layer: ZLayer[_, E1, _]*): ZManaged[R0, E1, A] =
    ${ ZManagedMacros.provideImpl[R0, R, E1, A]('self, 'layer) }
}

object ZManagedMacros {
  import scala.quoted._

  def provideImpl[R0: Type, R: Type, E: Type, A: Type](
    schedule: Expr[ZManaged[R, E, A]],
    layer: Expr[Seq[ZLayer[_, E, _]]]
  )(using Quotes): Expr[ZManaged[R0, E, A]] = {
    val layerExpr = LayerMacros.constructStaticProvideLayer[R0, R, E](layer)
    '{
      $schedule.provideLayer($layerExpr.asInstanceOf[ZLayer[R0, E, R]])
    }
  }
}
