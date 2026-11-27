/*
 * Copyright 2023-2024 John A. De Goes and the ZIO Contributors
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

package zio.macros

import zio._

/**
 * A `ServiceReloader` is a "registry" of services, allowing services to be
 * reloaded dynamically. You can create a reloadable version of your service by
 * using the `reloadable` operator on `ZLayer`. Then call
 * `ServiceReloader.reload` to reload the service.
 *
 * The registry keeps one service per type. On Scala 3, the derived
 * `IsReloadable` instance that `register` and `reloadable` use is marked
 * `@experimental`, so code that relies on it must be in an `@experimental`
 * scope.
 *
 * {{{
 * // scala-2-only
 * import zio._
 *
 * trait Greeter { def greet(name: String): UIO[String] }
 *
 * val greeterLayer: ULayer[Greeter] =
 *   ZLayer.succeed(new Greeter {
 *     def greet(name: String): UIO[String] = ZIO.succeed(s"Hello, \$name")
 *   })
 *
 * val program: ZIO[ServiceReloader, ServiceReloader.Error, String] =
 *   for {
 *     greeter <- ServiceReloader.register[Greeter](greeterLayer)
 *     _       <- ServiceReloader.reload[Greeter]
 *     message <- greeter.greet("Ada")
 *   } yield message // Hello, Ada
 * }}}
 *
 * @see
 *   [[https://zio.dev/reference/service-pattern/reloadable-services/ Reloadable Services]]
 */
trait ServiceReloader {

  /**
   * Registers the specified service with a layer that constructs the service.
   * Returns an implementation of the service that is backed by a `ScopedRef`
   * which will handle reloading the service. Fails with a
   * `ServiceInitializationError` exception if an error occurs while acquiring
   * the service.
   *
   * Registering a service type that is already registered dies with a
   * `java.lang.ClassCastException`. The registry keeps the earlier layer, and a
   * later `reload` builds the service from that layer.
   */
  def register[A: Tag: IsReloadable](serviceLayer: ZLayer[Any, Any, A]): IO[ServiceReloader.Error, A]

  /**
   * Reloads the specified service, releasing any resources associated with the
   * service and acquiring a new service. Fails with a
   * `ServiceInitializationError` exception if an error occurs reloading the
   * service or a `ServiceNotRegistered` exception if a layer constructing the
   * service was never registered.
   */
  def reload[A: Tag]: IO[ServiceReloader.Error, Unit]
}

/** The `ServiceReloader` errors, its live layer and its accessors. */
object ServiceReloader {

  /** An error that `register` or `reload` fails with. */
  sealed trait Error extends Throwable

  /** The layer that builds the service failed with `error`. */
  final case class ServiceInitializationError(error: Any) extends Error

  /**
   * `reload` was called for the service type `tag`, which was never registered.
   */
  final case class ServiceNotRegistered(tag: LightTypeTag) extends Error

  /**
   * The live implementation of the `ServiceReloader` service.
   */
  val live: ZLayer[Any, Nothing, ServiceReloader] =
    ZLayer.scoped {
      for {
        ref   <- Ref.Synchronized.make[Map[LightTypeTag, (ZLayer[Any, Any, Any], ScopedRef[Any])]](Map.empty)
        scope <- ZIO.scope
      } yield new ServiceReloader {
        def register[A: Tag: IsReloadable](serviceLayer: ZLayer[Any, Any, A]): IO[ServiceReloader.Error, A] =
          ref.modifyZIO { map =>
            val tag = Tag[A].tag
            map.get(tag) match {
              case Some((_, scopedRef)) =>
                scope.extend {
                  scopedRef
                    .set(serviceLayer.build.map(_.unsafe.get(tag)(Unsafe.unsafe)))
                    .foldZIO(
                      error => ZIO.fail(ServiceInitializationError(error)),
                      _ => scopedRef.get.map(a => (a.asInstanceOf[A], map))
                    )
                }
              case None =>
                scope.extend {
                  ScopedRef
                    .fromAcquire(serviceLayer.build.map(_.unsafe.get[A](tag)(Unsafe.unsafe)))
                    .foldZIO(
                      error => ZIO.fail(ServiceInitializationError(error)),
                      scopedRef =>
                        ZIO.succeed(
                          (
                            IsReloadable[A].reloadable(scopedRef),
                            map.updated(tag, (serviceLayer, scopedRef.asInstanceOf[ScopedRef[Any]]))
                          )
                        )
                    )
                }
            }
          }
        def reload[A: Tag]: IO[ServiceReloader.Error, Unit] =
          ref.modifyZIO { map =>
            val tag = Tag[A].tag
            map.get(tag) match {
              case Some((serviceLayer, scopedRef)) =>
                scope.extend {
                  scopedRef
                    .set(serviceLayer.build.map(_.unsafe.get[A](tag)(Unsafe.unsafe)))
                    .foldZIO(
                      error => ZIO.fail(ServiceInitializationError(error)),
                      _ => ZIO.succeed(((), map))
                    )
                }
              case None =>
                ZIO.fail(ServiceNotRegistered(tag))
            }
          }
      }
    }

  /**
   * Registers the specified service with a layer that constructs the service.
   * Returns an implementation of the service that is backed by a `ScopedRef`
   * which will handle reloading the service. Fails with a
   * `ServiceInitializationError` exception if an error occurs while acquiring
   * the service.
   *
   * Registering a service type that is already registered dies with a
   * `java.lang.ClassCastException`. The registry keeps the earlier layer, and a
   * later `reload` builds the service from that layer.
   */
  def register[A: Tag: IsReloadable](
    serviceLayer: ZLayer[Any, Any, A]
  ): ZIO[ServiceReloader, ServiceReloader.Error, A] =
    ZIO.serviceWithZIO(_.register(serviceLayer))

  /**
   * Reloads the specified service, releasing any resources associated with the
   * service and acquiring a new service. Fails with a
   * `ServiceInitializationError` exception if an error occurs reloading the
   * service or a `ServiceNotRegistered` exception if a layer constructing the
   * service was never registered.
   */
  def reload[A: Tag]: ZIO[ServiceReloader, ServiceReloader.Error, Unit] =
    ZIO.serviceWithZIO(_.reload)
}
