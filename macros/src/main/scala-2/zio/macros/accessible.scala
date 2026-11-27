/*
 * Copyright 2019-2024 John A. De Goes and the ZIO Contributors
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

import scala.annotation.{StaticAnnotation, compileTimeOnly}

/**
 * Generates accessors for a ZIO service: for each `def` and `val` of the
 * service, an accessor of the same name and parameters (a `val` for a `val` of
 * a service without type parameters) that gets the service from the
 * environment and calls the member on it.
 *
 * Annotate either a service trait, in which case the accessors go into its
 * companion object (created when missing), or an object that contains a trait
 * named `Service`, in which case they go into that object. For a service `S`:
 *
 *   - a member returning `ZIO[R, E, A]`, `ZStream[R, E, A]`, `ZSink[R, E, A, L,
 *     B]` or `ZManaged[R, E, A]` gets an accessor of the same type with `S`
 *     added to `R` (`ZIO[S with R, E, A]`, which is the same type as
 *     `ZIO[S, E, A]` when `R` is `Any`);
 *   - a member returning any other type `A` gets an accessor returning `ZIO[S,
 *     Nothing, A]`, so an exception it throws becomes a defect; mark a `def`
 *     with [[throwing]] to get `ZIO[S, Throwable, A]` instead.
 *
 * A `@deprecated` annotation on a member is copied to its accessor. For
 * example, on `trait Users` with the member `def find(id: Int): IO[String,
 * User]`, `@accessible` generates `def find(id: Int): ZIO[Users, String, User]`
 * in `object Users`.
 *
 * `@accessible` exists on Scala 2 only. On Scala 2.13 it needs the
 * `-Ymacro-annotations` compiler option, and on Scala 2.12 the macro paradise
 * plugin; without them compilation fails with "enable macro paradise to expand
 * macro annotations".
 *
 * @see
 *   [[https://zio.dev/reference/service-pattern/accessor-methods/ Accessor Methods]]
 */
@compileTimeOnly("enable macro paradise to expand macro annotations")
class accessible[A] extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro AccessibleMacro.apply
}
