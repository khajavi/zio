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
 * Generates accessor methods, like [[accessible]], for a service with one type
 * parameter of kind `F[_, _]`. The annotation's type argument, which must be
 * `IO`, `RIO`, `URIO`, `Managed`, `RManaged` or `URManaged`, replaces that
 * parameter in the accessors' types. For example, on `trait Store[F[_, _]]`
 * with the member `def get(key: String): F[String, Int]`, `@accessibleMM[IO]`
 * generates `def get(key: String): ZIO[Store[IO], String, Int]` in
 * `object Store`.
 *
 * Macro annotations exist on Scala 2 only. On Scala 2.13 they need the
 * `-Ymacro-annotations` compiler option, and on Scala 2.12 the macro paradise
 * plugin; without them compilation fails with "enable macro paradise to expand
 * macro annotations".
 *
 * @see [[https://zio.dev/reference/service-pattern/accessor-methods/ Accessor Methods]]
 */
@compileTimeOnly("enable macro paradise to expand macro annotations")
class accessibleMM[F[_, _]] extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro AccessibleMMMacro.apply
}
