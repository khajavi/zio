/*
 * Copyright 2021-2024 John A. De Goes and the ZIO Contributors
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

import scala.annotation.StaticAnnotation

/**
 * Marks a `def` of a service annotated with [[accessible]], [[accessibleM]] or
 * [[accessibleMM]] as one that can throw. Its accessor runs the call in
 * `ZIO.attempt`, so it returns `ZIO[S, Throwable, A]` and a thrown exception
 * becomes a failure instead of a defect. It changes only `def`s whose result
 * type is not a `ZIO`, `ZStream`, `ZSink` or `ZManaged`; on a `val` it has no
 * effect.
 *
 * @see [[https://zio.dev/reference/service-pattern/accessor-methods/ Accessor Methods]]
 */
class throwing extends StaticAnnotation
