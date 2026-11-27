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

package zio.internal.stacktracer

import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * The source file path and the 1-based line of a call site, filled in by the
 * compiler through the implicit in the companion object. `path` is the path of
 * the source file as the compiler received it, not only the file name. ZIO
 * Test uses it to report where an assertion was written.
 */
final case class SourceLocation(path: String, line: Int)
/** Provides the implicit [[SourceLocation]] of the call site. */
object SourceLocation {
  /** Materializes the [[SourceLocation]] of the call site. */
  inline given SourceLocation = ${ Macros.sourceLocation }
}
