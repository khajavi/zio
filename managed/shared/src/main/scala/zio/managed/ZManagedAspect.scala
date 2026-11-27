package zio.managed

import zio._

import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A `ZManagedAspect` transforms a `ZManaged` value into another `ZManaged`
 * value with the same environment, error and result types, within the bounds
 * given by its type parameters. Apply an aspect with `ZManaged#@@`, and combine
 * aspects with `>>>`, `@@` or `andThen`.
 */
trait ZManagedAspect[+LowerR, -UpperR, +LowerE, -UpperE, +LowerA, -UpperA] { self =>

  /** Applies this aspect to the specified managed value. */
  def apply[R >: LowerR <: UpperR, E >: LowerE <: UpperE, A >: LowerA <: UpperA](
    managed: ZManaged[R, E, A]
  ): ZManaged[R, E, A]

  /** An alias for `andThen`. */
  def >>>[
    LowerR1 >: LowerR,
    UpperR1 <: UpperR,
    LowerE1 >: LowerE,
    UpperE1 <: UpperE,
    LowerA1 >: LowerA,
    UpperA1 <: UpperA
  ](
    that: ZManagedAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1]
  ): ZManagedAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1] =
    self.andThen(that)

  /**
   * Returns a new aspect that represents the sequential composition of this
   * aspect with the specified one. An alias for `>>>`.
   */
  def @@[
    LowerR1 >: LowerR,
    UpperR1 <: UpperR,
    LowerE1 >: LowerE,
    UpperE1 <: UpperE,
    LowerA1 >: LowerA,
    UpperA1 <: UpperA
  ](
    that: ZManagedAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1]
  ): ZManagedAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1] =
    self >>> that

  /**
   * Returns a new aspect that applies this aspect to a managed value and then
   * applies `that` to the result.
   */
  def andThen[
    LowerR1 >: LowerR,
    UpperR1 <: UpperR,
    LowerE1 >: LowerE,
    UpperE1 <: UpperE,
    LowerA1 >: LowerA,
    UpperA1 <: UpperA
  ](
    that: ZManagedAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1]
  ): ZManagedAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1] =
    new ZManagedAspect[LowerR1, UpperR1, LowerE1, UpperE1, LowerA1, UpperA1] {
      def apply[R >: LowerR1 <: UpperR1, E >: LowerE1 <: UpperE1, A >: LowerA1 <: UpperA1](
        managed: ZManaged[R, E, A]
      ): ZManaged[R, E, A] =
        that(self(managed))
    }
}
