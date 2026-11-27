/*
 * Copyright 2018-2024 John A. De Goes and the ZIO Contributors
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
import zio.managed.ZManaged.ReleaseMap
import zio.stacktracer.TracingImplicits.disableAutoTrace

import scala.collection.immutable.LongMap
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

/**
 * A `Reservation[-R, +E, +A]` pairs the effect that acquires a resource with
 * the function that releases it, without saying when or how the resource is
 * used. `release` receives the `Exit` value that the resource is released with.
 *
 * [[ZManaged#reserve]] splits a `ZManaged` into a `Reservation`, and
 * `ZManaged.fromReservation` turns a `Reservation` back into a `ZManaged`. The
 * `ZManaged` built by `fromReservation` registers `release` before it runs
 * `acquire`, so `release` also runs when `acquire` fails or is interrupted.
 */
final case class Reservation[-R, +E, +A](acquire: ZIO[R, E, A], release: Exit[Any, Any] => URIO[R, Any])

/**
 * A `ZManaged[R, E, A]` is a managed resource of type `A`, which may be used by
 * invoking the `use` method of the resource. The resource will be automatically
 * acquired before the resource is used, and automatically released after the
 * resource is used.
 *
 * Composing managed values with `flatMap`, `zip` or a for comprehension
 * acquires them in order and, when `use` ends, releases them one after another
 * in the reverse order. Release happens whether the `use` function succeeds,
 * fails, dies or is interrupted, and each finalizer receives that `Exit` value.
 * Values combined with `zipPar` are acquired in parallel and released in
 * parallel.
 *
 * Resources do not survive the scope of `use`, meaning that if you attempt to
 * capture the resource, leak it from `use`, and then use it after the resource
 * has been consumed, the resource will not be valid anymore and may fail with
 * some checked error, as per the type of the functions provided by the
 * resource.
 *
 * `ZManaged` lives in the `zio-managed` module, kept so that code written with
 * `ZManaged` still compiles. ZIO 2 code describes resources with `ZIO` and
 * `Scope` instead (`ZIO.acquireRelease`, `ZIO.scoped`). `scoped` turns a
 * `ZManaged` into a scoped `ZIO`, and `ZManaged.scoped` goes the other way.
 *
 * {{{
 * import zio._
 *
 * def resource(name: String, log: Ref[List[String]]): ZManaged[Any, Nothing, String] =
 *   ZManaged.acquireReleaseWith(log.update(("acquire " + name) :: _).as(name))(_ =>
 *     log.update(("release " + name) :: _)
 *   )
 *
 * val program: UIO[List[String]] =
 *   for {
 *     log <- Ref.make(List.empty[String])
 *     _ <- (resource("a", log) <*> resource("b", log)).use { case (a, b) =>
 *            log.update(("use " + a + b) :: _)
 *          }
 *     events <- log.get
 *   } yield events.reverse
 * // List(acquire a, acquire b, use ab, release b, release a)
 * }}}
 *
 * @see
 *   [[https://zio.dev/guides/migrate/zio-2.x-migration-guide/ ZIO 2.x Migration Guide]]
 */
sealed abstract class ZManaged[-R, +E, +A] extends ZManagedVersionSpecific[R, E, A] with Serializable { self =>

  /**
   * The ZIO value that underlies this ZManaged value. It succeeds with a tuple
   * of a finalizer that releases the resource and the resource itself. It reads
   * the `ReleaseMap` it registers finalizers in from the fiber ref
   * `ZManaged.currentReleaseMap`, which `use` and the other operators that run
   * a `ZManaged` set for it.
   *
   * Note that this method is a low-level interface, not intended for regular
   * usage. As such, it offers no guarantees on interruption or resource safety:
   * those are up to the caller to enforce.
   */
  def zio: ZIO[R, E, (ZManaged.Finalizer, A)]

  /**
   * Syntax for adding aspects.
   */
  final def @@[LowerR <: UpperR, UpperR <: R, LowerE >: E, UpperE >: LowerE, LowerA >: A, UpperA >: LowerA](
    aspect: => ZManagedAspect[LowerR, UpperR, LowerE, UpperE, LowerA, UpperA]
  )(implicit trace: Trace): ZManaged[UpperR, LowerE, LowerA] =
    ZManaged.suspend(aspect(self))

  /**
   * A symbolic alias for `orDie`.
   */
  final def !(implicit ev1: E <:< Throwable, ev2: CanFail[E], trace: Trace): ZManaged[R, Nothing, A] =
    self.orDie

  /**
   * Symbolic alias for zipParRight
   */
  def &>[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit trace: Trace): ZManaged[R1, E1, A1] =
    zipPar(that).map(_._2)

  /**
   * Symbolic alias for zipRight
   */
  def *>[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit trace: Trace): ZManaged[R1, E1, A1] =
    flatMap(_ => that)

  /**
   * Symbolic alias for zipParLeft
   */
  def <&[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit trace: Trace): ZManaged[R1, E1, A] =
    zipPar(that).map(_._1)

  /**
   * Symbolic alias for zipPar
   */
  def <&>[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit
    zippable: Zippable[A, A1],
    trace: Trace
  ): ZManaged[R1, E1, zippable.Out] =
    zipWithPar(that)(zippable.zip(_, _))

  /**
   * Symbolic alias for zipLeft.
   */
  def <*[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit trace: Trace): ZManaged[R1, E1, A] =
    flatMap(r => that.map(_ => r))

  /**
   * Symbolic alias for zip.
   */
  def <*>[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit
    zippable: Zippable[A, A1],
    trace: Trace
  ): ZManaged[R1, E1, zippable.Out] =
    zipWith(that)(zippable.zip(_, _))

  /**
   * Operator alias for `orElse`.
   */
  def <>[R1 <: R, E2, A1 >: A](
    that: => ZManaged[R1, E2, A1]
  )(implicit ev: CanFail[E], trace: Trace): ZManaged[R1, E2, A1] =
    orElse(that)

  /**
   * Submerges the error case of an `Either` into the `ZManaged`. The inverse
   * operation of `either`.
   */
  def absolve[E1 >: E, B](implicit ev: A IsSubtypeOfOutput Either[E1, B], trace: Trace): ZManaged[R, E1, B] =
    ZManaged.absolve(self.map(ev))

  /**
   * Turns failures and defects of the acquisition into a single `Throwable`
   * failure. The cause is squashed to one `Throwable`: when it holds both a
   * typed failure and a defect, the typed failure is kept and the defect is
   * dropped; a cause with only a defect fails with that defect.
   */
  def absorb(implicit ev: E IsSubtypeOfError Throwable, trace: Trace): ZManaged[R, Throwable, A] =
    absorbWith(ev)

  /**
   * Like `absorb`, but converts the typed failure to a `Throwable` with `f`.
   * The cause is squashed to one `Throwable` with `Cause#squashWith`, so the
   * rest of the cause is dropped.
   */
  def absorbWith(f: E => Throwable)(implicit trace: Trace): ZManaged[R, Throwable, A] =
    self.sandbox
      .foldManaged(
        cause => ZManaged.fail(cause.squashWith(f)),
        ZManaged.succeed(_)
      )

  /**
   * Maps this effect to the specified constant while preserving the effects of
   * this effect.
   */
  def as[B](b: => B)(implicit trace: Trace): ZManaged[R, E, B] =
    map(_ => b)

  /**
   * Maps the success value of this effect to an optional value.
   */
  final def asSome(implicit trace: Trace): ZManaged[R, E, Option[A]] =
    map(Some(_))

  /**
   * Maps the error value of this effect to an optional value.
   */
  final def asSomeError(implicit trace: Trace): ZManaged[R, Option[E], A] =
    mapError(Some(_))

  /**
   * Recovers from all typed failures with `h`. A cause with only defects or
   * interruption is not caught. When the cause holds both a typed failure and a
   * defect (for example from `zipPar`), `h` receives the failure and the defect
   * is dropped.
   */
  def catchAll[R1 <: R, E2, A1 >: A](
    h: E => ZManaged[R1, E2, A1]
  )(implicit ev: CanFail[E], trace: Trace): ZManaged[R1, E2, A1] =
    foldManaged(h, ZManaged.succeed(_))

  /**
   * Recovers from any failure of the acquisition with `h`, which receives the
   * full `Cause`: typed failures, defects, and an interruption the acquisition
   * caused itself (such as `ZIO.interrupt`). It does not recover when the fiber
   * is interrupted from outside.
   *
   * {{{
   * import zio._
   *
   * val port: ZManaged[Any, String, Int] = ZManaged.fail("no port configured")
   *
   * val portOrDefault: ZManaged[Any, Nothing, Int] =
   *   port.catchAllCause(_ => ZManaged.succeed(8080))
   * }}}
   *
   * @see
   *   [[absorb]], [[sandbox]], [[mapErrorCause]] - other functions that work
   *   with defects
   */
  def catchAllCause[R1 <: R, E2, A1 >: A](h: Cause[E] => ZManaged[R1, E2, A1])(implicit
    trace: Trace
  ): ZManaged[R1, E2, A1] =
    self.foldCauseManaged[R1, E2, A1](h, ZManaged.succeed(_))

  /**
   * Recovers from some or all of the error cases.
   */
  def catchSome[R1 <: R, E1 >: E, A1 >: A](
    pf: PartialFunction[E, ZManaged[R1, E1, A1]]
  )(implicit ev: CanFail[E], trace: Trace): ZManaged[R1, E1, A1] =
    foldManaged(pf.applyOrElse[E, ZManaged[R1, E1, A1]](_, ZManaged.fail(_)), ZManaged.succeed(_))

  /**
   * Recovers from some or all of the error Causes.
   */
  def catchSomeCause[R1 <: R, E1 >: E, A1 >: A](
    pf: PartialFunction[Cause[E], ZManaged[R1, E1, A1]]
  )(implicit trace: Trace): ZManaged[R1, E1, A1] =
    foldCauseManaged(pf.applyOrElse[Cause[E], ZManaged[R1, E1, A1]](_, ZManaged.failCause(_)), ZManaged.succeed(_))

  /**
   * Fail with `e` if the supplied `PartialFunction` does not match, otherwise
   * succeed with the returned value.
   */
  def collect[E1 >: E, B](e: => E1)(pf: PartialFunction[A, B])(implicit trace: Trace): ZManaged[R, E1, B] =
    collectManaged(e)(pf.andThen(ZManaged.succeed(_)))

  /**
   * Fail with `e` if the supplied `PartialFunction` does not match, otherwise
   * continue with the returned value.
   */
  def collectManaged[R1 <: R, E1 >: E, B](e: => E1)(pf: PartialFunction[A, ZManaged[R1, E1, B]])(implicit
    trace: Trace
  ): ZManaged[R1, E1, B] =
    self.flatMap(v => pf.applyOrElse[A, ZManaged[R1, E1, B]](v, _ => ZManaged.fail(e)))

  /**
   * Returns an effect whose failure and success have been lifted into an
   * `Either`. The resulting effect has no typed failure. A cause with only
   * defects or interruption still ends it; when the cause holds both a typed
   * failure and a defect, it succeeds with `Left` of the failure and the defect
   * is dropped.
   */
  def either(implicit ev: CanFail[E], trace: Trace): ZManaged[R, Nothing, Either[E, A]] =
    fold(Left[E, A], Right[E, A])

  /**
   * Ensures that `f` is executed when this ZManaged is finalized, after the
   * existing finalizer. `f` also runs when the acquisition fails or dies.
   *
   * For usecases that need access to the ZManaged's result, see
   * [[ZManaged#onExit]].
   */
  def ensuring[R1 <: R](f: => ZIO[R1, Nothing, Any])(implicit trace: Trace): ZManaged[R1, E, A] =
    onExit(_ => f)

  /**
   * Ensures that `f` is executed when this ZManaged is finalized, before the
   * existing finalizer.
   *
   * For usecases that need access to the ZManaged's result, see
   * [[ZManaged#onExitFirst]].
   */
  def ensuringFirst[R1 <: R](f: => ZIO[R1, Nothing, Any])(implicit trace: Trace): ZManaged[R1, E, A] =
    onExitFirst(_ => f)

  /**
   * Returns a ZManaged that ignores typed failures raised by the acquire effect
   * and runs it repeatedly until it eventually succeeds. Defects are not
   * retried; an interruption the acquisition causes itself (such as
   * `ZIO.interrupt`) is retried.
   */
  def eventually(implicit ev: CanFail[E], trace: Trace): ZManaged[R, Nothing, A] =
    ZManaged(zio.eventually)

  /**
   * Returns a managed resource that attempts to acquire this managed resource
   * and in case of failure, attempts to acquire each of the specified managed
   * resources in order until one of them is successfully acquired, ensuring
   * that the acquired resource is properly released after being used.
   */
  final def firstSuccessOf[R1 <: R, E1 >: E, A1 >: A](rest: => Iterable[ZManaged[R1, E1, A1]])(implicit
    trace: Trace
  ): ZManaged[R1, E1, A1] =
    ZManaged.firstSuccessOf(self, rest)

  /**
   * Acquires this resource, passes it to `f`, and acquires the resource that
   * `f` returns. On release, the resource from `f` is released first and this
   * resource second; the second release runs even when the first one dies.
   */
  def flatMap[R1 <: R, E1 >: E, B](f: A => ZManaged[R1, E1, B])(implicit trace: Trace): ZManaged[R1, E1, B] =
    ZManaged {
      self.zio.flatMap { case (releaseSelf, a) =>
        f(a).zio.map { case (releaseThat, b) =>
          (
            e =>
              releaseThat(e).exit
                .flatMap(e1 =>
                  releaseSelf(e).exit
                    .flatMap(e2 => e1 *> e2)
                ),
            b
          )
        }
      }
    }

  /**
   * Effectfully map the error channel
   */
  def flatMapError[R1 <: R, E2](
    f: E => ZManaged[R1, Nothing, E2]
  )(implicit ev: CanFail[E], trace: Trace): ZManaged[R1, E2, A] =
    flipWith(_.flatMap(f))

  /**
   * Returns an effect that performs the outer effect first, followed by the
   * inner effect, yielding the value of the inner effect.
   *
   * This method can be used to "flatten" nested effects.
   */
  def flatten[R1 <: R, E1 >: E, B](implicit
    ev: A IsSubtypeOfOutput ZManaged[R1, E1, B],
    trace: Trace
  ): ZManaged[R1, E1, B] =
    flatMap(ev)

  /**
   * Returns an effect that performs the outer effect first, followed by the
   * inner effect, yielding the value of the inner effect.
   *
   * This method can be used to "flatten" nested effects.
   */
  def flattenZIO[R1 <: R, E1 >: E, B](implicit
    ev: A IsSubtypeOfOutput ZIO[R1, E1, B],
    trace: Trace
  ): ZManaged[R1, E1, B] =
    mapZIO(ev)

  /**
   * Flip the error and result
   */
  def flip(implicit trace: Trace): ZManaged[R, A, E] =
    foldManaged(ZManaged.succeed(_), ZManaged.fail(_))

  /**
   * Flip the error and result, then apply an effectful function to the effect
   */
  def flipWith[R1, A1, E1](f: ZManaged[R, A, E] => ZManaged[R1, A1, E1])(implicit
    trace: Trace
  ): ZManaged[R1, E1, A1] =
    f(flip).flip

  /**
   * Folds over the failure value or the success value to yield an effect that
   * does not fail, but succeeds with the value returned by the left or right
   * function passed to `fold`.
   */
  def fold[B](failure: E => B, success: A => B)(implicit
    ev: CanFail[E],
    trace: Trace
  ): ZManaged[R, Nothing, B] =
    foldManaged(failure.andThen(ZManaged.succeed(_)), success.andThen(ZManaged.succeed(_)))

  /**
   * A version of `fold` whose `failure` function receives the full `Cause`. It
   * recovers from typed failures, defects, and an interruption the acquisition
   * caused itself (such as `ZIO.interrupt`), but not when the fiber is
   * interrupted from outside.
   */
  def foldCause[B](failure: Cause[E] => B, success: A => B)(implicit trace: Trace): ZManaged[R, Nothing, B] =
    sandbox.fold(failure, success)

  /**
   * A version of `foldManaged` whose `failure` function receives the full
   * `Cause`. It recovers from typed failures, defects, and an interruption the
   * acquisition caused itself (such as `ZIO.interrupt`), but not when the fiber
   * is interrupted from outside.
   */
  def foldCauseManaged[R1 <: R, E1, A1](
    failure: Cause[E] => ZManaged[R1, E1, A1],
    success: A => ZManaged[R1, E1, A1]
  )(implicit trace: Trace): ZManaged[R1, E1, A1] =
    ZManaged(self.zio.foldCauseZIO(failure(_).zio, { case (_, a) => success(a).zio }))

  /**
   * Recovers from errors by accepting one effect to execute for the case of an
   * error, and one effect to execute for the case of success. Only typed
   * failures reach `failure`; when the cause holds both a typed failure and a
   * defect, `failure` receives the typed failure and the defect is dropped.
   */
  def foldManaged[R1 <: R, E2, B](
    failure: E => ZManaged[R1, E2, B],
    success: A => ZManaged[R1, E2, B]
  )(implicit ev: CanFail[E], trace: Trace): ZManaged[R1, E2, B] =
    foldCauseManaged(_.failureOrCause.fold(failure, ZManaged.failCause(_)), success)

  /**
   * Creates a `ZManaged` value that acquires the original resource in a new
   * daemon fiber, and provides that fiber. The finalizer for this value will
   * interrupt the fiber and run the original finalizer.
   */
  def fork(implicit trace: Trace): ZManaged[R, Nothing, Fiber.Runtime[E, A]] =
    ZManaged {
      ZIO.uninterruptibleMask { restore =>
        for {
          outerReleaseMap <- ZManaged.currentReleaseMap.get
          innerReleaseMap <- ReleaseMap.make
          fiber           <- ZManaged.currentReleaseMap.locally(innerReleaseMap)(restore(zio.map(_._2).forkDaemon))
          releaseMapEntry <-
            outerReleaseMap.add(e => fiber.interrupt *> innerReleaseMap.releaseAll(e, ExecutionStrategy.Sequential))
        } yield (releaseMapEntry, fiber)
      }
    }

  /**
   * Returns a new effect that ignores the success or failure of this effect.
   */
  def ignore(implicit trace: Trace): ZManaged[R, Nothing, Unit] =
    fold(_ => (), _ => ())

  /**
   * Returns a new managed effect that ignores defects in finalizers. The change
   * applies to the whole release map this value is acquired in, so it also
   * ignores the defects of other finalizers in that map, including ones
   * composed before or after it with `flatMap` or `zip`. Finalizers on the
   * other side of a `zipPar` are in a different map and are not affected.
   */
  def ignoreReleaseFailures(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged(
      ZManaged.currentReleaseMap.get.tap(
        _.updateAll(finalizer => exit => finalizer(exit).catchAllCause(_ => ZIO.unit))
      ) *> zio
    )

  /**
   * Returns whether this managed effect is a failure.
   */
  def isFailure(implicit trace: Trace): ZManaged[R, Nothing, Boolean] =
    fold(_ => true, _ => false)

  /**
   * Returns whether this managed effect is a success.
   */
  def isSuccess(implicit trace: Trace): ZManaged[R, Nothing, Boolean] =
    fold(_ => false, _ => true)

  /**
   * Returns an effect whose success is mapped by the specified `f` function.
   */
  def map[B](f: A => B)(implicit trace: Trace): ZManaged[R, E, B] =
    ZManaged(zio.map { case (fin, a) => (fin, f(a)) })

  /**
   * Returns an effect whose failure and success channels have been mapped by
   * the specified pair of functions, `f` and `g`.
   */
  def mapBoth[E1, A1](f: E => E1, g: A => A1)(implicit ev: CanFail[E], trace: Trace): ZManaged[R, E1, A1] =
    mapError(f).map(g)

  /**
   * Returns an effect whose success is mapped by the specified side effecting
   * `f` function, translating any non-fatal exception that `f` throws into a
   * typed failure.
   */
  final def mapAttempt[B](
    f: A => B
  )(implicit ev: E IsSubtypeOfError Throwable, trace: Trace): ZManaged[R, Throwable, B] =
    foldManaged(e => ZManaged.fail(ev(e)), a => ZManaged.attempt(f(a)))

  /**
   * Returns an effect whose failure is mapped by the specified `f` function.
   */
  def mapError[E1](f: E => E1)(implicit ev: CanFail[E], trace: Trace): ZManaged[R, E1, A] =
    ZManaged(zio.mapError(f))

  /**
   * Returns an effect whose full failure is mapped by the specified `f`
   * function.
   */
  def mapErrorCause[E1](f: Cause[E] => Cause[E1])(implicit trace: Trace): ZManaged[R, E1, A] =
    ZManaged(zio.mapErrorCause(f))

  /**
   * Effectfully maps the resource acquired by this value.
   */
  def mapZIO[R1 <: R, E1 >: E, B](f: A => ZIO[R1, E1, B])(implicit trace: Trace): ZManaged[R1, E1, B] =
    ZManaged(zio.flatMap { case (fin, a) => f(a).map((fin, _)) })

  /**
   * Returns a managed value whose result is a memoized version of this one. The
   * inner value acquires this resource the first time it is used, and every
   * later use, sequential or parallel, gets the same resource without acquiring
   * it again; a failed acquisition is not retried, and later uses fail with the
   * same error. The resource is added to the release map in which the outer
   * value is acquired, so it is released when that map is released (for `use`,
   * when the `use` effect ends), not when a use of the inner value ends; an
   * early release of the outer value with `withEarlyRelease` does not release
   * it. It is never acquired if the inner value is not used.
   */
  def memoize(implicit trace: Trace): ZManaged[Any, Nothing, ZManaged[R, E, A]] =
    ZManaged.releaseMap.mapZIO { finalizers =>
      for {
        promise <- Promise.make[E, A]
        complete <- ZManaged.currentReleaseMap
                      .locally(finalizers)(self.zio)
                      .map(_._2)
                      .intoPromise(promise)
                      .once
      } yield (complete *> promise.await).toManaged
    }

  /**
   * Returns a new effect where the error channel has been merged into the
   * success channel to their common combined type.
   */
  def merge[A1 >: A](implicit
    ev1: E IsSubtypeOfError A1,
    ev2: CanFail[E],
    trace: Trace
  ): ZManaged[R, Nothing, A1] =
    self.foldManaged(e => ZManaged.succeed(ev1(e)), ZManaged.succeed(_))

  /**
   * Requires the option produced by this value to be `None`.
   */
  final def none[B](implicit ev: A IsSubtypeOfOutput Option[B], trace: Trace): ZManaged[R, Option[E], Unit] =
    self.foldManaged(
      e => ZManaged.fail(Some(e)),
      a => ev(a).fold[ZManaged[R, Option[E], Unit]](ZManaged.succeed(()))(_ => ZManaged.fail(None))
    )

  /**
   * Locks this managed effect to the specified executor, guaranteeing that this
   * managed effect as well as managed effects that are composed sequentially
   * after it will be run on the specified executor. The function passed to
   * `use` and this resource's release action also run on it.
   */
  final def onExecutor(executor: => Executor)(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.onExecutor(executor) *> self

  /**
   * Runs this managed effect, as well as any managed effects that are composed
   * sequentially after it, using the specified `ExecutionContext`.
   */
  final def onExecutionContext(ec: => ExecutionContext)(implicit trace: Trace): ZManaged[R, E, A] =
    self.onExecutor(Executor.fromExecutionContext(ec))

  /**
   * Ensures that a cleanup function runs when this ZManaged is finalized, after
   * the existing finalizers.
   */
  def onExit[R1 <: R](cleanup: Exit[E, A] => ZIO[R1, Nothing, Any])(implicit trace: Trace): ZManaged[R1, E, A] =
    ZManaged {
      ZIO.uninterruptibleMask { restore =>
        for {
          r1              <- ZIO.environment[R1]
          outerReleaseMap <- ZManaged.currentReleaseMap.get
          innerReleaseMap <- ReleaseMap.make
          exitEA          <- ZManaged.currentReleaseMap.locally(innerReleaseMap)(restore(zio.map(_._2)).exit)
          releaseMapEntry <- outerReleaseMap.add { e =>
                               innerReleaseMap
                                 .releaseAll(e, ExecutionStrategy.Sequential)
                                 .exit
                                 .zipWith(cleanup(exitEA).provideEnvironment(r1).exit)((l, r) => l *> r)
                                 .flatten
                             }
          a <- exitEA
        } yield (releaseMapEntry, a)
      }
    }

  /**
   * Ensures that a cleanup function runs when this ZManaged is finalized,
   * before the existing finalizers.
   */
  def onExitFirst[R1 <: R](
    cleanup: Exit[E, A] => ZIO[R1, Nothing, Any]
  )(implicit trace: Trace): ZManaged[R1, E, A] =
    ZManaged {
      ZIO.uninterruptibleMask { restore =>
        for {
          r1              <- ZIO.environment[R1]
          outerReleaseMap <- ZManaged.currentReleaseMap.get
          innerReleaseMap <- ReleaseMap.make
          exitEA <- ZManaged.currentReleaseMap.locally(innerReleaseMap)(
                      restore(zio).exit.map(_.mapExit((t: (ZManaged.Finalizer, A)) => t._2))
                    )
          releaseMapEntry <- outerReleaseMap.add { e =>
                               cleanup(exitEA)
                                 .provideEnvironment(r1)
                                 .exit
                                 .zipWith(innerReleaseMap.releaseAll(e, ExecutionStrategy.Sequential).exit)((l, r) =>
                                   l *> r
                                 )
                                 .flatten
                             }
          a <- exitEA
        } yield (releaseMapEntry, a)
      }
    }

  /**
   * Executes this effect, skipping the error but returning optionally the
   * success.
   */
  def option(implicit ev: CanFail[E], trace: Trace): ZManaged[R, Nothing, Option[A]] =
    fold(_ => None, Some(_))

  /**
   * Translates effect failure into death of the fiber, making all failures
   * unchecked and not a part of the type of the effect.
   */
  def orDie(implicit
    ev1: E IsSubtypeOfError Throwable,
    ev2: CanFail[E],
    trace: Trace
  ): ZManaged[R, Nothing, A] =
    orDieWith(ev1)

  /**
   * Keeps none of the errors, and terminates the fiber with them, using the
   * specified function to convert the `E` into a `Throwable`.
   */
  def orDieWith(f: E => Throwable)(implicit ev: CanFail[E], trace: Trace): ZManaged[R, Nothing, A] =
    mapError(f).catchAll(ZManaged.die(_))

  /**
   * Executes this effect and returns its value, if it succeeds, but otherwise
   * executes the specified effect. `that` runs when the cause holds a typed
   * failure, even when it also holds a defect (which is then dropped); a cause
   * with only defects or interruption is not recovered. `ZIO#orElse` differs:
   * it does not run `that` when the cause holds a defect, and it does run
   * `that` for an interruption the effect caused itself (such as
   * `ZIO.interrupt`).
   */
  def orElse[R1 <: R, E2, A1 >: A](
    that: => ZManaged[R1, E2, A1]
  )(implicit ev: CanFail[E], trace: Trace): ZManaged[R1, E2, A1] =
    foldManaged(_ => that, ZManaged.succeed(_))

  /**
   * Returns an effect that will produce the value of this effect, unless it
   * fails, in which case, it will produce the value of the specified effect.
   */
  def orElseEither[R1 <: R, E2, B](
    that: => ZManaged[R1, E2, B]
  )(implicit ev: CanFail[E], trace: Trace): ZManaged[R1, E2, Either[A, B]] =
    foldManaged(_ => that.map(Right[A, B]), a => ZManaged.succeed(Left[A, B](a)))

  /**
   * Executes this effect and returns its value, if it succeeds, but otherwise
   * fails with the specified error.
   */
  final def orElseFail[E1](e1: => E1)(implicit ev: CanFail[E], trace: Trace): ZManaged[R, E1, A] =
    orElse(ZManaged.fail(e1))

  /**
   * Returns an effect that will produce the value of this effect, unless it
   * fails with the `None` value, in which case it will produce the value of the
   * specified effect.
   */
  final def orElseOptional[R1 <: R, E1, A1 >: A](
    that: => ZManaged[R1, Option[E1], A1]
  )(implicit ev: E IsSubtypeOfError Option[E1], trace: Trace): ZManaged[R1, Option[E1], A1] =
    catchAll(ev(_).fold(that)(e => ZManaged.fail(Some(e))))

  /**
   * Executes this effect and returns its value, if it succeeds, but otherwise
   * succeeds with the specified value.
   */
  final def orElseSucceed[A1 >: A](a1: => A1)(implicit ev: CanFail[E], trace: Trace): ZManaged[R, Nothing, A1] =
    orElse(ZManaged.succeed(a1))

  /**
   * Acquires the managed resource now and succeeds with a `Managed` that cannot
   * fail: acquiring that `Managed` gives the resource already acquired and
   * registers its finalizer, which then runs when that `Managed` is released.
   * If the acquisition fails, the finalizers it registered run at once, and the
   * effect fails with that cause. You should take care that you are not
   * interrupted between running preallocate and actually acquiring the resource
   * as you might leak otherwise.
   */
  def preallocate(implicit trace: Trace): ZIO[R, E, Managed[Nothing, A]] =
    ZIO.uninterruptibleMask { restore =>
      for {
        releaseMap <- ReleaseMap.make
        tp         <- restore(ZManaged.currentReleaseMap.locally(releaseMap)(self.zio)).exit
        preallocated <- tp.foldExitZIO(
                          c =>
                            releaseMap
                              .releaseAll(Exit.fail(c), ExecutionStrategy.Sequential) *>
                              Exit.failCause(c),
                          { case (release, a) =>
                            ZIO.succeed(
                              ZManaged {
                                ZManaged.currentReleaseMap.get.flatMap { releaseMap =>
                                  releaseMap.add(release).map((_, a))
                                }
                              }
                            )
                          }
                        )
      } yield preallocated
    }

  /**
   * Like `preallocate`, but as a `ZManaged`: acquiring the outer value acquires
   * this resource, and the inner `Managed` cannot fail and gives that resource.
   * The resource is released once: when the inner value is released, or when
   * the outer value is released if the inner value was not used.
   */
  def preallocateManaged(implicit trace: Trace): ZManaged[R, E, Managed[Nothing, A]] =
    ZManaged {
      self.zio.map { case (release, a) =>
        (
          release,
          ZManaged {
            ZManaged.currentReleaseMap.get.flatMap { releaseMap =>
              releaseMap.add(release).map((_, a))
            }
          }
        )
      }
    }

  /**
   * Provides the `ZManaged` effect with its required environment, which
   * eliminates its dependency on `R`.
   */
  def provideEnvironment(r: => ZEnvironment[R])(implicit trace: Trace): Managed[E, A] =
    provideSomeEnvironment(_ => r)

  /**
   * Builds `layer` as part of the acquisition and provides its output to this
   * `ZManaged`, so the result only needs the layer's input `R0`.
   */
  final def provideLayer[E1 >: E, R0](
    layer: => ZLayer[R0, E1, R]
  )(implicit trace: Trace): ZManaged[R0, E1, A] =
    ZManaged(
      Scope.make.flatMap { scope =>
        layer.build
          .provideSomeEnvironment[R0](_.union[Scope](ZEnvironment(scope)))
          .map(r => ((exit: Exit[Any, Any]) => scope.close(exit), r))
      }
    ).flatMap(self.provideEnvironment(_))

  /**
   * Transforms the environment being provided to this effect with the specified
   * function.
   */
  def provideSomeEnvironment[R0](
    f: ZEnvironment[R0] => ZEnvironment[R]
  )(implicit trace: Trace): ZManaged[R0, E, A] =
    ZManaged(zio.provideSomeEnvironment(f))

  /**
   * Splits the environment into two parts, providing one part using the
   * specified layer and leaving the remainder `R0`.
   *
   * {{{
   * import zio._
   *
   * trait Logging
   * trait Database
   *
   * val loggingLayer: ZLayer[Any, Nothing, Logging] =
   *   ZLayer.succeed(new Logging {})
   *
   * val managed: ZManaged[Logging with Database, Nothing, Unit] = ZManaged.unit
   *
   * val managed2: ZManaged[Database, Nothing, Unit] =
   *   managed.provideSomeLayer[Database](loggingLayer)
   * }}}
   */
  final def provideSomeLayer[R0]: ZManaged.ProvideSomeLayer[R0, R, E, A] =
    new ZManaged.ProvideSomeLayer[R0, R, E, A](self)

  /**
   * Keeps some of the errors, and terminates the fiber with the rest.
   */
  def refineOrDie[E1](
    pf: PartialFunction[E, E1]
  )(implicit ev1: E IsSubtypeOfError Throwable, ev2: CanFail[E], trace: Trace): ZManaged[R, E1, A] =
    refineOrDieWith(pf)(ev1)

  /**
   * Keeps some of the errors, and terminates the fiber with the rest, using the
   * specified function to convert the `E` into a `Throwable`.
   */
  def refineOrDieWith[E1](
    pf: PartialFunction[E, E1]
  )(f: E => Throwable)(implicit ev: CanFail[E], trace: Trace): ZManaged[R, E1, A] =
    catchAll(e => pf.lift(e).fold[ZManaged[R, E1, A]](ZManaged.die(f(e)))(ZManaged.fail(_)))

  /**
   * Fail with the returned value if the `PartialFunction` matches, otherwise
   * continue with our held value.
   */
  def reject[E1 >: E](pf: PartialFunction[A, E1])(implicit trace: Trace): ZManaged[R, E1, A] =
    rejectManaged(pf.andThen(ZManaged.fail(_)))

  /**
   * Continue with the returned computation if the `PartialFunction` matches,
   * translating the successful match into a failure, otherwise continue with
   * our held value.
   */
  def rejectManaged[R1 <: R, E1 >: E](
    pf: PartialFunction[A, ZManaged[R1, E1, E1]]
  )(implicit trace: Trace): ZManaged[R1, E1, A] =
    self.flatMap { v =>
      pf.andThen[ZManaged[R1, E1, A]](_.flatMap(ZManaged.fail(_)))
        .applyOrElse[A, ZManaged[R1, E1, A]](v, ZManaged.succeed(_))
    }

  /**
   * Acquires this resource and releases it right away, succeeding with the
   * acquired value. In `a *> b.release *> c`, `b` is released before `c` is
   * acquired, while `a` is released at the end as usual. This is useful to
   * conceptually "close" a scope when composing multiple managed effects. Note
   * that this is only safe if the result of this managed effect is valid
   * outside its scope.
   */
  def release(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.fromZIO(useNow)

  /**
   * Returns a `Reservation` that allows separately accessing effects describing
   * resource acquisition and release. Its `release` runs, one after another,
   * every finalizer that its `acquire` registered.
   */
  def reserve(implicit trace: Trace): UIO[Reservation[R, E, A]] =
    ReleaseMap.make.map { releaseMap =>
      Reservation(
        ZManaged.currentReleaseMap.locally(releaseMap)(zio).map(_._2),
        releaseMap.releaseAll(_, ExecutionStrategy.Sequential)
      )
    }

  /**
   * Retries the acquisition of this resource with the specified retry policy.
   * Retries are done following a failure of the acquisition (up to a fixed
   * maximum with `once` or `recurs` for example), so that
   * `managed.retry(Schedule.once)` means "acquire `managed` and in case of
   * failure, try again once".
   */
  def retry[R1 <: R, S](
    policy: => Schedule[R1, E, S]
  )(implicit ev: CanFail[E], trace: Trace): ZManaged[R1, E, A] =
    ZManaged(zio.retry(policy))

  /**
   * Returns a managed value that succeeds with the `Exit` of this one's
   * acquisition: the resource on success, or the full `Cause` on failure,
   * including defects and an interruption the acquisition caused itself. It
   * does not catch an interruption of the fiber from outside.
   */
  def exit(implicit trace: Trace): ZManaged[R, Nothing, Exit[E, A]] =
    foldCauseManaged(
      cause => ZManaged.succeed(Exit.failCause(cause)),
      succ => ZManaged.succeed(Exit.succeed(succ))
    )

  /**
   * Exposes the full cause of failure of this effect.
   */
  def sandbox(implicit trace: Trace): ZManaged[R, Cause[E], A] =
    ZManaged(zio.sandbox)

  /**
   * Companion helper to `sandbox`. Allows recovery, and partial recovery, from
   * errors and defects alike.
   */
  def sandboxWith[R1 <: R, E2, B](
    f: ZManaged[R1, Cause[E], A] => ZManaged[R1, Cause[E2], B]
  )(implicit trace: Trace): ZManaged[R1, E2, B] =
    ZManaged.unsandbox(f(self.sandbox))

  /**
   * Converts this managed value into a scoped `ZIO`. Running the effect
   * acquires the resource, and its finalizers run in reverse order when the
   * `Scope` closes, together with the other finalizers of that scope.
   * `ZManaged.scoped` converts in the other direction.
   *
   * {{{
   * import zio._
   *
   * val counter: ZManaged[Any, Nothing, Ref[Int]] =
   *   ZManaged.acquireReleaseWith(Ref.make(0))(_ => ZIO.unit)
   *
   * val program: UIO[Int] =
   *   ZIO.scoped {
   *     counter.scoped.flatMap(ref => ref.updateAndGet(_ + 1))
   *   }
   * }}}
   */
  def scoped(implicit trace: Trace): ZIO[R with Scope, E, A] =
    for {
      scope      <- ZIO.scope
      releaseMap <- ZManaged.ReleaseMap.make
      _          <- scope.addFinalizerExit(releaseMap.releaseAll(_, ExecutionStrategy.Sequential))
      tuple      <- ZManaged.currentReleaseMap.locally(releaseMap)(zio)
      (_, a)      = tuple
    } yield a

  /**
   * Converts an option on values into an option on errors.
   */
  final def some[B](implicit ev: A IsSubtypeOfOutput Option[B], trace: Trace): ZManaged[R, Option[E], B] =
    self.foldManaged(
      e => ZManaged.fail(Some(e)),
      a => ev(a).fold[ZManaged[R, Option[E], B]](ZManaged.fail(Option.empty[E]))(ZManaged.succeed(_))
    )

  /**
   * Extracts the optional value, or returns the given 'default'. Superseded by
   * `someOrElse` with better type inference. This method was left for binary
   * compatibility.
   */
  protected final def someOrElse[B](
    default: => B
  )(implicit ev: A IsSubtypeOfOutput Option[B], trace: Trace): ZManaged[R, E, B] =
    map(a => ev(a).getOrElse(default))

  /**
   * Extracts the optional value, or returns the given 'default'.
   */
  final def someOrElse[B, C](
    default: => C
  )(implicit ev0: A IsSubtypeOfOutput Option[B], ev1: C <:< B, trace: Trace): ZManaged[R, E, B] =
    map(a => ev0(a).getOrElse(default))

  /**
   * Extracts the optional value, or executes the effect 'default'. Superseded
   * by `someOrElseManaged` with better type inference. This method was left for
   * binary compatibility.
   */
  protected final def someOrElseManaged[B, R1 <: R, E1 >: E](
    default: => ZManaged[R1, E1, B]
  )(implicit ev: A IsSubtypeOfOutput Option[B], trace: Trace): ZManaged[R1, E1, B] =
    self.flatMap(ev(_) match {
      case Some(value) => ZManaged.succeed(value)
      case None        => default
    })

  /**
   * Extracts the optional value, or executes the effect 'default'.
   */
  final def someOrElseManaged[B, R1 <: R, E1 >: E, C](
    default: => ZManaged[R1, E1, C]
  )(implicit ev0: A IsSubtypeOfOutput Option[B], ev1: C <:< B, trace: Trace): ZManaged[R1, E1, B] =
    self.flatMap(ev0(_) match {
      case Some(value) => ZManaged.succeed(value)
      case None        => default.map(ev1)
    })

  /**
   * Extracts the optional value, or fails with the given error 'e'.
   */
  final def someOrFail[B, E1 >: E](
    e: => E1
  )(implicit ev: A IsSubtypeOfOutput Option[B], trace: Trace): ZManaged[R, E1, B] =
    self.flatMap(ev(_) match {
      case Some(value) => ZManaged.succeed(value)
      case None        => ZManaged.fail(e)
    })

  /**
   * Extracts the optional value, or fails with a
   * `java.util.NoSuchElementException` whose message is `None.get`.
   */
  final def someOrFailException[B, E1 >: E](implicit
    ev: A IsSubtypeOfOutput Option[B],
    ev2: NoSuchElementException <:< E1,
    trace: Trace
  ): ZManaged[R, E1, B] =
    self.foldManaged(
      e => ZManaged.fail(e),
      ev(_) match {
        case Some(value) => ZManaged.succeed(value)
        case None        => ZManaged.fail(ev2(new NoSuchElementException("None.get")))
      }
    )

  /**
   * Returns an effect that effectfully peeks at the acquired resource.
   */
  def tap[R1 <: R, E1 >: E](f: A => ZManaged[R1, E1, Any])(implicit trace: Trace): ZManaged[R1, E1, A] =
    flatMap(a => f(a).as(a))

  /**
   * Returns an effect that effectfully peeks at the failure or success of the
   * acquired resource.
   */
  def tapBoth[R1 <: R, E1 >: E](f: E => ZManaged[R1, E1, Any], g: A => ZManaged[R1, E1, Any])(implicit
    ev: CanFail[E],
    trace: Trace
  ): ZManaged[R1, E1, A] =
    foldManaged(
      e => f(e) *> ZManaged.fail(e),
      a => g(a).as(a)
    )

  /**
   * Returns an effect that effectually "peeks" at the defect of the acquired
   * resource. `f` receives the cause with its typed failures removed. It is
   * called for every failed acquisition except an interruption from outside, so
   * a cause with only typed failures calls `f` with an empty cause.
   * `ZIO#tapDefect` differs: it does not call `f` when the cause has no defect.
   */
  final def tapDefect[R1 <: R, E1 >: E](f: Cause[Nothing] => ZManaged[R1, E1, Any])(implicit
    trace: Trace
  ): ZManaged[R1, E1, A] =
    catchAllCause(c => f(c.stripFailures) *> ZManaged.failCause(c))

  /**
   * Returns an effect that effectfully peeks at the failure of the acquired
   * resource.
   */
  def tapError[R1 <: R, E1 >: E](
    f: E => ZManaged[R1, E1, Any]
  )(implicit ev: CanFail[E], trace: Trace): ZManaged[R1, E1, A] =
    tapBoth(f, ZManaged.succeed(_))

  /**
   * Returns an effect that effectually peeks at the cause of the failure of the
   * acquired resource.
   */
  final def tapErrorCause[R1 <: R, E1 >: E](f: Cause[E] => ZManaged[R1, E1, Any])(implicit
    trace: Trace
  ): ZManaged[R1, E1, A] =
    catchAllCause(c => f(c) *> ZManaged.failCause(c))

  /**
   * Like [[ZManaged#tap]], but uses a function that returns a ZIO value rather
   * than a ZManaged value.
   */
  def tapZIO[R1 <: R, E1 >: E](f: A => ZIO[R1, E1, Any])(implicit trace: Trace): ZManaged[R1, E1, A] =
    mapZIO(a => f(a).as(a))

  /**
   * Returns a new effect that executes this one and times the acquisition of
   * the resource.
   */
  def timed(implicit trace: Trace): ZManaged[R, E, (Duration, A)] =
    ZManaged {
      self.zio.timed.map { case (duration, (fin, a)) =>
        (fin, (duration, a))
      }
    }

  /**
   * Returns an effect that will timeout this resource. It succeeds with `Some`
   * of the resource if the acquisition completes within `d`, and fails if the
   * acquisition fails within `d`. Otherwise it succeeds with `None` as soon as
   * `d` elapses, without waiting for the acquisition, and a new daemon fiber
   * interrupts the acquisition. An uninterruptible acquisition, such as one
   * from `acquireReleaseWith`, still runs to the end, and its resource is then
   * released.
   */
  def timeout(d: => Duration)(implicit trace: Trace): ZManaged[R, E, Option[A]] =
    ZManaged {
      ZIO.uninterruptibleMask { restore =>
        for {
          outerReleaseMap <- ZManaged.currentReleaseMap.get
          innerReleaseMap <- ZManaged.ReleaseMap.make
          earlyRelease    <- outerReleaseMap.add(innerReleaseMap.releaseAll(_, ExecutionStrategy.Sequential))
          raceResult <- restore {
                          ZManaged.currentReleaseMap
                            .locally(innerReleaseMap)(zio)
                            .raceWith(ZIO.sleep(d).as(None))(
                              (result, sleeper) => sleeper.interrupt *> result.map(tp => Right(tp._2)),
                              (_, resultFiber) => ZIO.succeed(Left(resultFiber))
                            )
                        }
          a <- raceResult match {
                 case Right(value) => ZIO.succeed(Some(value))
                 case Left(fiber) =>
                   ZIO.fiberIdWith { id =>
                     fiber.interrupt
                       .ensuring(innerReleaseMap.releaseAll(Exit.interrupt(id), ExecutionStrategy.Sequential))
                       .forkDaemon
                   }.as(None)
               }
        } yield (earlyRelease, a)
      }
    }

  /**
   * Constructs a layer from this managed resource. Building the layer acquires
   * the resource, and it is released when the scope of the layer closes.
   */
  def toLayer[A1 >: A: Tag](implicit trace: Trace): ZLayer[R, E, A1] =
    ZLayer.scoped[R][E, A1](self.scoped)

  /**
   * Constructs a layer from this managed resource, which must return one or
   * more services.
   */
  final def toLayerEnvironment[B](implicit
    ev: A <:< ZEnvironment[B],
    trace: Trace
  ): ZLayer[R, E, B] =
    ZLayer.scopedEnvironment[R](self.map(ev).scoped)

  /**
   * Return unit while running the effect
   */
  def unit(implicit trace: Trace): ZManaged[R, E, Unit] =
    as(())

  /**
   * The moral equivalent of `if (!p) exp`
   */
  final def unless(b: => Boolean)(implicit trace: Trace): ZManaged[R, E, Option[A]] =
    ZManaged.unless(b)(self)

  /**
   * The moral equivalent of `if (!p) exp` when `p` has side-effects
   */
  final def unlessManaged[R1 <: R, E1 >: E](b: => ZManaged[R1, E1, Boolean])(implicit
    trace: Trace
  ): ZManaged[R1, E1, Option[A]] =
    ZManaged.unlessManaged(b)(self)

  /**
   * The inverse operation of `sandbox`: submerges the full `Cause` in the error
   * channel back into the failure of the effect.
   */
  def unsandbox[E1](implicit ev: E IsSubtypeOfError Cause[E1], trace: Trace): ZManaged[R, E1, A] =
    ZManaged.unsandbox(mapError(ev))

  /**
   * Converts an option on errors into an option on values.
   */
  final def unsome[E1](implicit ev: E IsSubtypeOfError Option[E1], trace: Trace): ZManaged[R, E1, Option[A]] =
    self.foldManaged(
      e => ev(e).fold[ZManaged[R, E1, Option[A]]](ZManaged.succeed(Option.empty[A]))(ZManaged.fail(_)),
      a => ZManaged.succeed(Some(a))
    )

  /**
   * Updates a service in the environment of this effect.
   */
  final def updateService[M] =
    new ZManaged.UpdateService[R, E, A, M](self)

  /**
   * Updates a service at the specified key in the environment of this effect.
   */
  final def updateServiceAt[Service]: ZManaged.UpdateServiceAt[R, E, A, Service] =
    new ZManaged.UpdateServiceAt[R, E, A, Service](self)

  /**
   * Run an effect while acquiring the resource before and releasing it after.
   * The resource is released whether `f` succeeds, fails, dies or is
   * interrupted, and also when the acquisition fails partway. Resources
   * acquired in sequence are released one after another, in reverse order of
   * acquisition (the two sides of a `zipPar` are released in parallel), and the
   * finalizers receive the `Exit` value of the whole effect.
   */
  def use[R1 <: R, E1 >: E, B](f: A => ZIO[R1, E1, B])(implicit trace: Trace): ZIO[R1, E1, B] =
    ReleaseMap.make.flatMap { releaseMap =>
      ZManaged.currentReleaseMap.locally(releaseMap) {
        ZIO.acquireReleaseExitWith {
          ZManaged.currentReleaseMap.get
        } { (relMap, exit: Exit[E1, B]) =>
          relMap.releaseAll(exit, ExecutionStrategy.Sequential)
        } { relMap =>
          zio.flatMap { case (_, a) => f(a) }
        }
      }
    }

  /**
   * Run an effect while acquiring the resource before and releasing it after.
   * This does not provide the resource to the function
   */
  def useDiscard[R1 <: R, E1 >: E, B](f: => ZIO[R1, E1, B])(implicit trace: Trace): ZIO[R1, E1, B] =
    use(_ => f)

  /**
   * Use the resource until interruption. Useful for resources that you want to
   * acquire and use as long as the application is running, like a HTTP server.
   */
  def useForever(implicit trace: Trace): ZIO[R, E, Nothing] =
    use(_ => ZIO.never)

  /**
   * Runs the acquire and release actions and returns the result of this managed
   * effect. Note that this is only safe if the result of this managed effect is
   * valid outside its scope.
   */
  def useNow(implicit trace: Trace): ZIO[R, E, A] =
    use(ZIO.succeed(_))

  /**
   * The moral equivalent of `if (p) exp`
   */
  def when(b: => Boolean)(implicit trace: Trace): ZManaged[R, E, Option[A]] =
    ZManaged.when(b)(self)

  /**
   * The moral equivalent of `if (p) exp` when `p` has side-effects
   */
  def whenManaged[R1 <: R, E1 >: E](b: => ZManaged[R1, E1, Boolean])(implicit
    trace: Trace
  ): ZManaged[R1, E1, Option[A]] =
    ZManaged.whenManaged(b)(self)

  /**
   * Modifies this `ZManaged` to provide a canceler that can be used to eagerly
   * execute the finalizer of this `ZManaged`. The canceler will run
   * uninterruptibly with an exit value indicating that the effect was
   * interrupted, and if completed will cause the regular finalizer to not run.
   */
  def withEarlyRelease(implicit trace: Trace): ZManaged[R, E, (UIO[Any], A)] =
    ZManaged.fiberId.flatMap(fiberId => withEarlyReleaseExit(Exit.interrupt(fiberId)))

  /**
   * A more powerful version of [[withEarlyRelease]] that allows specifying an
   * exit value in the event of early release.
   */
  def withEarlyReleaseExit(e: => Exit[Any, Any])(implicit trace: Trace): ZManaged[R, E, (UIO[Any], A)] =
    ZManaged(zio.map(tp => (tp._1, (tp._1(e).uninterruptible, tp._2))))

  /**
   * Runs this managed effect with the specified maximum number of fibers for
   * parallel operators, guaranteeing that this managed effect as well as
   * managed effects that are composed sequentially after it will be run with
   * the specified maximum number of fibers for parallel operators.
   */
  def withParallelism(n: => Int)(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.withParallism(n) *> self

  /**
   * Runs this managed effect with an unbounded maximum number of fibers for
   * parallel operators, guaranteeing that this managed effect as well as
   * managed effects that are composed sequentially after it will be run with an
   * unbounded maximum number of fibers for parallel operators.
   */
  def withParallelismUnbounded(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.withParallismUnbounded *> self

  /**
   * Named alias for `<*>`. Acquires this resource and then `that`, and succeeds
   * with both values combined by `Zippable` (a flat tuple, with `Unit` values
   * left out). On release, `that` is released first.
   */
  def zip[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit
    zippable: Zippable[A, A1],
    trace: Trace
  ): ZManaged[R1, E1, zippable.Out] =
    self <*> that

  /**
   * Named alias for `<*`.
   */
  def zipLeft[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit trace: Trace): ZManaged[R1, E1, A] =
    self <* that

  /**
   * Named alias for `<&>`. Acquires this resource and `that` in parallel, and
   * succeeds with both values combined by `Zippable` (a flat tuple, with `Unit`
   * values left out); see `zipWithPar` for failure and release.
   */
  def zipPar[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit
    zippable: Zippable[A, A1],
    trace: Trace
  ): ZManaged[R1, E1, zippable.Out] =
    self <&> that

  /**
   * Named alias for `<&`.
   */
  def zipParLeft[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit
    trace: Trace
  ): ZManaged[R1, E1, A] =
    self <& that

  /**
   * Named alias for `&>`.
   */
  def zipParRight[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit
    trace: Trace
  ): ZManaged[R1, E1, A1] =
    self &> that

  /**
   * Named alias for `*>`.
   */
  def zipRight[R1 <: R, E1 >: E, A1](that: => ZManaged[R1, E1, A1])(implicit
    trace: Trace
  ): ZManaged[R1, E1, A1] =
    self *> that

  /**
   * Returns an effect that executes both this effect and the specified effect,
   * in sequence, combining their results with the specified `f` function.
   */
  def zipWith[R1 <: R, E1 >: E, A1, A2](that: => ZManaged[R1, E1, A1])(f: (A, A1) => A2)(implicit
    trace: Trace
  ): ZManaged[R1, E1, A2] =
    flatMap(r => that.map(r1 => f(r, r1)))

  /**
   * Returns an effect that executes both this effect and the specified effect,
   * in parallel, combining their results with the specified `f` function. If
   * either side fails, then the other side will be interrupted. On release, the
   * two sides are released in parallel, and the finalizers of each side run one
   * after another in reverse order.
   */
  def zipWithPar[R1 <: R, E1 >: E, A1, A2](
    that: => ZManaged[R1, E1, A1]
  )(f: (A, A1) => A2)(implicit trace: Trace): ZManaged[R1, E1, A2] =
    ZManaged.ReleaseMap.makeManaged(ExecutionStrategy.Parallel).mapZIO { parallelReleaseMap =>
      val innerMap =
        ZManaged.currentReleaseMap
          .locally(parallelReleaseMap)(ZManaged.ReleaseMap.makeManaged(ExecutionStrategy.Sequential).zio)

      (innerMap zip innerMap) flatMap { case (_, l, (_, r)) =>
        ZManaged.currentReleaseMap
          .locally(l)(self.zio)
          .zipWithPar(ZManaged.currentReleaseMap.locally(r)(that.zio)) {
            // We can safely discard the finalizers here because the resulting ZManaged's early
            // release will trigger the ReleaseMap, which would release both finalizers in
            // parallel.
            case ((_, a), (_, b)) =>
              f(a, b)
          }
      }
    }
}

/**
 * Constructors and helpers for [[ZManaged]] values.
 *
 * A `ZManaged` pairs an acquire action with a release action. `use` runs the
 * acquire actions, passes the resource to a function, and runs the release
 * actions when that function succeeds, fails or is interrupted. Resources
 * acquired one after another (with `flatMap` or `foreach`) are released in
 * reverse order of acquisition.
 *
 * {{{
 * import zio._
 *
 * val program: UIO[List[String]] =
 *   for {
 *     log <- Ref.make(List.empty[String])
 *     resource = (name: String) =>
 *                  ZManaged.acquireRelease(log.update(s"acquire \$name" :: _).as(name))(
 *                    log.update(s"release \$name" :: _)
 *                  )
 *     _      <- (resource("a") *> resource("b")).useDiscard(log.update("use" :: _))
 *     result <- log.get.map(_.reverse)
 *   } yield result
 * // List(acquire a, acquire b, use, release b, release a)
 * }}}
 *
 * In ZIO 2, `Scope` and scoped `ZIO` effects replace `ZManaged`;
 * `ZManaged.scoped` converts a scoped effect into a `ZManaged`.
 *
 * @see
 *   [[https://zio.dev/guides/migrate/zio-2.x-migration-guide/ ZIO 2.x Migration Guide]]
 */
object ZManaged extends ZManagedPlatformSpecific {

  /**
   * The [[ReleaseMap]] of the current scope, kept in a `FiberRef`. `use`
   * installs a new `ReleaseMap` here while the managed value runs, and the
   * `acquireRelease` constructors add their release actions to it. It is a
   * low-level hook for writing new `ZManaged` constructors.
   */
  lazy val currentReleaseMap: FiberRef[ReleaseMap] =
    FiberRef.unsafe.make[ReleaseMap](ReleaseMap.unsafe.make()(Unsafe.unsafe))(Unsafe.unsafe)

  private sealed abstract class State
  private final case class Exited(nextKey: Long, exit: Exit[Any, Any], update: Finalizer => Finalizer) extends State
  private final case class Running(nextKey: Long, finalizers: LongMap[Finalizer], update: Finalizer => Finalizer)
      extends State

  /**
   * The partially applied form of `ZManaged.environmentWith`, which fixes the
   * environment type `R`.
   */
  final class EnvironmentWithPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Returns a `ZManaged` that succeeds with `f` applied to the environment.
     */
    def apply[A](f: ZEnvironment[R] => A)(implicit trace: Trace): ZManaged[R, Nothing, A] =
      ZManaged.environment.map(f)
  }

  /**
   * The partially applied form of `ZManaged.environmentWithZIO`, which fixes
   * the environment type `R`.
   */
  final class EnvironmentWithZIOPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Returns a `ZManaged` that runs the `ZIO` effect that `f` returns for the
     * environment. The result has no release action.
     */
    def apply[R1 <: R, E, A](f: ZEnvironment[R] => ZIO[R1, E, A])(implicit
      trace: Trace
    ): ZManaged[R with R1, E, A] =
      ZManaged.environment.mapZIO(f)
  }

  /**
   * The partially applied form of `ZManaged.environmentWithManaged`, which
   * fixes the environment type `R`.
   */
  final class EnvironmentWithManagedPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Returns the `ZManaged` that `f` builds from the environment, with the
     * release actions of that `ZManaged`.
     */
    def apply[R1 <: R, E, A](f: ZEnvironment[R] => ZManaged[R1, E, A])(implicit
      trace: Trace
    ): ZManaged[R with R1, E, A] =
      ZManaged.environment.flatMap(f)
  }

  /**
   * The partially applied form of `ZManaged.scoped`, which fixes the
   * environment type `R` that is left after the `Scope` is provided.
   */
  final class ScopedPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Converts a scoped effect into a `ZManaged`. Acquiring the result creates
     * a new `Scope` and runs `scoped` in it. Releasing the result closes that
     * `Scope` with the exit value, which runs the finalizers that `scoped`
     * added to it.
     */
    def apply[E, A](scoped: => ZIO[zio.Scope with R, E, A])(implicit trace: Trace): ZManaged[R, E, A] =
      ZManaged.acquireReleaseExitWith(zio.Scope.make)((scope, exit) => scope.close(exit)).mapZIO { scope =>
        scoped.provideSomeEnvironment[R](_.union[zio.Scope](ZEnvironment(scope)))
      }
  }

  /**
   * The partially applied form of `ZManaged.serviceAt`, which fixes the
   * `Service` type.
   */
  final class ServiceAtPartiallyApplied[Service](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Returns a `ZManaged` that succeeds with the service stored under `key` in
     * the `Map[Key, Service]` of the environment, or with `None` when the map
     * has no such key.
     */
    def apply[Key](
      key: => Key
    )(implicit
      tag: EnvironmentTag[Map[Key, Service]],
      trace: Trace
    ): ZManaged[Map[Key, Service], Nothing, Option[Service]] =
      ZManaged.environmentWith(_.getAt(key))
  }

  /**
   * The partially applied form of `ZManaged.serviceWith`, which fixes the
   * `Service` type.
   */
  final class ServiceWithPartiallyApplied[Service](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Returns a `ZManaged` that succeeds with `f` applied to the `Service` in
     * the environment.
     */
    def apply[A](f: Service => A)(implicit
      tag: Tag[Service],
      trace: Trace
    ): ZManaged[Service, Nothing, A] =
      ZManaged.fromZIO(ZIO.serviceWith[Service](f))
  }

  /**
   * The partially applied form of `ZManaged.serviceWithZIO`, which fixes the
   * `Service` type.
   */
  final class ServiceWithZIOPartiallyApplied[Service](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Returns a `ZManaged` that runs the `ZIO` effect that `f` returns for the
     * `Service` in the environment. The result has no release action.
     */
    def apply[R <: Service, E, A](f: Service => ZIO[R, E, A])(implicit
      tag: Tag[Service],
      trace: Trace
    ): ZManaged[R with Service, E, A] =
      ZManaged.fromZIO(ZIO.serviceWithZIO[Service](f))
  }

  /**
   * The partially applied form of `ZManaged.serviceWithManaged`, which fixes
   * the `Service` type.
   */
  final class ServiceWithManagedPartiallyApplied[Service](private val dummy: Boolean = true) extends AnyVal {

    /**
     * Returns the `ZManaged` that `f` builds from the `Service` in the
     * environment, with the release actions of that `ZManaged`.
     */
    def apply[R <: Service, E, A](f: Service => ZManaged[R, E, A])(implicit
      tag: Tag[Service],
      trace: Trace
    ): ZManaged[R with Service, E, A] =
      ZManaged.environmentWithManaged(environment => f(environment.get[Service]))
  }

  /**
   * A finalizer used in a [[ReleaseMap]]. The [[Exit]] value passed to it is
   * the exit value of the scope: for [[ZManaged#use]], the exit of the whole
   * `use` effect. A finalizer run through `ReleaseMap#release` or
   * `ReleaseMap#releaseAll` receives the `Exit` value passed to that method.
   */
  type Finalizer = Exit[Any, Any] => UIO[Any]

  /**
   * Holds the no-op [[Finalizer]].
   */
  object Finalizer {

    /**
     * A finalizer that ignores the exit value and does nothing.
     */
    val noop: Finalizer = _ => ZIO.unit
  }

  /**
   * The partially applied form of `ZManaged.ifManaged`, which holds the
   * condition.
   */
  final class IfManaged[R, E](private val b: () => ZManaged[R, E, Boolean]) extends AnyVal {

    /**
     * Returns a `ZManaged` that runs the condition, then continues with
     * `onTrue` when it is `true` and with `onFalse` when it is `false`.
     */
    def apply[R1 <: R, E1 >: E, A](
      onTrue: => ZManaged[R1, E1, A],
      onFalse: => ZManaged[R1, E1, A]
    )(implicit trace: Trace): ZManaged[R1, E1, A] =
      ZManaged.suspend(b().flatMap(b => if (b) onTrue else onFalse))
  }

  /**
   * The partially applied form of `provideSomeLayer` on a [[ZManaged]], which
   * fixes the environment type `R0` that is left to provide.
   */
  final class ProvideSomeLayer[R0, -R, +E, +A](private val self: ZManaged[R, E, A]) extends AnyVal {

    /**
     * Provides the `R1` part of the environment with `layer` and leaves `R0` to
     * be provided later. The layer is built as part of the acquisition.
     */
    def apply[E1 >: E, R1](
      layer: => ZLayer[R0, E1, R1]
    )(implicit
      ev: R0 with R1 <:< R,
      tagged: EnvironmentTag[R1],
      trace: Trace
    ): ZManaged[R0, E1, A] =
      self.asInstanceOf[ZManaged[R0 with R1, E, A]].provideLayer(ZLayer.environment[R0] <*> layer)
  }

  /**
   * The partially applied form of `ZManaged.unlessManaged`, which holds the
   * condition.
   */
  final class UnlessManaged[R, E](private val b: () => ZManaged[R, E, Boolean]) extends AnyVal {

    /**
     * Returns a `ZManaged` that runs the condition, then runs `managed` and
     * wraps its result in `Some` when the condition is `false`, or succeeds
     * with `None` when it is `true`.
     */
    def apply[R1 <: R, E1 >: E, A](managed: => ZManaged[R1, E1, A])(implicit
      trace: Trace
    ): ZManaged[R1, E1, Option[A]] =
      ZManaged.suspend(b().flatMap(b => if (b) none else managed.asSome))
  }

  /**
   * The partially applied form of `updateService` on a [[ZManaged]], which
   * fixes the service type `M`.
   */
  final class UpdateService[-R, +E, +A, M](private val self: ZManaged[R, E, A]) extends AnyVal {

    /**
     * Runs the `ZManaged` with the service of type `M` in its environment
     * replaced by `f` applied to it.
     */
    def apply[R1 <: R with M](
      f: M => M
    )(implicit tag: Tag[M], trace: Trace): ZManaged[R1, E, A] =
      self.provideSomeEnvironment(_.update(f))
  }

  /**
   * The partially applied form of `updateServiceAt` on a [[ZManaged]], which
   * fixes the `Service` type.
   */
  final class UpdateServiceAt[-R, +E, +A, Service](private val self: ZManaged[R, E, A]) extends AnyVal {

    /**
     * Runs the `ZManaged` with the service stored under `key` in the `Map[Key,
     * Service]` of its environment replaced by `f` applied to it.
     */
    def apply[R1 <: R with Map[Key, Service], Key](key: => Key)(
      f: Service => Service
    )(implicit tag: Tag[Map[Key, Service]], trace: Trace): ZManaged[R1, E, A] =
      self.provideSomeEnvironment(_.updateAt(key)(f))
  }

  /**
   * The partially applied form of `ZManaged.whenManaged`, which holds the
   * condition.
   */
  final class WhenManaged[R, E](private val b: () => ZManaged[R, E, Boolean]) extends AnyVal {

    /**
     * Returns a `ZManaged` that runs the condition, then runs `managed` and
     * wraps its result in `Some` when the condition is `true`, or succeeds with
     * `None` when it is `false`.
     */
    def apply[R1 <: R, E1 >: E, A](managed: => ZManaged[R1, E1, A])(implicit
      trace: Trace
    ): ZManaged[R1, E1, Option[A]] =
      ZManaged.suspend(b().flatMap(b => if (b) managed.asSome else none))
  }

  /**
   * A `ReleaseMap` represents the finalizers associated with a scope. `use`
   * creates one, and the `acquireRelease` constructors add their release
   * actions to it. `releaseAll` with `ExecutionStrategy.Sequential` runs the
   * finalizers in reverse order of insertion.
   *
   * The design of `ReleaseMap` is inspired by ResourceT, written by Michael
   * Snoyman @snoyberg.
   * (https://github.com/snoyberg/conduit/blob/master/resourcet/Control/Monad/Trans/Resource/Internal.hs)
   */
  abstract class ReleaseMap extends Serializable {

    /**
     * An opaque identifier for a finalizer stored in the map.
     */
    type Key

    /**
     * Adds a finalizer to the finalizers associated with this scope. If the
     * finalizers associated with this scope have already been run this
     * finalizer will be run immediately.
     *
     * The finalizer returned from this method will remove the original
     * finalizer from the map and run it. When the scope was already closed, the
     * returned finalizer does nothing.
     */
    def add(finalizer: Finalizer)(implicit trace: Trace): UIO[Finalizer]

    /**
     * Adds a finalizer to the finalizers associated with this scope. If the
     * scope is still open, a [[Key]] will be returned. This is an opaque
     * identifier that can be used to activate this finalizer and remove it from
     * the map. If the scope has been closed, the finalizer will be executed
     * immediately (with the [[Exit]] value with which the scope has ended) and
     * no Key will be returned.
     */
    def addIfOpen(finalizer: Finalizer)(implicit trace: Trace): UIO[Option[Key]]

    /**
     * Retrieves the finalizer associated with this key. Returns `None` when the
     * key is not in the map or the scope has been closed.
     */
    def get(key: Key)(implicit trace: Trace): UIO[Option[Finalizer]]

    /**
     * Runs the specified finalizer and removes it from the finalizers
     * associated with this scope. The finalizer, wrapped by the functions given
     * to `updateAll`, receives `exit`. Does nothing when the key is not in the
     * map or the scope has been closed.
     */
    def release(key: Key, exit: Exit[Any, Any])(implicit trace: Trace): UIO[Any]

    /**
     * Runs the finalizers associated with this scope using the specified
     * execution strategy. After this action finishes, any finalizers added to
     * this scope will be run immediately.
     *
     * With `ExecutionStrategy.Sequential` the finalizers run one at a time, in
     * reverse order of insertion, and a finalizer that dies does not stop the
     * ones after it. A second call does nothing.
     */
    def releaseAll(exit: Exit[Any, Any], execStrategy: ExecutionStrategy)(implicit trace: Trace): UIO[Any]

    /**
     * Removes the finalizer associated with this key and returns it.
     */
    def remove(key: Key)(implicit trace: Trace): UIO[Option[Finalizer]]

    /**
     * Replaces the finalizer associated with this key and returns the previous
     * one, or `None` when the key had no finalizer. If the finalizers
     * associated with this scope have already been run, `finalizer` is run
     * immediately with the exit value of the scope and `None` is returned.
     */
    def replace(key: Key, finalizer: Finalizer)(implicit trace: Trace): UIO[Option[Finalizer]]

    /**
     * Updates the finalizers associated with this scope using the specified
     * function. The function is applied when `release` or `releaseAll` runs a
     * finalizer, also to finalizers added after this call.
     */
    def updateAll(f: Finalizer => Finalizer)(implicit trace: Trace): UIO[Unit]
  }

  /**
   * Constructors for [[ReleaseMap]].
   */
  object ReleaseMap {

    /**
     * Construct a [[ReleaseMap]] wrapped in a [[ZManaged]]. When the resulting
     * `ZManaged` is released, the finalizers in the `ReleaseMap` run in
     * parallel: with `ExecutionStrategy.ParallelN(n)` when the parallelism is
     * set to `n` (see `ZManaged.parallelism`), and with
     * `ExecutionStrategy.Parallel` otherwise.
     */
    def makeManagedPar(implicit trace: Trace): ZManaged[Any, Nothing, ReleaseMap] =
      ZManaged.parallelism.flatMap {
        case Some(n) => makeManaged(ExecutionStrategy.ParallelN(n))
        case None    => makeManaged(ExecutionStrategy.Parallel)
      }

    /**
     * Construct a [[ReleaseMap]] wrapped in a [[ZManaged]]. The `ReleaseMap`
     * will be released with the specified [[ExecutionStrategy]] as the release
     * action for the resulting `ZManaged`.
     */
    def makeManaged(executionStrategy: ExecutionStrategy)(implicit
      trace: Trace
    ): ZManaged[Any, Nothing, ReleaseMap] =
      ZManaged.acquireReleaseExitWith(make)((m, e) => m.releaseAll(e, executionStrategy))

    /**
     * Creates a new ReleaseMap.
     */
    def make(implicit trace: Trace): UIO[ReleaseMap] =
      ZIO.succeed(unsafe.make()(Unsafe.unsafe))

    private[zio] object unsafe {

      /**
       * Creates a new ReleaseMap.
       */
      def make()(implicit unsafe: Unsafe) = {
        // The sorting order of the LongMap uses bit ordering (000, 001, ... 111 but with 64 bits). This
        // works out to be `0 ... Long.MaxValue, Long.MinValue, ... -1`. The order of the map is mainly
        // important for the finalization, in which we want to walk it in reverse order. So we insert
        // into the map using keys that will build it in reverse. That way, when we do the final iteration,
        // the finalizers are already in correct order.
        val initialKey: Long = -1L

        def next(l: Long) =
          if (l == 0L) throw new RuntimeException("ReleaseMap wrapped around")
          else if (l == Long.MinValue) Long.MaxValue
          else l - 1

        val ref: Ref[State] =
          Ref.unsafe.make(Running(initialKey, LongMap.empty, identity))

        new ReleaseMap {
          type Key = Long

          def add(finalizer: Finalizer)(implicit trace: Trace): UIO[Finalizer] =
            addIfOpen(finalizer).map {
              case Some(key) => release(key, _)
              case None      => _ => ZIO.unit
            }

          def addIfOpen(finalizer: Finalizer)(implicit trace: Trace): UIO[Option[Key]] =
            ref.modify {
              case Exited(nextKey, exit, update) =>
                finalizer(exit).as(None) -> Exited(next(nextKey), exit, update)
              case Running(nextKey, fins, update) =>
                ZIO.succeed(Some(nextKey)) -> Running(next(nextKey), fins.updated(nextKey, finalizer), update)
            }.flatten

          def get(key: Key)(implicit trace: Trace): UIO[Option[Finalizer]] =
            ref.get.map {
              case Exited(_, _, _)     => None
              case Running(_, fins, _) => fins get key
            }

          def release(key: Key, exit: Exit[Any, Any])(implicit trace: Trace): UIO[Any] =
            ref.modify {
              case s @ Exited(_, _, _) => (ZIO.unit, s)
              case s @ Running(_, fins, update) =>
                (
                  fins.get(key).fold(ZIO.unit: UIO[Any])(fin => update(fin)(exit)),
                  s.copy(finalizers = fins - key)
                )
            }.flatten

          def releaseAll(exit: Exit[Any, Any], execStrategy: ExecutionStrategy)(implicit trace: Trace): UIO[Any] =
            ref.modify {
              case s @ Exited(_, _, _) => (ZIO.unit, s)
              case Running(nextKey, fins, update) =>
                execStrategy match {
                  case ExecutionStrategy.Sequential =>
                    (
                      ZIO
                        .foreach(fins: Iterable[(Long, Finalizer)]) { case (_, fin) =>
                          update(fin).apply(exit).exit
                        }
                        .flatMap(Exit.collectAllDiscard),
                      Exited(nextKey, exit, update)
                    )

                  case ExecutionStrategy.Parallel =>
                    (
                      ZIO
                        .foreachPar(fins: Iterable[(Long, Finalizer)]) { case (_, finalizer) =>
                          update(finalizer)(exit).exit
                        }
                        .flatMap(Exit.collectAllParDiscard),
                      Exited(nextKey, exit, update)
                    )

                  case ExecutionStrategy.ParallelN(n) =>
                    (
                      ZIO
                        .foreachPar(fins: Iterable[(Long, Finalizer)]) { case (_, finalizer) =>
                          update(finalizer)(exit).exit
                        }
                        .flatMap(Exit.collectAllParDiscard)
                        .withParallelism(n),
                      Exited(nextKey, exit, update)
                    )

                }
            }.flatten

          def remove(key: Key)(implicit trace: Trace): UIO[Option[Finalizer]] =
            ref.modify {
              case Exited(nk, exit, update)  => (None, Exited(nk, exit, update))
              case Running(nk, fins, update) => (fins get key, Running(nk, fins - key, update))
            }

          def replace(key: Key, finalizer: Finalizer)(implicit trace: Trace): UIO[Option[Finalizer]] =
            ref.modify {
              case Exited(nk, exit, update) => (finalizer(exit).as(None), Exited(nk, exit, update))
              case Running(nk, fins, update) =>
                (ZIO.succeed(fins get key), Running(nk, fins.updated(key, finalizer), update))
            }.flatten

          def updateAll(f: Finalizer => Finalizer)(implicit trace: Trace): UIO[Unit] =
            ref.update {
              case Exited(key, exit, update)  => Exited(key, exit, update.andThen(f))
              case Running(key, exit, update) => Running(key, exit, update.andThen(f))
            }
        }
      }
    }
  }

  /**
   * Submerges the error case of an `Either` into the `ZManaged`. The inverse
   * operation of `ZManaged.either`.
   */
  def absolve[R, E, A](v: => ZManaged[R, E, Either[E, A]])(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.suspend(v.flatMap(fromEither(_)))

  /**
   * Lifts a `ZIO[R, E, A]` into `ZManaged[R, E, A]` with a release action that
   * does not need access to the resource. The acquire and release actions will
   * be performed uninterruptibly.
   *
   * When the fiber is interrupted during `acquire`, `acquire` runs to the end
   * and `release` then runs. When `acquire` fails, `release` does not run.
   */
  def acquireRelease[R, R1 <: R, E, A](acquire: => ZIO[R, E, A])(
    release: => ZIO[R1, Nothing, Any]
  )(implicit trace: Trace): ZManaged[R1, E, A] =
    acquireReleaseWith(acquire)(_ => release)

  /**
   * Lifts a synchronous effect into `ZManaged[Any, Throwable, A]` with a
   * release action that does not need access to the resource. The acquire and
   * release actions will be performed uninterruptibly. An exception thrown by
   * `acquire` becomes a failure; an exception thrown by `release` becomes a
   * defect.
   */
  def acquireReleaseAttempt[A](acquire: => A)(release: => Any)(implicit
    trace: Trace
  ): ZManaged[Any, Throwable, A] =
    acquireReleaseAttemptWith(acquire)(_ => release)

  /**
   * Lifts a synchronous effect into `ZManaged[Any, Throwable, A]` with a
   * release action. The acquire and release actions will be performed
   * uninterruptibly. An exception thrown by `acquire` becomes a failure; an
   * exception thrown by `release` becomes a defect.
   */
  def acquireReleaseAttemptWith[A](acquire: => A)(release: A => Any)(implicit
    trace: Trace
  ): ZManaged[Any, Throwable, A] =
    acquireReleaseWith(ZIO.attempt(acquire))(a => ZIO.attempt(release(a)).orDie)

  /**
   * Lifts a `ZIO[R, E, A]` into `ZManaged[R, E, A]` with a release action that
   * does not need access to the resource but handles `Exit`. The acquire and
   * release actions will be performed uninterruptibly.
   */
  def acquireReleaseExit[R, R1 <: R, E, A](acquire: => ZIO[R, E, A])(
    release: Exit[Any, Any] => ZIO[R1, Nothing, Any]
  )(implicit trace: Trace): ZManaged[R1, E, A] =
    acquireReleaseExitWith(acquire)((_, exit) => release(exit))

  /**
   * Lifts a `ZIO[R, E, A]` into `ZManaged[R, E, A]` with a release action that
   * handles `Exit`. The acquire and release actions will be performed
   * uninterruptibly.
   *
   * Under `use`, `release` receives the exit of the whole `use` effect: its
   * success value, its failure or defect (including a failure of a later
   * acquire step), or the interruption.
   */
  def acquireReleaseExitWith[R, R1 <: R, E, A](
    acquire: => ZIO[R, E, A]
  )(release: (A, Exit[Any, Any]) => ZIO[R1, Nothing, Any])(implicit trace: Trace): ZManaged[R1, E, A] =
    ZManaged {
      (for {
        r               <- ZIO.environment[R1]
        releaseMap      <- ZManaged.currentReleaseMap.get
        a               <- acquire
        releaseMapEntry <- releaseMap.add(release(a, _).provideEnvironment(r))
      } yield (releaseMapEntry, a)).uninterruptible
    }

  /**
   * Lifts a `ZIO[R, E, A]` into `ZManaged[R, E, A]` with a release action that
   * does not require access to the resource. The acquire action will be
   * performed interruptibly, while release will be performed uninterruptibly.
   * When `acquire` fails or is interrupted, `release` does not run.
   */
  def acquireReleaseInterruptible[R, R1 <: R, E, A](
    acquire: => ZIO[R, E, A]
  )(release: => ZIO[R1, Nothing, Any])(implicit trace: Trace): ZManaged[R1, E, A] =
    acquireReleaseInterruptibleWith(acquire)(_ => release)

  /**
   * Lifts a `ZIO[R, E, A]` into `ZManaged[R, E, A]` with a release action. The
   * acquire action will be performed interruptibly, while release will be
   * performed uninterruptibly. When `acquire` fails or is interrupted,
   * `release` does not run.
   */
  def acquireReleaseInterruptibleWith[R, R1 <: R, E, A](
    acquire: => ZIO[R, E, A]
  )(release: A => URIO[R1, Any])(implicit trace: Trace): ZManaged[R1, E, A] =
    ZManaged.fromZIO(acquire).onExitFirst(_.foreach(release))

  /**
   * Lifts a synchronous effect that does not throw exceptions into a
   * `ZManaged[Any, Nothing, A]` with a release action that does not need access
   * to the resource. The acquire and release actions will be performed
   * uninterruptibly.
   */
  def acquireReleaseSucceed[A](acquire: => A)(release: => Any)(implicit
    trace: Trace
  ): ZManaged[Any, Nothing, A] =
    acquireReleaseSucceedWith(acquire)(_ => release)

  /**
   * Lifts a synchronous effect that does not throw exceptions into a
   * `ZManaged[Any, Nothing, A]` with a release action. The acquire and release
   * actions will be performed uninterruptibly.
   */
  def acquireReleaseSucceedWith[A](acquire: => A)(release: A => Any)(implicit
    trace: Trace
  ): ZManaged[Any, Nothing, A] =
    acquireReleaseWith(ZIO.succeed(acquire))(a => ZIO.succeed(release(a)))

  /**
   * Lifts a `ZIO[R, E, A]` into `ZManaged[R, E, A]` with a release action. The
   * acquire and release actions will be performed uninterruptibly.
   *
   * {{{
   * import zio._
   *
   * final class Counter(val id: Int) {
   *   def increment(n: Int): Int = id + n
   * }
   *
   * def open(log: Ref[List[String]], id: Int): ZManaged[Any, Nothing, Counter] =
   *   ZManaged.acquireReleaseWith(log.update(s"open \$id" :: _).as(new Counter(id)))(c =>
   *     log.update(s"close \${c.id}" :: _)
   *   )
   *
   * val program: UIO[(Int, List[String])] =
   *   for {
   *     log    <- Ref.make(List.empty[String])
   *     result <- open(log, 7).use(c => ZIO.succeed(c.increment(3)))
   *     events <- log.get.map(_.reverse)
   *   } yield (result, events)
   * // (10,List(open 7, close 7))
   * }}}
   */
  def acquireReleaseWith[R, R1 <: R, E, A](acquire: => ZIO[R, E, A])(
    release: A => ZIO[R1, Nothing, Any]
  )(implicit trace: Trace): ZManaged[R1, E, A] =
    acquireReleaseExitWith(acquire)((a, _) => release(a))

  /**
   * Creates new [[ZManaged]] from a ZIO value that uses a [[ReleaseMap]] and
   * returns a resource and a finalizer.
   *
   * The correct usage of this constructor consists of:
   *   - Properly registering a finalizer in the ReleaseMap as part of the ZIO
   *     value;
   *   - Managing interruption safety - take care to use [[ZIO.uninterruptible]]
   *     or [[ZIO.uninterruptibleMask]] to verify that the finalizer is
   *     registered in the ReleaseMap after acquiring the value;
   *   - Returning the finalizer returned from [[ReleaseMap#add]]. This is
   *     important to prevent double-finalization.
   */
  def apply[R, E, A](run0: ZIO[R, E, (Finalizer, A)]): ZManaged[R, E, A] =
    new ZManaged[R, E, A] {
      def zio = run0
    }

  /**
   * Lifts a synchronous side-effect into a `ZManaged[R, Throwable, A]`,
   * translating any thrown exceptions into typed failed effects.
   */
  def attempt[A](r: => A)(implicit trace: Trace): ZManaged[Any, Throwable, A] =
    ZManaged.fromZIO(ZIO.attempt(r))

  /**
   * Evaluate each effect in the structure from left to right, collecting the
   * successful values and discarding the empty cases (an element whose effect
   * fails with `None`). A failure with `Some(e)` fails the result with `e`. For
   * a parallel version, see `collectPar`.
   */
  def collect[R, E, A, B, Collection[+Element] <: Iterable[Element]](in: Collection[A])(
    f: A => ZManaged[R, Option[E], B]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]], trace: Trace): ZManaged[R, E, Collection[B]] =
    foreach[R, E, A, Option[B], Iterable](in)(a => f(a).unsome).map(_.flatten).map(bf.fromSpecific(in))

  /**
   * Evaluate each effect in the structure from left to right, and collect the
   * results. For a parallel version, see `collectAllPar`.
   */
  def collectAll[R, E, A, Collection[+Element] <: Iterable[Element]](
    ms: Collection[ZManaged[R, E, A]]
  )(implicit
    bf: BuildFrom[Collection[ZManaged[R, E, A]], A, Collection[A]],
    trace: Trace
  ): ZManaged[R, E, Collection[A]] =
    foreach(ms)(ZIO.identityFn)

  /**
   * Evaluate each effect in the structure from left to right, and discard the
   * results. For a parallel version, see `collectAllParDiscard`.
   */
  def collectAllDiscard[R, E, A](ms: => Iterable[ZManaged[R, E, A]])(implicit
    trace: Trace
  ): ZManaged[R, E, Unit] =
    foreachDiscard(ms)(ZIO.identityFn)

  /**
   * Evaluate each effect in the structure in parallel, and collect the results.
   * The release actions also run in parallel. For a sequential version, see
   * `collectAll`.
   */
  def collectAllPar[R, E, A, Collection[+Element] <: Iterable[Element]](
    as: Collection[ZManaged[R, E, A]]
  )(implicit
    bf: BuildFrom[Collection[ZManaged[R, E, A]], A, Collection[A]],
    trace: Trace
  ): ZManaged[R, E, Collection[A]] =
    foreachPar(as)(ZIO.identityFn)

  /**
   * Evaluate each effect in the structure in parallel, and discard the results.
   * The release actions also run in parallel. For a sequential version, see
   * `collectAllDiscard`.
   */
  def collectAllParDiscard[R, E, A](as: => Iterable[ZManaged[R, E, A]])(implicit
    trace: Trace
  ): ZManaged[R, E, Unit] =
    foreachParDiscard(as)(ZIO.identityFn)

  /**
   * Collects the first element of the `Iterable[A]` for which the effectual
   * function `f` returns `Some`.
   */
  def collectFirst[R, E, A, B](
    as: => Iterable[A]
  )(f: A => ZManaged[R, E, Option[B]])(implicit trace: Trace): ZManaged[R, E, Option[B]] =
    succeed(as.iterator).flatMap { iterator =>
      def loop: ZManaged[R, E, Option[B]] =
        if (iterator.hasNext) f(iterator.next()).flatMap(_.fold(loop)(some(_)))
        else none
      loop
    }

  /**
   * Evaluate each effect in the structure in parallel, collecting the
   * successful values and discarding the empty cases (an element whose effect
   * fails with `None`). A failure with `Some(e)` fails the result with `e`.
   */
  def collectPar[R, E, A, B, Collection[+Element] <: Iterable[Element]](in: Collection[A])(
    f: A => ZManaged[R, Option[E], B]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]], trace: Trace): ZManaged[R, E, Collection[B]] =
    foreachPar[R, E, A, Option[B], Iterable](in)(a => f(a).unsome).map(_.flatten).map(bf.fromSpecific(in))

  /**
   * Similar to `Either.cond`: evaluates the predicate, and succeeds with
   * `result` when it is `true` or fails with `error` when it is `false`.
   */
  def cond[E, A](predicate: => Boolean, result: => A, error: => E)(implicit trace: Trace): Managed[E, A] =
    ZManaged.suspend(if (predicate) succeed(result) else fail(error))

  /**
   * Returns an effect that dies with the specified `Throwable`. This method can
   * be used for terminating a fiber because a defect has been detected in the
   * code.
   */
  def die(t: => Throwable)(implicit trace: Trace): ZManaged[Any, Nothing, Nothing] =
    failCause(Cause.die(t))

  /**
   * Returns an effect that dies with a [[java.lang.RuntimeException]] having
   * the specified text message. This method can be used for terminating a fiber
   * because a defect has been detected in the code.
   */
  def dieMessage(message: => String)(implicit trace: Trace): ZManaged[Any, Nothing, Nothing] =
    die(new RuntimeException(message))

  /**
   * Returns an effect from a lazily evaluated [[zio.Exit]] value.
   */
  def done[E, A](r: => Exit[E, A])(implicit trace: Trace): ZManaged[Any, E, A] =
    ZManaged.fromZIO(r)

  /**
   * Accesses the whole environment of the effect.
   */
  def environment[R](implicit trace: Trace): ZManaged[R, Nothing, ZEnvironment[R]] =
    ZManaged.fromZIO(ZIO.environment)

  /**
   * Create a managed that accesses the environment and applies a pure function
   * to it.
   */
  def environmentWith[R]: EnvironmentWithPartiallyApplied[R] =
    new EnvironmentWithPartiallyApplied

  /**
   * Create a managed that accesses the environment and continues with the
   * `ZManaged` that a function builds from it.
   */
  def environmentWithManaged[R]: EnvironmentWithManagedPartiallyApplied[R] =
    new EnvironmentWithManagedPartiallyApplied

  /**
   * Create a managed that accesses the environment and runs the `ZIO` effect
   * that a function builds from it.
   */
  def environmentWithZIO[R]: EnvironmentWithZIOPartiallyApplied[R] =
    new EnvironmentWithZIOPartiallyApplied

  /**
   * Determines whether any element of the `Iterable[A]` satisfies the effectual
   * predicate `f`.
   */
  def exists[R, E, A](
    as: => Iterable[A]
  )(f: A => ZManaged[R, E, Boolean])(implicit trace: Trace): ZManaged[R, E, Boolean] =
    succeed(as.iterator).flatMap { iterator =>
      def loop: ZManaged[R, E, Boolean] =
        if (iterator.hasNext) f(iterator.next()).flatMap(b => if (b) succeed(b) else loop)
        else succeed(false)
      loop
    }

  /**
   * Returns an effect that models failure with the specified error. The moral
   * equivalent of `throw` for pure code.
   */
  def fail[E](error: => E)(implicit trace: Trace): ZManaged[Any, E, Nothing] =
    failCause(Cause.fail(error))

  /**
   * Returns an effect that models failure with the specified `Cause`.
   */
  def failCause[E](cause: => Cause[E])(implicit trace: Trace): ZManaged[Any, E, Nothing] =
    ZManaged.fromZIO(ZIO.failCause(cause))

  /**
   * Returns an effect that succeeds with the `FiberId` of the caller.
   */
  def fiberId(implicit trace: Trace): ZManaged[Any, Nothing, FiberId] =
    ZManaged.fromZIO(ZIO.fiberId)

  /**
   * Creates an effect that only executes the provided finalizer as its release
   * action.
   */
  def finalizer[R](f: => URIO[R, Any])(implicit trace: Trace): ZManaged[R, Nothing, Unit] =
    finalizerExit(_ => f)

  /**
   * Creates an effect that only executes the provided function as its release
   * action.
   */
  def finalizerExit[R](f: Exit[Any, Any] => URIO[R, Any])(implicit trace: Trace): ZManaged[R, Nothing, Unit] =
    acquireReleaseExitWith(ZIO.unit)((_, e) => f(e))

  /**
   * Creates an effect that executes a finalizer stored in a [[Ref]]. The `Ref`
   * is yielded as the result of the effect, allowing for control flows that
   * require mutating finalizers.
   */
  def finalizerRef[R](initial: => Finalizer)(implicit trace: Trace): ZManaged[R, Nothing, Ref[Finalizer]] =
    ZManaged.acquireReleaseExitWith(Ref.make(initial))((ref, exit) => ref.get.flatMap(_.apply(exit)))

  /**
   * Returns a managed resource that attempts to acquire the first managed
   * resource and in case of failure, attempts to acquire each of the specified
   * managed resources in order until one of them is successfully acquired,
   * ensuring that the acquired resource is properly released after being used.
   */
  def firstSuccessOf[R, E, A](
    first: => ZManaged[R, E, A],
    rest: => Iterable[ZManaged[R, E, A]]
  )(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.suspend(rest.foldLeft(first)(_ <> _))

  /**
   * Returns an effect that performs the outer effect first, followed by the
   * inner effect, yielding the value of the inner effect.
   *
   * This method can be used to "flatten" nested effects.
   */
  def flatten[R, E, A](zManaged: => ZManaged[R, E, ZManaged[R, E, A]])(implicit
    trace: Trace
  ): ZManaged[R, E, A] =
    ZManaged.suspend(zManaged.flatMap(scala.Predef.identity))

  /**
   * Returns an effect that performs the outer effect first, followed by the
   * inner effect, yielding the value of the inner effect.
   *
   * This method can be used to "flatten" nested effects.
   */
  def flattenZIO[R, E, A](zManaged: => ZManaged[R, E, ZIO[R, E, A]])(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.suspend(zManaged.mapZIO(scala.Predef.identity))

  /**
   * Folds an Iterable[A] using an effectual function f, working sequentially
   * from left to right.
   */
  def foldLeft[R, E, S, A](
    in: => Iterable[A]
  )(zero: => S)(f: (S, A) => ZManaged[R, E, S])(implicit trace: Trace): ZManaged[R, E, S] =
    ZManaged.suspend(in.foldLeft[ZManaged[R, E, S]](ZManaged.succeed(zero))((acc, el) => acc.flatMap(f(_, el))))

  /**
   * Determines whether all elements of the `Iterable[A]` satisfy the effectual
   * predicate `f`.
   */
  def forall[R, E, A](
    as: => Iterable[A]
  )(f: A => ZManaged[R, E, Boolean])(implicit trace: Trace): ZManaged[R, E, Boolean] =
    succeed(as.iterator).flatMap { iterator =>
      def loop: ZManaged[R, E, Boolean] =
        if (iterator.hasNext) f(iterator.next()).flatMap(b => if (b) loop else succeed(b))
        else succeed(true)
      loop
    }

  /**
   * Applies the function `f` to each element of the `Collection[A]` and returns
   * the results in a new `Collection[B]`. The resources are acquired one at a
   * time from left to right and released in reverse order.
   *
   * For a parallel version of this method, see `foreachPar`. If you do not need
   * the results, see `foreachDiscard` for a more efficient implementation.
   */
  def foreach[R, E, A1, A2, Collection[+Element] <: Iterable[Element]](in: Collection[A1])(
    f: A1 => ZManaged[R, E, A2]
  )(implicit bf: BuildFrom[Collection[A1], A2, Collection[A2]], trace: Trace): ZManaged[R, E, Collection[A2]] =
    ZManaged(ZIO.foreach(in.toList)(f(_).zio).map { result =>
      val (fins, as) = result.unzip
      (e => ZIO.foreach(fins.reverse)(_.apply(e)), bf.fromSpecific(in)(as))
    })

  /**
   * Applies the function `f` if the argument is non-empty and returns the
   * results in a new `Option[A2]`.
   */
  final def foreach[R, E, A1, A2](in: Option[A1])(f: A1 => ZManaged[R, E, A2])(implicit
    trace: Trace
  ): ZManaged[R, E, Option[A2]] =
    in.fold[ZManaged[R, E, Option[A2]]](succeed(None))(f(_).map(Some(_)))

  /**
   * Applies the function `f` to each element of the `Collection[A]` and returns
   * the result in a new `Collection[B]` using the specified execution strategy.
   */
  final def foreachExec[R, E, A, B, Collection[+Element] <: Iterable[Element]](as: Collection[A])(
    exec: => ExecutionStrategy
  )(
    f: A => ZManaged[R, E, B]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]], trace: Trace): ZManaged[R, E, Collection[B]] =
    ZManaged.suspend {
      exec match {
        case ExecutionStrategy.Parallel     => ZManaged.foreachPar(as)(f).withParallelismUnbounded
        case ExecutionStrategy.ParallelN(n) => ZManaged.foreachPar(as)(f).withParallelism(n)
        case ExecutionStrategy.Sequential   => ZManaged.foreach(as)(f)
      }
    }

  /**
   * Applies the function `f` to each element of the `Collection[A]` in
   * parallel, and returns the results in a new `Collection[B]`, in the order of
   * the input.
   *
   * The resources of the elements are acquired in parallel and released in
   * parallel. The finalizers of one element run in reverse order of
   * acquisition. When the managed value of one element fails, the resources
   * already acquired for the other elements are released. The number of
   * elements acquired at the same time follows `withParallelism`.
   *
   * {{{
   * import zio._
   *
   * val program: UIO[List[Int]] =
   *   ZManaged
   *     .foreachPar(List(1, 2, 3)) { n =>
   *       ZManaged.acquireRelease(ZIO.succeed(n * 10))(ZIO.unit)
   *     }
   *     .useNow
   * // List(10, 20, 30)
   * }}}
   *
   * For a sequential version of this method, see `foreach`.
   */
  def foreachPar[R, E, A1, A2, Collection[+Element] <: Iterable[Element]](as: Collection[A1])(
    f: A1 => ZManaged[R, E, A2]
  )(implicit bf: BuildFrom[Collection[A1], A2, Collection[A2]], trace: Trace): ZManaged[R, E, Collection[A2]] =
    ReleaseMap.makeManagedPar.mapZIO { parallelReleaseMap =>
      val makeInnerMap =
        ZManaged.currentReleaseMap.locally(parallelReleaseMap)(
          ReleaseMap.makeManaged(ExecutionStrategy.Sequential).zio.map(_._2)
        )

      ZIO
        .foreachPar(as)(a =>
          makeInnerMap.flatMap(innerMap => ZManaged.currentReleaseMap.locally(innerMap)(f(a).zio.map(_._2)))
        )
        .map(bf.fromSpecific(as))
    }

  /**
   * Applies the function `f` to each element of the `Iterable[A]` and runs
   * produced effects sequentially. The resources are released in reverse order
   * of acquisition.
   *
   * Equivalent to `foreach(as)(f).unit`, but without the cost of building the
   * list of results.
   */
  def foreachDiscard[R, E, A](
    as: => Iterable[A]
  )(f: A => ZManaged[R, E, Any])(implicit trace: Trace): ZManaged[R, E, Unit] =
    ZManaged {
      ZIO.suspendSucceed {
        ZIO.foreach(as)(f(_).zio).map { result =>
          val (fins, _) = result.unzip
          (e => ZIO.foreach(fins.toList.reverse)(_.apply(e)), ())
        }
      }
    }

  /**
   * Applies the function `f` to each element of the `Iterable[A]` and runs
   * produced effects in parallel, discarding the results. The release actions
   * also run in parallel.
   *
   * For a sequential version of this method, see `foreachDiscard`.
   */
  def foreachParDiscard[R, E, A](
    as: => Iterable[A]
  )(f: A => ZManaged[R, E, Any])(implicit trace: Trace): ZManaged[R, E, Unit] =
    ReleaseMap.makeManagedPar.mapZIO { parallelReleaseMap =>
      val makeInnerMap =
        ZManaged.currentReleaseMap.locally(parallelReleaseMap)(
          ReleaseMap.makeManaged(ExecutionStrategy.Sequential).zio.map(_._2)
        )

      ZIO.foreachParDiscard(as)(a =>
        makeInnerMap.flatMap(innerMap => ZManaged.currentReleaseMap.locally(innerMap)(f(a).zio.map(_._2)))
      )
    }

  /**
   * Constructs a `ZManaged` value of the appropriate type for the specified
   * input.
   */
  def from[Input](
    input: => Input
  )(implicit constructor: ZManagedConstructor[Input], trace: Trace): constructor.Out =
    constructor.make(input)

  /**
   * Creates a [[ZManaged]] from an `AutoCloseable` resource. The resource's
   * `close` method will be used as the release action. When `fa` succeeds with
   * `null`, no `close` is called. An exception thrown by `close` becomes a
   * defect.
   *
   * {{{
   * import zio._
   *
   * final class Session(val user: String) extends AutoCloseable {
   *   var closed = false
   *   def close(): Unit = closed = true
   * }
   *
   * val program: UIO[(String, Boolean)] =
   *   for {
   *     session <- ZIO.succeed(new Session("ada"))
   *     user    <- ZManaged.fromAutoCloseable(ZIO.succeed(session)).use(s => ZIO.succeed(s.user))
   *   } yield (user, session.closed)
   * // (ada,true)
   * }}}
   */
  def fromAutoCloseable[R, E, A <: AutoCloseable](fa: => ZIO[R, E, A])(implicit
    trace: Trace
  ): ZManaged[R, E, A] =
    acquireReleaseWith(fa)(a => if (a eq null) ZIO.unit else ZIO.succeed(a.close()))

  /**
   * Lifts an `Either` into a `ZManaged` value.
   */
  def fromEither[E, A](v: => Either[E, A])(implicit trace: Trace): ZManaged[Any, E, A] =
    succeed(v).flatMap(_.fold(fail(_), succeed(_)))

  /**
   * Lifts an `Option` into a `ZManaged` but preserves the error as an option in
   * the error channel, making it easier to compose in some scenarios.
   */
  def fromOption[A](v: => Option[A])(implicit trace: Trace): ZManaged[Any, Option[Nothing], A] =
    succeed(v).flatMap(_.fold[Managed[Option[Nothing], A]](fail(None))(succeed(_)))

  /**
   * Lifts a pure `Reservation[R, E, A]` into `ZManaged[R, E, A]`. The
   * acquisition step is performed interruptibly.
   *
   * The release step is registered before the acquisition step runs, so it also
   * runs when the acquisition step fails or is interrupted.
   *
   * {{{
   * import zio._
   *
   * val program: UIO[List[String]] =
   *   for {
   *     log <- Ref.make(List.empty[String])
   *     reservation = Reservation(
   *                     ZIO.fail("no slot"),
   *                     (exit: Exit[Any, Any]) => log.update(s"released, success = \${exit.isSuccess}" :: _)
   *                   )
   *     _      <- ZManaged.fromReservation(reservation).useDiscard(ZIO.unit).ignore
   *     events <- log.get
   *   } yield events
   * // List(released, success = false)
   * }}}
   */
  def fromReservation[R, E, A](reservation: => Reservation[R, E, A])(implicit trace: Trace): ZManaged[R, E, A] =
    fromReservationZIO(ZIO.succeed(reservation))

  /**
   * Creates a ZManaged from a [[Reservation]] produced by an effect. Evaluating
   * the effect that produces the reservation will be performed
   * *uninterruptibly*, while the acquisition step of the reservation will be
   * performed *interruptibly*. The release step will be performed
   * uninterruptibly as usual. The release step is registered before the
   * acquisition step runs, so it also runs when the acquisition step fails or
   * is interrupted.
   *
   * This two-phase acquisition allows for resource acquisition flows that can
   * be safely interrupted and released.
   */
  def fromReservationZIO[R, E, A](
    reservation: => ZIO[R, E, Reservation[R, E, A]]
  )(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged {
      ZIO.uninterruptibleMask { restore =>
        for {
          r          <- ZIO.environment[R]
          releaseMap <- ZManaged.currentReleaseMap.get
          reserved   <- reservation
          releaseKey <- releaseMap.addIfOpen(reserved.release(_).provideEnvironment(r))
          finalizerAndA <- releaseKey match {
                             case Some(key) =>
                               restore(reserved.acquire)
                                 .map((releaseMap.release(key, (_: Exit[Any, Any])), _))
                             case None => ZIO.interrupt
                           }
        } yield finalizerAndA
      }
    }

  /**
   * Lifts a `Try` into a `ZManaged`.
   */
  def fromTry[A](value: => scala.util.Try[A])(implicit trace: Trace): TaskManaged[A] =
    attempt(value).flatMap {
      case scala.util.Success(v) => succeed(v)
      case scala.util.Failure(t) => fail(t)
    }

  /**
   * Lifts a ZIO[R, E, A] into ZManaged[R, E, A] with no release action. The
   * effect will be performed interruptibly.
   */
  def fromZIO[R, E, A](fa: => ZIO[R, E, A])(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged(
      ZIO.uninterruptibleMask(restore => restore(fa).map((Finalizer.noop, _)))
    )

  /**
   * Lifts a ZIO[R, E, A] into ZManaged[R, E, A] with no release action. The
   * effect will be performed uninterruptibly. You usually want the
   * [[ZManaged.fromZIO]] variant.
   */
  def fromZIOUninterruptible[R, E, A](fa: => ZIO[R, E, A])(implicit trace: Trace): ZManaged[R, E, A] =
    fromZIO(fa.uninterruptible)

  /**
   * Runs `onTrue` if the result of `b` is `true` and `onFalse` otherwise.
   */
  def ifManaged[R, E](b: => ZManaged[R, E, Boolean]): ZManaged.IfManaged[R, E] =
    new ZManaged.IfManaged(() => b)

  /**
   * Returns an effect that is interrupted as if by the fiber calling this
   * method.
   */
  def interrupt(implicit trace: Trace): ZManaged[Any, Nothing, Nothing] =
    ZManaged.fromZIO(ZIO.fiberId).flatMap(id => failCause(Cause.interrupt(id)))

  /**
   * Returns an effect that is interrupted as if by the specified fiber.
   */
  def interruptAs(fiberId: => FiberId)(implicit trace: Trace): ZManaged[Any, Nothing, Nothing] =
    failCause(Cause.interrupt(fiberId))

  /**
   * Iterates with the specified effectual function. The moral equivalent of:
   *
   * {{{
   * import zio._
   *
   * val initial                = 1
   * def cont(s: Int): Boolean  = s < 100
   * def body(s: Int): Int      = s * 3
   *
   * var s = initial
   *
   * while (cont(s)) {
   *   s = body(s)
   * }
   *
   * val result = s // 243
   *
   * val managed: UManaged[Int] =
   *   ZManaged.iterate(initial)(cont)(s => ZManaged.succeed(body(s))) // 243
   * }}}
   */
  def iterate[R, E, S](
    initial: => S
  )(cont: S => Boolean)(body: S => ZManaged[R, E, S])(implicit trace: Trace): ZManaged[R, E, S] =
    ZManaged.suspend {
      def loop(initial: S): ZManaged[R, E, S] =
        if (cont(initial)) body(initial).flatMap(loop)
        else ZManaged.succeed(initial)

      loop(initial)
    }

  /**
   * Returns a managed effect that describes shifting to the specified executor
   * as the `acquire` action and shifting back to the original executor as the
   * `release` action.
   */
  def lock(executor: => Executor)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    onExecutor(executor)

  /**
   * Logs the specified message at the current log level.
   */
  def log(message: => String)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.fromZIO(ZIO.log(message))

  /**
   * Annotates each log in managed effects composed after this.
   */
  def logAnnotate(key: => String, value: => String)(implicit
    trace: Trace
  ): ZManaged[Any, Nothing, Unit] =
    ZManaged.scoped {
      FiberRef.currentLogAnnotations.get.flatMap { annotations =>
        FiberRef.currentLogAnnotations.locallyScoped(annotations.updated(key, value))
      }
    }

  /**
   * Retrieves current log annotations.
   */
  def logAnnotations(implicit trace: Trace): ZManaged[Any, Nothing, Map[String, String]] =
    ZManaged.fromZIO(FiberRef.currentLogAnnotations.get)

  /**
   * Logs the specified message at the debug log level.
   */
  def logDebug(message: => String)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.fromZIO(ZIO.logDebug(message))

  /**
   * Logs the specified message at the error log level.
   */
  def logError(message: => String)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.fromZIO(ZIO.logError(message))

  /**
   * Logs the specified cause as an error.
   */
  def logErrorCause(cause: => Cause[Any])(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.fromZIO(ZIO.logErrorCause(cause))

  /**
   * Logs the specified message at the fatal log level.
   */
  def logFatal(message: => String)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.fromZIO(ZIO.logFatal(message))

  /**
   * Logs the specified message at the informational log level.
   */
  def logInfo(message: => String)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.fromZIO(ZIO.logInfo(message))

  /**
   * Sets the log level for managed effects composed after this.
   */
  def logLevel(level: LogLevel)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.scoped(FiberRef.currentLogLevel.locallyScoped(level))

  /**
   * Adjusts the label for the logging span for managed effects composed after
   * this.
   */
  def logSpan(label: => String)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.scoped {
      FiberRef.currentLogSpan.get.flatMap { stack =>
        val instant = java.lang.System.currentTimeMillis()
        val logSpan = LogSpan(label, instant)

        FiberRef.currentLogSpan.locallyScoped(logSpan :: stack)
      }
    }

  /**
   * Logs the specified message at the trace log level.
   */
  def logTrace(message: => String)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.fromZIO(ZIO.logTrace(message))

  /**
   * Logs the specified message at the warning log level.
   */
  def logWarning(message: => String)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.fromZIO(ZIO.logWarning(message))

  /**
   * Loops with the specified effectual function, collecting the results into a
   * list. The moral equivalent of:
   *
   * {{{
   * import zio._
   *
   * val initial               = 1
   * def cont(s: Int): Boolean = s <= 4
   * def inc(s: Int): Int      = s + 1
   * def body(s: Int): Int     = s * s
   *
   * var s  = initial
   * var as = List.empty[Int]
   *
   * while (cont(s)) {
   *   as = body(s) :: as
   *   s  = inc(s)
   * }
   *
   * val result = as.reverse // List(1, 4, 9, 16)
   *
   * val managed: UManaged[List[Int]] =
   *   ZManaged.loop(initial)(cont, inc)(s => ZManaged.succeed(body(s))) // List(1, 4, 9, 16)
   * }}}
   */
  def loop[R, E, A, S](
    initial: => S
  )(cont: S => Boolean, inc: S => S)(
    body: S => ZManaged[R, E, A]
  )(implicit trace: Trace): ZManaged[R, E, List[A]] =
    ZManaged.suspend {

      def loop(initial: S): ZManaged[R, E, List[A]] =
        if (cont(initial))
          body(initial).flatMap(a => loop(inc(initial)).map(as => a :: as))
        else
          ZManaged.succeed(List.empty[A])

      loop(initial)
    }

  /**
   * Loops with the specified effectual function purely for its effects. The
   * moral equivalent of:
   *
   * {{{
   * import zio._
   *
   * val initial                = 0
   * def cont(s: Int): Boolean  = s < 3
   * def inc(s: Int): Int       = s + 1
   * def body(s: Int): Unit     = println(s"step \$s")
   *
   * var s = initial
   *
   * while (cont(s)) {
   *   body(s)
   *   s = inc(s)
   * }
   *
   * val managed: UManaged[Unit] =
   *   ZManaged.loopDiscard(initial)(cont, inc)(s => ZManaged.succeed(body(s)))
   * }}}
   */
  def loopDiscard[R, E, S](
    initial: => S
  )(cont: S => Boolean, inc: S => S)(
    body: S => ZManaged[R, E, Any]
  )(implicit trace: Trace): ZManaged[R, E, Unit] =
    ZManaged.suspend {

      def loop(initial: S): ZManaged[R, E, Unit] =
        if (cont(initial)) body(initial) *> loop(inc(initial))
        else ZManaged.unit

      loop(initial)
    }

  /**
   * Returns a memoized version of the specified resourceful function in the
   * context of a managed scope. Each time the memoized function is evaluated a
   * new resource will be acquired, if the function has not already been
   * evaluated with that input, or else the previously acquired resource will be
   * returned. All resources acquired by evaluating the function will be
   * released at the end of the scope.
   */
  def memoize[R, E, A, B](
    f: A => ZManaged[R, E, B]
  )(implicit trace: Trace): ZManaged[Any, Nothing, A => ZIO[R, E, B]] =
    for {
      fiberId <- ZIO.fiberId.toManaged
      ref     <- Ref.make[Map[A, Promise[E, B]]](Map.empty).toManaged
      scope   <- ZManaged.scope
    } yield a =>
      ref.modify { map =>
        map.get(a) match {
          case Some(promise) => (promise.await, map)
          case None =>
            val promise = Promise.unsafe.make[E, B](fiberId)(Unsafe.unsafe)
            (scope(f(a)).map(_._2).intoPromise(promise) *> promise.await, map.updated(a, promise))
        }
      }.flatten

  /**
   * Merges an `Iterable[ZManaged]` to a single `ZManaged`, working
   * sequentially.
   */
  def mergeAll[R, E, A, B](in: => Iterable[ZManaged[R, E, A]])(zero: => B)(f: (B, A) => B)(implicit
    trace: Trace
  ): ZManaged[R, E, B] =
    ZManaged.suspend(in.foldLeft[ZManaged[R, E, B]](succeed(zero))(_.zipWith(_)(f)))

  /**
   * Merges an `Iterable[ZManaged]` to a single `ZManaged`, working in parallel.
   * The release actions also run in parallel.
   *
   * Due to the parallel nature of this combinator, `f` must be both:
   *   - commutative: `f(a, b) == f(b, a)`
   *   - associative: `f(a, f(b, c)) == f(f(a, b), c)`
   */
  def mergeAllPar[R, E, A, B](
    in: => Iterable[ZManaged[R, E, A]]
  )(zero: => B)(f: (B, A) => B)(implicit trace: Trace): ZManaged[R, E, B] =
    ReleaseMap.makeManagedPar.mapZIO { parallelReleaseMap =>
      ZManaged.currentReleaseMap.locally(parallelReleaseMap)(ZIO.mergeAllPar(in.map(_.zio.map(_._2)))(zero)(f))
    }

  /**
   * Returns a `ZManaged` that never acquires a resource.
   */
  def never(implicit trace: Trace): ZManaged[Any, Nothing, Nothing] =
    ZManaged.fromZIO(ZIO.never)

  /**
   * Returns a `ZManaged` with the empty value.
   */
  val none: Managed[Nothing, Option[Nothing]] =
    succeed(None)(Trace.empty)

  /**
   * Returns a managed effect that describes shifting to the specified executor
   * as the `acquire` action and shifting back to the original executor as the
   * `release` action.
   */
  def onExecutor(executor: => Executor)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.acquireReleaseWith {
      ZIO.descriptorWith { descriptor =>
        if (descriptor.isLocked) ZIO.shift(executor).as(Some(descriptor.executor))
        else ZIO.shift(executor).as(None)
      }
    } {
      case Some(executor) => ZIO.shift(executor)
      case None           => ZIO.unshift
    }.unit

  /**
   * Retrieves the maximum number of fibers for parallel operators or `None` if
   * it is unbounded.
   */
  def parallelism(implicit trace: Trace): ZManaged[Any, Nothing, Option[Int]] =
    ZManaged.fromZIO(FiberRef.parallelism.get)

  /**
   * A scope in which resources can be safely preallocated. Passing a
   * [[ZManaged]] to the `apply` method will create (inside an effect) a managed
   * resource which is already acquired and cannot fail.
   */
  sealed abstract class PreallocationScope {

    /**
     * Acquires `managed` now and returns a `Managed` that holds the acquired
     * resource. Using the returned `Managed` does not acquire again; releasing
     * it runs the release actions of `managed`. Release actions that have not
     * run by then run with the release map in which the `preallocationScope`
     * value was acquired (for `use`, when the `use` effect ends). A failure of
     * the acquisition fails the returned effect.
     */
    def apply[R, E, A](managed: => ZManaged[R, E, A]): ZIO[R, E, Managed[Nothing, A]]
  }

  /**
   * Creates a scope in which resources can be safely preallocated.
   */
  def preallocationScope(implicit trace: Trace): Managed[Nothing, PreallocationScope] =
    scope.map { allocate =>
      new PreallocationScope {
        def apply[R, E, A](managed: => ZManaged[R, E, A]) =
          allocate(managed).map { case (release, res) =>
            ZManaged.acquireReleaseExitWith(ZIO.succeed(res))((_, exit) => release(exit))
          }
      }
    }

  /**
   * Provides the `ROut` part of the environment of `managed` with `layer`. The
   * result needs the input `RIn` of the layer and the rest `RIn2` of the
   * environment of `managed`. The layer is built as part of the acquisition.
   */
  def provideLayer[RIn, E, ROut, RIn2, ROut2](layer: ZLayer[RIn, E, ROut])(
    managed: ZManaged[ROut with RIn2, E, ROut2]
  )(implicit
    ev: EnvironmentTag[RIn2],
    tag: EnvironmentTag[ROut],
    trace: Trace
  ): ZManaged[RIn with RIn2, E, ROut2] =
    managed.provideSomeLayer[RIn with RIn2](ZLayer.environment[RIn2] <*> layer)

  /**
   * Reduces `a` and the `Iterable[ZManaged]` to a single `ZManaged` with `f`,
   * working sequentially from left to right.
   */
  def reduceAll[R, E, A](a: => ZManaged[R, E, A], as: => Iterable[ZManaged[R, E, A]])(
    f: (A, A) => A
  )(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.suspend(as.foldLeft[ZManaged[R, E, A]](a)(_.zipWith(_)(f)))

  /**
   * Reduces `a` and the `Iterable[ZManaged]` to a single `ZManaged` with `f`,
   * working in parallel. The release actions also run in parallel.
   */
  def reduceAllPar[R, E, A](a: => ZManaged[R, E, A], as: => Iterable[ZManaged[R, E, A]])(
    f: (A, A) => A
  )(implicit trace: Trace): ZManaged[R, E, A] =
    ReleaseMap.makeManagedPar.mapZIO { parallelReleaseMap =>
      ZManaged.currentReleaseMap.locally(parallelReleaseMap)(
        ZIO.reduceAllPar(a.zio.map(_._2), as.map(_.zio.map(_._2)))(f)
      )
    }

  /**
   * Returns the [[ReleaseMap]] of the current scope. Under `use`, this is the
   * map that `use` releases at the end, so finalizers added to it run when the
   * `use` effect ends.
   */
  def releaseMap(implicit trace: Trace): ZManaged[Any, Nothing, ReleaseMap] =
    apply(ZManaged.currentReleaseMap.get.map((Finalizer.noop, _)))

  /**
   * Returns an ZManaged that accesses the runtime, which can be used to
   * (unsafely) execute tasks. This is useful for integration with legacy code
   * that must call back into ZIO code.
   */
  def runtime[R](implicit trace: Trace): ZManaged[R, Nothing, Runtime[R]] =
    ZManaged.fromZIO(ZIO.runtime[R])

  /**
   * Exposes the full cause of failure of `v` in the error channel, so failures
   * and defects can be handled as a `Cause[E]`. The inverse of `unsandbox`.
   */
  def sandbox[R, E, A](v: ZManaged[R, E, A])(implicit trace: Trace): ZManaged[R, Cause[E], A] =
    ZManaged.suspend(v.sandbox)

  /**
   * Returns a managed effect that describes shifting to the specified executor
   * as the `acquire` action with no release action.
   */
  def shift(executor: => Executor)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.fromZIO(ZIO.shift(executor))

  /**
   * A scope in which [[ZManaged]] values can be safely allocated. Passing a
   * managed resource to the `apply` method will return an effect that allocates
   * the resource and returns it with an early-release handle.
   */
  sealed abstract class Scope {

    /**
     * Acquires `managed` with its finalizers added to this scope, and returns
     * the resource together with a finalizer that releases it early. Resources
     * that are not released early are released with the release map in which
     * the `scope` value was acquired (for `use`, when the `use` effect ends).
     */
    def apply[R, E, A](managed: => ZManaged[R, E, A]): ZIO[R, E, (ZManaged.Finalizer, A)]
  }

  /**
   * Creates a [[Scope]] in which resources can be allocated, each with an
   * early-release handle. The resources still allocated in the `Scope` are
   * released with the release map in which this `ZManaged` is acquired (for
   * `use`, when the `use` effect ends), in reverse order together with the
   * other resources there. An early release of this value with
   * `withEarlyRelease` does not release them.
   */
  def scope(implicit trace: Trace): Managed[Nothing, Scope] =
    ZManaged.releaseMap.map { finalizers =>
      new Scope {
        override def apply[R, E, A](managed: => ZManaged[R, E, A]) =
          for {
            r  <- ZIO.environment[R]
            tp <- ZManaged.currentReleaseMap.locally(finalizers)(managed.zio)
          } yield tp
      }
    }

  /**
   * Converts a scoped `ZIO` effect (one that needs a `zio.Scope`) into a
   * `ZManaged`. The type parameter is the environment the effect needs besides
   * the `Scope`. The finalizers the effect adds to its `Scope` run when the
   * `ZManaged` is released, in reverse order of acquisition with the other
   * resources.
   *
   * {{{
   * import zio._
   *
   * val program: UIO[List[String]] =
   *   for {
   *     log <- Ref.make(List.empty[String])
   *     managed = ZManaged.scoped[Any](
   *                 ZIO.acquireRelease(log.update("acquire" :: _).as(42))(_ => log.update("release" :: _))
   *               )
   *     _      <- managed.use(n => log.update(s"use \$n" :: _))
   *     events <- log.get.map(_.reverse)
   *   } yield events
   * // List(acquire, use 42, release)
   * }}}
   *
   * @see
   *   [[https://zio.dev/reference/resource/scope/ Scope]]
   */
  def scoped[R]: ScopedPartiallyApplied[R] =
    new ScopedPartiallyApplied[R]

  /**
   * Accesses the specified service in the environment of the effect.
   */
  def service[A: Tag](implicit trace: Trace): ZManaged[A, Nothing, A] =
    ZManaged.environmentWith(_.get[A])

  /**
   * Accesses the service corresponding to the specified key in the environment.
   */
  def serviceAt[Service]: ZManaged.ServiceAtPartiallyApplied[Service] =
    new ZManaged.ServiceAtPartiallyApplied[Service]

  /**
   * Accesses the specified service in the environment of the effect.
   *
   * {{{
   * trait Foo {
   *   def foo(int: Int): String
   * }
   *
   * def foo(int: Int): ZManaged[Foo, Nothing, String] =
   *   ZManaged.serviceWith[Foo](_.foo(int))
   * }}}
   */
  def serviceWith[Service]: ServiceWithPartiallyApplied[Service] =
    new ServiceWithPartiallyApplied[Service]

  /**
   * Effectfully accesses the specified service in the environment of the
   * effect.
   *
   * {{{
   * import zio._
   *
   * trait Foo {
   *   def foo(int: Int): UIO[String]
   * }
   *
   * def foo(int: Int): ZManaged[Foo, Nothing, String] =
   *   ZManaged.serviceWithZIO[Foo](_.foo(int))
   * }}}
   */
  def serviceWithZIO[Service]: ServiceWithZIOPartiallyApplied[Service] =
    new ServiceWithZIOPartiallyApplied[Service]

  /**
   * Effectfully accesses the specified managed service in the environment of
   * the effect.
   *
   * Especially useful for creating "accessor" methods on Services' companion
   * objects accessing managed resources.
   *
   * {{{
   * trait Foo {
   *   def start(): ZManaged[Any, Nothing, Unit]
   * }
   *
   * def start: ZManaged[Foo, Nothing, Unit] =
   *   ZManaged.serviceWithManaged[Foo](_.start())
   * }}}
   */
  def serviceWithManaged[Service]: ServiceWithManagedPartiallyApplied[Service] =
    new ServiceWithManagedPartiallyApplied[Service]

  /**
   * Returns an effect with the optional value.
   */
  def some[A](a: => A)(implicit trace: Trace): UManaged[Option[A]] =
    succeed(Some(a))

  /**
   * Lifts a lazy, pure value into a Managed.
   */
  def succeed[A](r: => A)(implicit trace: Trace): ZManaged[Any, Nothing, A] =
    ZManaged(ZIO.succeed((Finalizer.noop, r)))

  /**
   * Returns a lazily constructed Managed.
   */
  def suspend[R, E, A](zManaged: => ZManaged[R, E, A])(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.unit.flatMap(_ => zManaged)

  /**
   * Returns a ZManaged value that represents a managed resource that can be
   * safely swapped within the scope of the ZManaged. The function provided
   * inside the ZManaged can be used to switch the resource currently in use.
   *
   * When the resource is switched, the finalizer for the previous resource will
   * be executed uninterruptibly, before the new resource is acquired. If the
   * effect executing inside the [[ZManaged#use]] is interrupted, the finalizer
   * for the resource currently in use is guaranteed to execute.
   *
   * This constructor can be used to create an expressive control flow that uses
   * several instances of a managed resource. For example:
   * {{{
   * import zio._
   *
   * trait FileWriter {
   *   def write(data: Int): Task[Unit]
   *   def close: UIO[Unit]
   * }
   * def makeWriter: Task[FileWriter] =
   *   ZIO.succeed(new FileWriter {
   *     def write(data: Int): Task[Unit] = ZIO.unit
   *     def close: UIO[Unit]             = ZIO.unit
   *   })
   *
   * val elements = List(1, 2, 3, 4)
   * val writingProgram =
   *   ZManaged.switchable[Any, Throwable, FileWriter].use { switchWriter =>
   *     ZIO.foreachDiscard(elements) { element =>
   *       for {
   *         writer <- switchWriter(makeWriter.toManagedWith(_.close))
   *         _      <- writer.write(element)
   *       } yield ()
   *     }
   *   }
   * }}}
   */
  def switchable[R, E, A](implicit trace: Trace): ZManaged[R, Nothing, ZManaged[R, E, A] => ZIO[R, E, A]] =
    for {
      releaseMap <- ZManaged.releaseMap
      key <- releaseMap
               .addIfOpen(_ => ZIO.unit)
               .flatMap {
                 case Some(key) => ZIO.succeed(key)
                 case None      => ZIO.interrupt
               }
               .toManaged
      switch = (newResource: ZManaged[R, E, A]) =>
                 ZIO.uninterruptibleMask { restore =>
                   for {
                     _ <- releaseMap
                            .replace(key, _ => ZIO.unit)
                            .flatMap(_.map(_.apply(Exit.unit)).getOrElse(ZIO.unit))
                     r     <- ZIO.environment[R]
                     inner <- ReleaseMap.make
                     a     <- restore(ZManaged.currentReleaseMap.locally(inner)(newResource.zio))
                     _ <- releaseMap
                            .replace(key, inner.releaseAll(_, ExecutionStrategy.Sequential))
                   } yield a._2
                 }
    } yield switch

  /**
   * A `ZManaged` that succeeds with `()` and has no release action.
   */
  lazy val unit: ZManaged[Any, Nothing, Unit] =
    ZManaged.succeed(())(Trace.empty)

  /**
   * The moral equivalent of `if (!p) exp`
   */
  def unless[R, E, A](b: => Boolean)(zManaged: => ZManaged[R, E, A])(implicit
    trace: Trace
  ): ZManaged[R, E, Option[A]] =
    suspend(if (b) none else zManaged.asSome)

  /**
   * The moral equivalent of `if (!p) exp` when `p` has side-effects
   */
  def unlessManaged[R, E](b: => ZManaged[R, E, Boolean]): ZManaged.UnlessManaged[R, E] =
    new ZManaged.UnlessManaged(() => b)

  /**
   * The inverse operation to `sandbox`. Submerges the full cause of failure.
   */
  def unsandbox[R, E, A](v: => ZManaged[R, Cause[E], A])(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.suspend(v.catchAll(ZManaged.failCause(_)))

  /**
   * Unwraps a `ZManaged` that is inside a `ZIO`.
   */
  def unwrap[R, E, A](fa: => ZIO[R, E, ZManaged[R, E, A]])(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.fromZIO(fa).flatten

  /**
   * The moral equivalent of `if (p) exp`
   */
  def when[R, E, A](b: => Boolean)(zManaged: => ZManaged[R, E, A])(implicit
    trace: Trace
  ): ZManaged[R, E, Option[A]] =
    ZManaged.suspend(if (b) zManaged.asSome else none)

  /**
   * Runs an effect when the supplied `PartialFunction` matches for the given
   * value, otherwise does nothing.
   */
  def whenCase[R, E, A, B](a: => A)(pf: PartialFunction[A, ZManaged[R, E, B]])(implicit
    trace: Trace
  ): ZManaged[R, E, Option[B]] =
    ZManaged.suspend(pf.andThen(_.asSome).applyOrElse(a, (_: A) => none))

  /**
   * Runs an effect when the supplied `PartialFunction` matches for the given
   * effectful value, otherwise does nothing.
   */
  def whenCaseManaged[R, E, A, B](
    a: => ZManaged[R, E, A]
  )(pf: PartialFunction[A, ZManaged[R, E, B]])(implicit trace: Trace): ZManaged[R, E, Option[B]] =
    ZManaged.suspend(a.flatMap(whenCase(_)(pf)))

  /**
   * The moral equivalent of `if (p) exp` when `p` has side-effects
   */
  def whenManaged[R, E](b: => ZManaged[R, E, Boolean]): ZManaged.WhenManaged[R, E] =
    new ZManaged.WhenManaged(() => b)

  /**
   * Locally installs a supervisor and an effect that succeeds with all the
   * children that have been forked in the returned effect.
   */
  def withChildren[R, E, A](
    get: UIO[Chunk[Fiber.Runtime[Any, Any]]] => ZManaged[R, E, A]
  )(implicit trace: Trace): ZManaged[R, E, A] =
    ZManaged.unwrap(Supervisor.track(true).map { supervisor =>
      // Filter out the fiber id of whoever is calling this:
      ZManaged(
        get(supervisor.value.flatMap(children => ZIO.fiberId.map(id => children.filter(_.id != id)))).zio
          .supervised(supervisor)
      )
    })

  /**
   * Returns a managed effect that describes setting the specified maximum
   * number of fibers for parallel operators as the `acquire` action and setting
   * it back to the original value as the `release` action.
   */
  def withParallism(n: => Int)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.scoped(FiberRef.parallelism.locallyScoped(Some(n)))

  /**
   * Returns a managed effect that describes setting an unbounded maximum number
   * of fibers for parallel operators as the `acquire` action and setting it
   * back to the original value as the `release` action.
   */
  def withParallismUnbounded(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
    ZManaged.scoped(FiberRef.parallelism.locallyScoped(None))

  /**
   * A `ZManagedConstructor[Input]` knows how to construct a `ZManaged` value
   * from an input of type `Input`. This allows the type of the `ZManaged` value
   * constructed to depend on `Input`.
   */
  sealed trait ZManagedConstructor[Input] {

    /**
     * The type of the `ZManaged` value.
     */
    type Out

    /**
     * Constructs a `ZManaged` value from the specified input.
     */
    def make(input: => Input)(implicit trace: Trace): Out
  }

  /**
   * The [[ZManagedConstructor]] instances that `ZManaged.from` uses: for
   * `Either`, `Option`, `Try` and their subtypes, for `Reservation` and for a
   * `ZIO` that produces a `Reservation`, and, with lower priority, for other
   * `ZIO` effects and for any other value.
   */
  object ZManagedConstructor extends ZManagedConstructorLowPriority1 {

    /**
     * Constructs a `ZManaged[Any, E, A]` from an `Either[E, A]`.
     */
    implicit def EitherConstructor[E, A]: WithOut[Either[E, A], ZManaged[Any, E, A]] =
      new ZManagedConstructor[Either[E, A]] {
        type Out = ZManaged[Any, E, A]
        def make(input: => Either[E, A])(implicit trace: Trace): ZManaged[Any, E, A] =
          ZManaged.fromEither(input)
      }

    /**
     * Constructs a `ZManaged[Any, E, A]` from a `Left[E, A]`.
     */
    implicit def EitherLeftConstructor[E, A]: WithOut[Left[E, A], ZManaged[Any, E, A]] =
      new ZManagedConstructor[Left[E, A]] {
        type Out = ZManaged[Any, E, A]
        def make(input: => Left[E, A])(implicit trace: Trace): ZManaged[Any, E, A] =
          ZManaged.fromEither(input)
      }

    /**
     * Constructs a `ZManaged[Any, E, A]` from a `Right[E, A]`.
     */
    implicit def EitherRightConstructor[E, A]: WithOut[Right[E, A], ZManaged[Any, E, A]] =
      new ZManagedConstructor[Right[E, A]] {
        type Out = ZManaged[Any, E, A]
        def make(input: => Right[E, A])(implicit trace: Trace): ZManaged[Any, E, A] =
          ZManaged.fromEither(input)
      }

    /**
     * Constructs a `ZManaged[Any, Option[Nothing], A]` from an `Option[A]`.
     */
    implicit def OptionConstructor[A]: WithOut[Option[A], ZManaged[Any, Option[Nothing], A]] =
      new ZManagedConstructor[Option[A]] {
        type Out = ZManaged[Any, Option[Nothing], A]
        def make(input: => Option[A])(implicit trace: Trace): ZManaged[Any, Option[Nothing], A] =
          ZManaged.fromOption(input)
      }

    /**
     * Constructs a `ZManaged[Any, Option[Nothing], Nothing]` from a `None`.
     */
    implicit val OptionNoneConstructor: WithOut[None.type, ZManaged[Any, Option[Nothing], Nothing]] =
      new ZManagedConstructor[None.type] {
        type Out = ZManaged[Any, Option[Nothing], Nothing]
        def make(input: => None.type)(implicit trace: Trace): ZManaged[Any, Option[Nothing], Nothing] =
          ZManaged.fromOption(input)
      }

    /**
     * Constructs a `ZManaged[Any, Option[Nothing], A]` from a `Some[A]`.
     */
    implicit def OptionSomeConstructor[A]: WithOut[Some[A], ZManaged[Any, Option[Nothing], A]] =
      new ZManagedConstructor[Some[A]] {
        type Out = ZManaged[Any, Option[Nothing], A]
        def make(input: => Some[A])(implicit trace: Trace): ZManaged[Any, Option[Nothing], A] =
          ZManaged.fromOption(input)
      }

    /**
     * Constructs a `ZManaged[R, E, A]` from a `Reservation[R, E, A]`.
     */
    implicit def ReservationConstructor[R, E, A]: WithOut[Reservation[R, E, A], ZManaged[R, E, A]] =
      new ZManagedConstructor[Reservation[R, E, A]] {
        type Out = ZManaged[R, E, A]
        def make(input: => Reservation[R, E, A])(implicit trace: Trace): ZManaged[R, E, A] =
          ZManaged.fromReservation(input)
      }

    /**
     * Constructs a `ZManaged[R1 with R2, E3, A]` from a `ZIO[R1, E1,
     * Reservation[R2, E2, A]]`, with `ZManaged.fromReservationZIO`.
     */
    implicit def ReservationZIOConstructor[R1, R2, E1 <: E3, E2 <: E3, E3, A]
      : WithOut[ZIO[R1, E1, Reservation[R2, E2, A]], ZManaged[R1 with R2, E3, A]] =
      new ZManagedConstructor[ZIO[R1, E1, Reservation[R2, E2, A]]] {
        type Out = ZManaged[R1 with R2, E3, A]
        def make(input: => ZIO[R1, E1, Reservation[R2, E2, A]])(implicit
          trace: Trace
        ): ZManaged[R1 with R2, E3, A] =
          ZManaged.fromReservationZIO(input)
      }

    /**
     * Constructs a `ZManaged[Any, Throwable, A]` from a `Try[A]`.
     */
    implicit def TryConstructor[A]: WithOut[scala.util.Try[A], ZManaged[Any, Throwable, A]] =
      new ZManagedConstructor[scala.util.Try[A]] {
        type Out = ZManaged[Any, Throwable, A]
        def make(input: => scala.util.Try[A])(implicit trace: Trace): ZManaged[Any, Throwable, A] =
          ZManaged.fromTry(input)
      }

    /**
     * Constructs a `ZManaged[Any, Throwable, A]` from a `Failure[A]`.
     */
    implicit def TryFailureConstructor[A]: WithOut[scala.util.Failure[A], ZManaged[Any, Throwable, A]] =
      new ZManagedConstructor[scala.util.Failure[A]] {
        type Out = ZManaged[Any, Throwable, A]
        def make(input: => scala.util.Failure[A])(implicit trace: Trace): ZManaged[Any, Throwable, A] =
          ZManaged.fromTry(input)
      }

    /**
     * Constructs a `ZManaged[Any, Throwable, A]` from a `Success[A]`.
     */
    implicit def TrySuccessConstructor[A]: WithOut[scala.util.Success[A], ZManaged[Any, Throwable, A]] =
      new ZManagedConstructor[scala.util.Success[A]] {
        type Out = ZManaged[Any, Throwable, A]
        def make(input: => scala.util.Success[A])(implicit trace: Trace): ZManaged[Any, Throwable, A] =
          ZManaged.fromTry(input)
      }
  }

  /**
   * The lower-priority [[ZManagedConstructor]] instance for `ZIO` effects, used
   * when no more specific instance (for example the one for a `ZIO` that
   * produces a `Reservation`) applies.
   */
  trait ZManagedConstructorLowPriority1 extends ZManagedConstructorLowPriority2 {

    /**
     * Constructs a `ZManaged[R, E, A]` from a `ZIO[R, E, A]`.
     */
    implicit def ZIOConstructor[R, E, A]: WithOut[ZIO[R, E, A], ZManaged[R, E, A]] =
      new ZManagedConstructor[ZIO[R, E, A]] {
        type Out = ZManaged[R, E, A]
        def make(input: => ZIO[R, E, A])(implicit trace: Trace): ZManaged[R, E, A] =
          ZManaged.fromZIO(input)
      }
  }

  /**
   * The lowest-priority [[ZManagedConstructor]] instance, which converts any
   * value with `ZManaged.attempt`, and the `WithOut` type alias.
   */
  trait ZManagedConstructorLowPriority2 {

    /**
     * The type of the `ZManagedConstructor` with the type of the `ZManaged`
     * value.
     */
    type WithOut[In, Out0] = ZManagedConstructor[In] { type Out = Out0 }

    /**
     * Constructs a `ZManaged[Any, Throwable, A]` from an `A`.
     */
    implicit def AttemptConstructor[A]: WithOut[A, ZManaged[Any, Throwable, A]] =
      new ZManagedConstructor[A] {
        type Out = ZManaged[Any, Throwable, A]
        def make(input: => A)(implicit trace: Trace): ZManaged[Any, Throwable, A] =
          ZManaged.attempt(input)
      }
  }

  @deprecated("use succeed", "2.0.9")
  private[zio] def succeedNow[A](r: A): ZManaged[Any, Nothing, A] =
    succeed(r)(Trace.empty)

  /**
   * Adds `refineToOrDie` to a [[ZManaged]] whose error type is a `Throwable`.
   */
  implicit final class RefineToOrDieOps[R, E <: Throwable, A](private val self: ZManaged[R, E, A]) extends AnyVal {

    /**
     * Keeps some of the errors, and terminates the fiber with the rest. Errors
     * of type `E1` stay failures; any other error becomes a defect.
     */
    def refineToOrDie[E1 <: E: ClassTag](implicit ev: CanFail[E], trace: Trace): ZManaged[R, E1, A] =
      self.refineOrDie { case e: E1 => e }
  }
}
