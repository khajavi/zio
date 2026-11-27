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

package zio

import zio.stacktracer.TracingImplicits.disableAutoTrace
import zio.stm._
import zio.stream._
import zio.ZLayer._

import java.io.{IOException, InputStream}

/**
 * The `Managed` library provides the `ZManaged` data type for backward
 * compatibility after the introduction of scopes. For the best experience,
 * import `zio.managed._` to get access to extension methods on ZIO data types
 * that convert to and from `ZManaged` values. This will give you the smoothest
 * possible experience until you are able to upgrade to using scopes directly.
 */
package object managed extends ZManagedCompatPlatformSpecific {

  /** A `ZManaged` with no requirements: `ZManaged[Any, E, A]`. */
  type Managed[+E, +A] = ZManaged[Any, E, A]

  /**
   * A `ZManaged` with no requirements that can fail with a `Throwable`:
   * `ZManaged[Any, Throwable, A]`.
   */
  type TaskManaged[+A] = ZManaged[Any, Throwable, A]

  /**
   * A `ZManaged` that requires an `R` and can fail with a `Throwable`:
   * `ZManaged[R, Throwable, A]`.
   */
  type RManaged[-R, +A] = ZManaged[R, Throwable, A]

  /**
   * A `ZManaged` with no requirements that cannot fail: `ZManaged[Any, Nothing,
   * A]`.
   */
  type UManaged[+A] = ZManaged[Any, Nothing, A]

  /**
   * A `ZManaged` that requires an `R` and cannot fail: `ZManaged[R, Nothing,
   * A]`.
   */
  type URManaged[-R, +A] = ZManaged[R, Nothing, A]

  /** Adds `makeManaged` to the `Promise` companion object. */
  implicit final class ZManagedPromiseCompanionSyntax(private val self: Promise.type) extends AnyVal {

    /**
     * Returns a managed value that creates a new `Promise` when acquired, with
     * no release action.
     */
    def makeManaged[E, A](implicit trace: Trace): ZManaged[Any, Nothing, Promise[E, A]] =
      ZManaged.fromZIO(Promise.make[E, A])
  }

  /** Adds `locallyManaged` to `FiberRef`. */
  implicit final class ZManagedFiberRefSyntax[A](private val self: FiberRef[A]) {

    /**
     * Returns a managed effect that sets the value associated with the current
     * fiber to the specified value as its `acquire` action and restores it to
     * its original value as its `release` action.
     */
    def locallyManaged(value: A)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
      ZManaged.scoped(self.locallyScoped(value))
  }

  /** Adds `forkManaged`, `toManaged` and `toManagedWith` to `ZIO`. */
  implicit final class ZManagedZIOSyntax[R, E, A](private val self: ZIO[R, E, A]) {

    /**
     * Forks this effect in a [[ZManaged]]. Using the [[ZManaged]] value starts
     * the effect in a new fiber, and the fiber is interrupted when the managed
     * value is released, for example when the effect passed to [[ZManaged#use]]
     * completes. A failure of the forked effect does not fail the managed
     * value; it is observed through the returned fiber.
     */
    final def forkManaged(implicit trace: Trace): ZManaged[R, Nothing, Fiber.Runtime[E, A]] =
      self.toManaged.fork

    /**
     * Converts this ZIO to [[ZManaged]] with no release action. It will be
     * performed interruptibly.
     */
    final def toManaged(implicit trace: Trace): ZManaged[R, E, A] =
      ZManaged.fromZIO(self)

    /**
     * Converts this ZIO to a [[ZManaged]] that runs `release` with the acquired
     * value as its release action. This ZIO and the release action are
     * performed uninterruptibly.
     */
    final def toManagedWith[R1 <: R](release: A => URIO[R1, Any])(implicit trace: Trace): ZManaged[R1, E, A] =
      ZManaged.acquireReleaseWith(self)(release)
  }

  /** Adds `toManagedAuto` to a `ZIO` that produces an `AutoCloseable`. */
  implicit final class ZManagedZIOAutoCloseableSyntax[R, E, A <: AutoCloseable](private val self: ZIO[R, E, A])
      extends AnyVal {

    /**
     * Converts this ZIO value to a ZManaged value. See
     * [[ZManaged.fromAutoCloseable]].
     */
    def toManagedAuto(implicit trace: Trace): ZManaged[R, E, A] =
      ZManaged.fromAutoCloseable(self)
  }

  /** Adds `reserve` to the `ZIO` companion object. */
  implicit final class ZManagedZIOCompanionSyntax(private val self: ZIO.type) extends AnyVal {

    /**
     * Acquires a resource, uses the resource, and then releases the resource.
     * However, unlike `acquireReleaseWith`, the separation of these phases
     * allows the acquisition to be interruptible: the effect that produces the
     * `Reservation` runs uninterruptibly, while the reservation's `acquire`
     * action is interruptible, and its release action runs when the use
     * completes or the acquisition is interrupted.
     *
     * Useful for concurrent data structures and other cases where the
     * 'deallocator' can tell if the allocation succeeded or not just by
     * inspecting internal / external state.
     */
    def reserve[R, E, A, B](reservation: => ZIO[R, E, Reservation[R, E, A]])(use: A => ZIO[R, E, B])(implicit
      trace: Trace
    ): ZIO[R, E, B] =
      ZManaged.fromReservationZIO(reservation).use(use)
  }

  /** Adds `toManaged` to `ZLayer`. */
  implicit final class ZManagedZLayerSyntax[R, E, A](private val self: ZLayer[R, E, A]) extends AnyVal {

    /**
     * Converts this layer to a ZManaged value that builds the layer's
     * environment when acquired and releases it when released.
     */
    def toManaged(implicit trace: Trace): ZManaged[R, E, ZEnvironment[A]] =
      ZManaged.scoped[R](self.build)
  }

  /** Adds `fromManaged` and `fromManagedEnvironment` to `ZLayer`. */
  implicit final class ZManagedZLayerCompanionSyntax(private val self: ZLayer.type) extends AnyVal {

    /**
     * Constructs a layer from a managed resource.
     */
    def fromManaged[R, E, A: Tag](m: ZManaged[R, E, A])(implicit
      trace: Trace
    ): ZLayer[R, E, A] =
      ZLayer.scoped[R](m.scoped)

    /**
     * Constructs a layer from a managed resource, which must return one or more
     * services.
     */
    def fromManagedEnvironment[R, E, A](m: ZManaged[R, E, ZEnvironment[A]])(implicit
      trace: Trace
    ): ZLayer[R, E, A] =
      ZLayer.scopedEnvironment[R](m.scoped)
  }

  /** Adds `makeManaged` to the `Ref` companion object. */
  implicit final class ZManagedRefCompanionSyntax(private val self: Ref.type) extends AnyVal {

    /**
     * Returns a managed value that creates a new `Ref` holding `a` when
     * acquired, with no release action.
     */
    def makeManaged[A](a: A)(implicit trace: Trace): ZManaged[Any, Nothing, Ref[A]] =
      ZManaged.fromZIO(Ref.make(a))
  }

  /** Adds `makeManaged` to the `Ref.Synchronized` companion object. */
  implicit final class ZManagedRefSynchronizedCompanionSyntax(private val self: Ref.Synchronized.type) extends AnyVal {

    /**
     * Returns a managed value that creates a new `Ref.Synchronized` holding `a`
     * when acquired, with no release action.
     */
    def makeManaged[A](a: A)(implicit trace: Trace): ZManaged[Any, Nothing, Ref.Synchronized[A]] =
      ZManaged.fromZIO(Ref.Synchronized.make(a))
  }

  /** Adds `subscribeManaged` to `THub`. */
  implicit final class ZManagedTHubSyntax[A](private val self: THub[A]) extends AnyVal {

    /**
     * Subscribes to receive messages from the hub. The resulting subscription
     * can be evaluated multiple times within the scope of the managed to take a
     * message from the hub each time.
     */
    final def subscribeManaged(implicit trace: Trace): ZManaged[Any, Nothing, TDequeue[A]] =
      ZManaged.scoped(self.subscribeScoped)
  }

  /** Adds `withPermitManaged` and `withPermitsManaged` to `TSemaphore`. */
  implicit final class ZManagedTSemaphoreSyntax(private val self: TSemaphore) extends AnyVal {

    /**
     * Returns a managed effect that describes acquiring a permit as the
     * `acquire` action and releasing it as the `release` action.
     */
    def withPermitManaged(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
      ZManaged.scoped(self.withPermitScoped)

    /**
     * Returns a managed effect that describes acquiring the specified number of
     * permits as the `acquire` action and releasing them as the `release`
     * action.
     */
    def withPermitsManaged(n: Long)(implicit trace: Trace): ZManaged[Any, Nothing, Unit] =
      ZManaged.scoped(self.withPermitsScoped(n))
  }

  /** Adds `runManaged` to `ZChannel`. */
  implicit final class ZManagedZChannelSyntax[Env, InErr, InElem, InDone, OutErr, OutElem, OutDone](
    private val self: ZChannel[Env, InErr, InElem, InDone, OutErr, OutElem, OutDone]
  ) extends AnyVal {

    /**
     * Runs this channel, which must have no input and no output elements, in a
     * [[ZManaged]] and yields its done value.
     */
    def runManaged(implicit
      ev1: Any <:< InElem,
      ev2: OutElem <:< Nothing,
      trace: Trace
    ): ZManaged[Env, OutErr, OutDone] =
      ZManaged.scoped[Env](self.runScoped)
  }

  /** Adds `managed`, `managedOut` and `unwrapManaged` to `ZChannel`. */
  implicit final class ZManagedZChannelCompanionSyntax(private val self: ZChannel.type) extends AnyVal {

    /**
     * Acquires the managed value `m`, then continues with the channel that
     * `use` builds from it.
     */
    def managed[Env, InErr, InElem, InDone, OutErr, OutElem, OutDone, A](m: => ZManaged[Env, OutErr, A])(
      use: A => ZChannel[Env, InErr, InElem, InDone, OutErr, OutElem, OutDone]
    )(implicit trace: Trace): ZChannel[Env, InErr, InElem, InDone, OutErr, OutElem, OutDone] =
      ZChannel.unwrapScoped[Env](m.scoped.map(use))

    /**
     * Acquires the managed value `m` and writes it as the only output element
     * of the channel.
     */
    def managedOut[R, E, A](
      m: => ZManaged[R, E, A]
    )(implicit trace: Trace): ZChannel[R, Any, Any, Any, E, A, Any] =
      ZChannel.scoped[R](m.scoped)

    /**
     * Acquires the managed value `channel`, then continues with the channel it
     * yields.
     */
    def unwrapManaged[Env, InErr, InElem, InDone, OutErr, OutElem, OutDone](
      channel: => ZManaged[Env, OutErr, ZChannel[Env, InErr, InElem, InDone, OutErr, OutElem, OutDone]]
    )(implicit
      trace: Trace
    ): ZChannel[Env, InErr, InElem, InDone, OutErr, OutElem, OutDone] =
      ZChannel.unwrapScoped[Env](channel.scoped)
  }

  /** Adds `unwrapManaged` to the `ZSink` companion object. */
  implicit final class ZManagedZSinkCompanionSyntax(private val self: ZSink.type) extends AnyVal {

    /**
     * Creates a sink produced from a managed effect: acquires `managed`, then
     * runs the sink it yields.
     */
    def unwrapManaged[R, E, In, L, Z](managed: => ZManaged[R, E, ZSink[R, E, In, L, Z]])(implicit
      trace: Trace
    ): ZSink[R, E, In, L, Z] =
      ZSink.unwrapScoped[R](managed.scoped)
  }

  /**
   * Adds the `...Managed` variants of the `ZStream` run methods, which return a
   * [[ZManaged]] instead of a scoped `ZIO`.
   */
  implicit final class ZManagedZStreamSyntax[R, E, A](private val self: ZStream[R, E, A]) extends AnyVal {

    /**
     * Executes a pure fold over the stream of values. Returns a managed value
     * that runs the fold when acquired and yields the result; the stream's
     * finalizers run when the managed value is released.
     */
    final def runFoldManaged[S](s: => S)(f: (S, A) => S)(implicit trace: Trace): ZManaged[R, E, S] =
      ZManaged.scoped[R](self.runFoldScoped(s)(f))

    /**
     * Executes an effectful fold over the stream of values. Returns a managed
     * value that runs the fold when acquired and yields the result; the
     * stream's finalizers run when the managed value is released.
     */
    final def runFoldManagedZIO[R1 <: R, E1 >: E, S](s: => S)(f: (S, A) => ZIO[R1, E1, S])(implicit
      trace: Trace
    ): ZManaged[R1, E1, S] =
      ZManaged.scoped[R1](self.runFoldScopedZIO[R1, E1, S](s)(f))

    /**
     * Executes an effectful fold over the stream of values. Returns a managed
     * value that runs the fold when acquired and yields the result; the
     * stream's finalizers run when the managed value is released. Stops the
     * fold early when the condition is not fulfilled.
     *
     * {{{
     * import zio._
     * import zio.managed._
     * import zio.stream._
     *
     * val sum: UIO[Int] =
     *   ZStream(1, 2, 3, 4)
     *     .runFoldWhileManagedZIO(0)(_ <= 4)((s, a) => ZIO.succeed(s + a))
     *     .use(ZIO.succeed(_)) // 6
     * }}}
     *
     * @param cont
     *   function which defines the early termination condition
     */
    final def runFoldWhileManagedZIO[R1 <: R, E1 >: E, S](
      s: => S
    )(cont: S => Boolean)(f: (S, A) => ZIO[R1, E1, S])(implicit trace: Trace): ZManaged[R1, E1, S] =
      ZManaged.scoped[R1](self.runFoldWhileScopedZIO[R1, E1, S](s)(cont)(f))

    /**
     * Executes a pure fold over the stream of values. Returns a managed value
     * that runs the fold when acquired and yields the result; the stream's
     * finalizers run when the managed value is released. Stops the fold early
     * when the condition is not fulfilled.
     */
    final def runFoldWhileManaged[S](s: => S)(cont: S => Boolean)(f: (S, A) => S)(implicit
      trace: Trace
    ): ZManaged[R, E, S] =
      ZManaged.scoped[R](self.runFoldWhileScoped(s)(cont)(f))

    /**
     * Like `ZStream#runForeachChunk`, but returns a `ZManaged` so the
     * finalization order can be controlled.
     */
    final def runForeachChunkManaged[R1 <: R, E1 >: E](f: Chunk[A] => ZIO[R1, E1, Any])(implicit
      trace: Trace
    ): ZManaged[R1, E1, Unit] =
      ZManaged.scoped[R1](self.runForeachChunkScoped(f))

    /**
     * Like `ZStream#runForeach`, but returns a `ZManaged` so the finalization
     * order can be controlled.
     *
     * `ZStream` has its own `runForeachScoped` member, which returns a scoped
     * `ZIO`, so `stream.runForeachScoped(f)` calls that member and not this
     * method.
     */
    final def runForeachScoped[R1 <: R, E1 >: E](f: A => ZIO[R1, E1, Any])(implicit
      trace: Trace
    ): ZManaged[R1, E1, Unit] =
      ZManaged.scoped[R1](self.runForeachScoped(f))

    /**
     * Like `ZStream#runForeachWhile`, but returns a `ZManaged` so the
     * finalization order can be controlled.
     */
    final def runForeachWhileManaged[R1 <: R, E1 >: E](f: A => ZIO[R1, E1, Boolean])(implicit
      trace: Trace
    ): ZManaged[R1, E1, Unit] =
      ZManaged.scoped[R1](self.runForeachWhileScoped(f))

    /**
     * Like `ZStream#runIntoHub`, but provides the result as a [[ZManaged]] to
     * allow for scope composition. Publishes the elements of the stream to the
     * hub as `Take` values, followed by a `Take` that signals the end of the
     * stream or its failure.
     */
    final def runIntoHubManaged[E1 >: E, A1 >: A](
      hub: => Hub[Take[E1, A1]]
    )(implicit trace: Trace): ZManaged[R, E1, Unit] =
      ZManaged.scoped[R](self.runIntoHubScoped(hub))

    /**
     * Like `ZStream#runIntoQueue`, but provides the result as a [[ZManaged]] to
     * allow for scope composition. Offers the elements of the stream to the
     * queue as `Take` values, followed by a `Take` that signals the end of the
     * stream or its failure.
     */
    final def runIntoQueueManaged(
      queue: => Enqueue[Take[E, A]]
    )(implicit trace: Trace): ZManaged[R, E, Unit] =
      ZManaged.scoped[R](self.runIntoQueueScoped(queue))

    /**
     * Like `ZStream#runIntoQueueElementsScoped`, but provides the result as a
     * [[ZManaged]] to allow for scope composition. Offers each element of the
     * stream to the queue as an `Exit.succeed`, followed by `Exit.fail(None)`
     * when the stream ends or `Exit.failCause` with the cause mapped to `Some`
     * when it fails.
     */
    final def runIntoQueueElementsManaged(
      queue: => Enqueue[Exit[Option[E], A]]
    )(implicit trace: Trace): ZManaged[R, E, Unit] =
      ZManaged.scoped[R](self.runIntoQueueElementsScoped(queue))

    /**
     * Runs the stream into the sink when the managed value is acquired and
     * yields the sink's result.
     */
    def runManaged[R1 <: R, E1 >: E, B](sink: => ZSink[R1, E1, A, Any, B])(implicit
      trace: Trace
    ): ZManaged[R1, E1, B] =
      ZManaged.scoped[R1](self.runScoped(sink))
  }

  /**
   * Adds the `...Managed` stream constructors to the `ZStream` companion
   * object.
   */
  implicit final class ZManagedZStreamCompanionSyntax(private val self: ZStream.type) extends AnyVal {

    /**
     * Creates a stream from a subscription to a hub in the context of a managed
     * effect. The managed effect describes subscribing to receive messages from
     * the hub while the stream describes taking messages from the hub.
     */
    def fromChunkHubManaged[O](
      hub: => Hub[Chunk[O]]
    )(implicit trace: Trace): ZManaged[Any, Nothing, ZStream[Any, Nothing, O]] =
      ZManaged.scoped(ZStream.fromChunkHubScoped(hub))

    /**
     * Creates a stream from a subscription to a hub in the context of a managed
     * effect. The managed effect describes subscribing to receive messages from
     * the hub while the stream describes taking messages from the hub.
     *
     * The hub will be shut down once the stream is closed.
     */
    def fromChunkHubManagedWithShutdown[O](
      hub: => Hub[Chunk[O]]
    )(implicit trace: Trace): ZManaged[Any, Nothing, ZStream[Any, Nothing, O]] =
      ZManaged.scoped(ZStream.fromChunkHubScopedWithShutdown(hub))

    /**
     * Creates a stream from a subscription to a hub in the context of a managed
     * effect. The managed effect describes subscribing to receive messages from
     * the hub while the stream describes taking messages from the hub.
     */
    def fromHubManaged[A](
      hub: => Hub[A],
      maxChunkSize: => Int = ZStream.DefaultChunkSize
    )(implicit trace: Trace): ZManaged[Any, Nothing, ZStream[Any, Nothing, A]] =
      ZManaged.scoped(ZStream.fromHubScoped(hub, maxChunkSize))

    /**
     * Creates a stream from a subscription to a hub in the context of a managed
     * effect. The managed effect describes subscribing to receive messages from
     * the hub while the stream describes taking messages from the hub.
     *
     * The hub will be shut down once the stream is closed.
     */
    def fromHubManagedWithShutdown[A](
      hub: => Hub[A],
      maxChunkSize: => Int = ZStream.DefaultChunkSize
    )(implicit trace: Trace): ZManaged[Any, Nothing, ZStream[Any, Nothing, A]] =
      ZManaged.scoped(ZStream.fromHubScopedWithShutdown(hub, maxChunkSize))

    /**
     * Creates a stream from a managed `java.io.InputStream` value.
     */
    def fromInputStreamManaged[R](
      is: => ZManaged[R, IOException, InputStream],
      chunkSize: => Int = ZStream.DefaultChunkSize
    )(implicit trace: Trace): ZStream[R, IOException, Byte] =
      ZStream.fromInputStreamScoped[R](is.scoped, chunkSize)

    /**
     * Creates a stream from a managed iterator. The `maxChunkSize` parameter is
     * the maximum number of elements in a chunk.
     */
    def fromIteratorManaged[R, A](
      iterator: => ZManaged[R, Throwable, Iterator[A]],
      maxChunkSize: => Int = ZStream.DefaultChunkSize
    )(implicit
      trace: Trace
    ): ZStream[R, Throwable, A] =
      ZStream.fromIteratorScoped[R, A](iterator.scoped, maxChunkSize)

    /**
     * Creates a stream from a managed Java iterator.
     */
    def fromJavaIteratorManaged[R, A](iterator: => ZManaged[R, Throwable, java.util.Iterator[A]])(implicit
      trace: Trace
    ): ZStream[R, Throwable, A] =
      fromJavaIteratorManaged(iterator, ZStream.DefaultChunkSize)

    /**
     * Creates a stream from a managed Java iterator. The `chunkSize` parameter
     * is the maximum number of elements in a chunk.
     */
    def fromJavaIteratorManaged[R, A](
      iterator: => ZManaged[R, Throwable, java.util.Iterator[A]],
      chunkSize: Int
    )(implicit
      trace: Trace
    ): ZStream[R, Throwable, A] =
      ZStream.fromJavaIteratorScoped[R, A](iterator.scoped, chunkSize)

    /**
     * Creates a single-element stream that acquires the managed value and emits
     * it.
     */
    def managed[R, E, A](managed: => ZManaged[R, E, A])(implicit trace: Trace): ZStream[R, E, A] =
      ZStream.scoped[R](managed.scoped)

    /**
     * Creates a stream from a managed effect that yields a stream: acquires
     * `fa`, then emits the elements of the stream it yields.
     *
     * `ZStream` has its own `unwrapScoped` member, so `ZStream.unwrapScoped(m)`
     * calls that member and not this method.
     */
    def unwrapScoped[R, E, A](fa: => ZManaged[R, E, ZStream[R, E, A]])(implicit
      trace: Trace
    ): ZStream[R, E, A] =
      ZStream.unwrapScoped[R](fa.scoped)
  }
}
