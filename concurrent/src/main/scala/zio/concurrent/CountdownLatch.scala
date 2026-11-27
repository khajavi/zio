package zio.concurrent

import zio._

/**
 * A synchronization aid that allows one or more fibers to wait until a set of
 * operations being performed in other fibers completes.
 *
 * A `CountdownLatch` is initialized with a given count. The `await` method
 * blocks until the current count reaches zero due to invocations of the
 * `countDown` method, after which all waiting fibers are released and any
 * subsequent invocations of `await` return immediately. This is a one-shot
 * phenomenon -- the count cannot be reset. If you need a version that resets
 * the count, consider using a [[CyclicBarrier]].
 *
 * A `CountdownLatch` can be used for a number of purposes. A `CountdownLatch`
 * initialized with a count of one serves as an on/off latch, or gate: all
 * fibers invoking `await` wait at the gate until it is opened by a fiber
 * invoking `countDown`. A `CountdownLatch` initialized to N can be used to make
 * one fiber wait until N fibers have completed some action, or some action has
 * been completed N times.
 *
 * A useful property of a `CountdownLatch` is that it doesn't require that
 * fibers calling `countDown` wait for the count to reach zero before
 * proceeding, it only prevents any fiber from proceeding past an `await` until
 * all fibers could pass. Interrupting one fiber in `await` does not affect the
 * other waiting fibers.
 *
 * {{{
 * import zio._
 *
 * val finished: UIO[Int] =
 *   for {
 *     latch <- CountdownLatch.make(3)
 *     done  <- Ref.make(0)
 *     _ <- ZIO.foreachDiscard(1 to 3) { _ =>
 *            (done.update(_ + 1) *> latch.countDown).fork
 *          }
 *     _ <- latch.await
 *     n <- done.get
 *   } yield n // 3
 * }}}
 *
 * @see
 *   [[https://zio.dev/reference/sync/countdownlatch/ CountdownLatch]]
 */
final class CountdownLatch private (_count: Ref[Int], _waiters: Promise[Nothing, Unit]) {

  /**
   * Causes the current fiber to wait until the latch has counted down to zero.
   * Returns immediately once the count is zero.
   */
  val await: UIO[Unit] = _waiters.await

  /**
   * Decrements the count of the latch, releasing all waiting fibers if the
   * count reaches zero. Does nothing when the count is already zero.
   */
  val countDown: UIO[Unit] = _count.modify {
    case 0 => ZIO.unit             -> 0
    case 1 => _waiters.succeedUnit -> 0
    case n => ZIO.unit             -> (n - 1)
  }.flatten.unit

  /** Returns the current count */
  val count: UIO[Int] = _count.get
}

/** Constructors for [[CountdownLatch]]. */
object CountdownLatch {

  /**
   * Creates a latch with the count `n`. The effect dies with a
   * `java.lang.IllegalArgumentException` ("n must be positive") when `n` is
   * zero or negative.
   */
  def make(n: Int): UIO[CountdownLatch] =
    if (n <= 0)
      ZIO.die(new IllegalArgumentException("n must be positive"))
    else
      Ref.make(n).zipWith(Promise.make[Nothing, Unit])(new CountdownLatch(_, _))
}
