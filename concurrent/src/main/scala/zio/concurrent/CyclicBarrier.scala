package zio.concurrent

import zio._
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * A synchronization aid that allows a set of fibers to all wait for each other
 * to reach a common barrier point.
 *
 * CyclicBarriers are useful in programs involving a fixed sized party of fibers
 * that must occasionally wait for each other. The barrier is called cyclic
 * because it can be re-used after the waiting fibers are released.
 *
 * A CyclicBarrier supports an optional action command that is run once per
 * barrier point, after the last fiber in the party arrives, but before any
 * fibers are released. This barrier action is useful for updating shared-state
 * before any of the parties continue. A fiber that calls `await` while the
 * action is still running joins the cycle that is finishing and is released
 * with it.
 *
 * When a waiting party is interrupted, the barrier breaks: the other waiting
 * parties fail with `()`, and every later `await` fails with `()` until `reset`
 * is called.
 *
 * {{{
 * import zio._
 *
 * val tickets: UIO[List[Int]] =
 *   for {
 *     barrier <- CyclicBarrier.make(3)
 *     arrived <- ZIO
 *                  .foreachPar(List("a", "b", "c"))(_ => barrier.await)
 *                  .orElseSucceed(Nil)
 *   } yield arrived.sorted // List(0, 1, 2)
 * }}}
 *
 * @see
 *   [[https://zio.dev/reference/sync/cyclicbarrier/ CyclicBarrier]]
 */
final class CyclicBarrier private (
  /** The number of parties required to trip this barrier. */
  val parties: Int,
  _action: UIO[Any]
) {
  import CyclicBarrier.{newPromise, trace}

  @volatile private var _broken: Boolean           = false
  @volatile private var _lock: Promise[Unit, Unit] = newPromise()
  @volatile private var _waiting: Int              = 0

  private val semaphore = Semaphore.unsafe.make(1)(Unsafe)

  private def break: UIO[Unit] =
    semaphore.withPermit(ZIO.succeed {
      _broken = true
      _lock.unsafe.done(Exit.failUnit)(Unsafe)
      ()
    })

  private val succeedAndReset =
    semaphore.withPermit(ZIO.succeed {
      _lock.unsafe.done(Exit.unit)(Unsafe)
      resetUnsafe()
    })

  /**
   * Unsafely resets the barrier to its initial state. Breaks any waiting party.
   *
   * '''NOTE''': This method must only be called when holding a permit!
   */
  private def resetUnsafe(): Unit = {
    if (_waiting != 0) {
      _lock.unsafe.done(Exit.failUnit)(Unsafe)
      _waiting = 0
    }
    _lock = newPromise()
    _broken = false
  }

  /**
   * Waits until all parties have invoked await on this barrier. Fails if the
   * barrier is broken.
   *
   * Returns the number of parties that had still to arrive when this one did:
   * `parties - 1` for the first party of a cycle and `0` for the last. Fails
   * with `()` when the barrier is already broken, and when it breaks while
   * waiting (because of `reset` or the interruption of another waiting party).
   */
  val await: IO[Unit, Int] =
    ZIO.uninterruptibleMask { restore =>
      semaphore.withPermit {
        ZIO.succeed {
          if (_broken) Exit.failUnit
          else {
            val nParties = parties
            val waiting = {
              val waiting0 = _waiting + 1
              if (waiting0 == nParties) _waiting = 0
              else _waiting = waiting0
              waiting0
            }
            val f =
              if (waiting == nParties) restore(_action) *> succeedAndReset
              else {
                val lock = _lock
                restore(lock.await).onInterrupt(break)
              }
            val remaining = nParties - waiting
            f.as(remaining)
          }
        }
      }.flatten
    }

  /**
   * Queries if this barrier is in a broken state. It becomes `true` when a
   * waiting party is interrupted, and `false` again after `reset`.
   */
  def isBroken: UIO[Boolean] =
    ZIO.succeed(_broken)

  /**
   * The number of parties currently waiting at the barrier. After a waiting
   * party is interrupted, it still counts the parties that were waiting when
   * the barrier broke, until `reset`.
   */
  def waiting: UIO[Int] =
    ZIO.succeed(_waiting)

  /**
   * Resets the barrier to its initial state. Breaks any waiting party.
   *
   * The waiting parties fail with `()`. Afterwards the barrier is not broken
   * and no party is waiting.
   */
  def reset: UIO[Unit] =
    semaphore.withPermit(ZIO.succeed(resetUnsafe()))

}

/** Constructors for [[CyclicBarrier]]. */
object CyclicBarrier {
  private implicit def trace: Trace = Trace.empty

  /**
   * Creates a barrier for `parties` fibers, with no barrier action. `parties`
   * is not checked: with `0`, `await` never completes.
   */
  def make(parties: Int): UIO[CyclicBarrier] =
    make(parties, Exit.unit)

  /**
   * Creates a barrier for `parties` fibers that runs `action` each time the
   * last party arrives, on that party's fiber, before the waiting parties are
   * released. When `action` dies, that last party dies with the same defect,
   * the other parties of the cycle keep waiting, and the barrier is not broken.
   */
  def make(parties: Int, action: UIO[Any]): UIO[CyclicBarrier] =
    ZIO.succeed(new CyclicBarrier(parties, action))

  private def newPromise(): Promise[Unit, Unit] =
    Promise.unsafe.make(FiberId.None)(Unsafe)
}
