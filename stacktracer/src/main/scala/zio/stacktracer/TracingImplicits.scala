package zio.stacktracer

/** Holds [[TracingImplicits.disableAutoTrace]]. */
object TracingImplicits {

  /**
   * Turns off automatic trace generation where it is imported. With `import
   * zio.stacktracer.TracingImplicits.disableAutoTrace` in scope, a call that
   * needs an implicit `zio.Trace` and finds none in scope no longer gets one
   * generated from its source position: it fails to compile. Pass the trace
   * through an implicit `Trace` parameter, or call
   * `zio.internal.stacktracer.Tracer.newTrace` to generate one explicitly.
   *
   * The compiler error reads "could not find implicit value for parameter
   * trace: zio.Trace" on Scala 2 and "Auto-generated traces are disabled" on
   * Scala 3; on both, the compiler also prints the call site and these hints.
   */
  implicit val disableAutoTrace: DisableAutoTrace = new DisableAutoTrace {}
}

/**
 * The type of [[TracingImplicits.disableAutoTrace]]. When an implicit value of
 * this type is in scope, traces are not generated automatically.
 */
sealed trait DisableAutoTrace
