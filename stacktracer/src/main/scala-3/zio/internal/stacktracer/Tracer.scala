package zio.internal.stacktracer

/**
 * Creates and reads the traces that ZIO records in execution traces.
 * `zio.Trace` is `Tracer.instance.Type with Tracer.Traced`.
 */
object Tracer {

  /**
   * The marker that `zio.Trace` adds to `Tracer.instance.Type`. It is a trait
   * on Scala 2 and an alias for `Any` on Scala 3.
   */
  type Traced = Any

  /**
   * Generates a trace for the call site whenever a `zio.Trace` is required
   * implicitly and no other implicit `Trace` is in scope (an implicit `Trace`
   * parameter, for example, is used instead). The trace reads
   * `owner.path(File.scala:line)`, where the owner path names the enclosing
   * package, objects, classes, methods and vals, and `File.scala` is the file
   * name without its directory.
   *
   * Importing `zio.stacktracer.TracingImplicits.disableAutoTrace` makes such a
   * call site fail to compile instead.
   */
  inline given autoTrace: Tracer.instance.Type =
    ${ Macros.autoTraceImpl }

  /**
   * Generates a trace for the call site, in the same form as `autoTrace`. It
   * also works where `zio.stacktracer.TracingImplicits.disableAutoTrace` is
   * imported.
   */
  inline def newTrace: Tracer.instance.Type =
    ${ Macros.newTraceImpl }

  /**
   * The `Tracer` that ZIO uses. Its traces are interned strings of the form
   * `location(file:line)`.
   */
  val instance: Tracer = new Tracer {
    type Type = String
    val empty = "".intern()

    def unapply(trace: Type): Option[(String, String, Int)] = {
      val parsed = parseOrNull(trace)
      if (parsed eq null) None else Some((parsed.location, parsed.file, parsed.line))
    }

    private[zio] def parseOrNull(trace: Type): ParsedTrace = TracerUtils.parse(trace)

    def apply(location: String, file: String, line: Int): Type with Traced =
      createTrace(location, file, line).asInstanceOf[Type with Traced]
  }

  private[internal] def createTrace(location: String, file: String, line: Int): String =
    s"$location($file:$line)".intern

}

/**
 * Creates traces of type `Type` and reads their parts. Its only instance is
 * `Tracer.instance`.
 */
sealed trait Tracer {

  /** The type of the traces that this `Tracer` creates. */
  type Type <: AnyRef

  /**
   * The empty trace, which carries no location. On Scala 2 its type is `Type
   * with Tracer.Traced`; on Scala 3 it is `Type`.
   */
  val empty: Type

  /**
   * Extracts the location, file and line of a trace. Returns `None` for the
   * empty trace and for a value that is not of the form `location(file:line)`
   * with a positive line.
   */
  def unapply(trace: Type): Option[(String, String, Int)]
  private[zio] def parseOrNull(trace: Type): ParsedTrace

  /** Creates the trace `location(file:line)`. */
  def apply(location: String, file: String, line: Int): Type with Tracer.Traced
}
