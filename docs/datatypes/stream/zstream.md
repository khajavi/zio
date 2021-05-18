---
id: zstream
title: "ZStream"
---

A `ZStream[R, E, O]` is a description of a program that, when evaluated, may emit zero or more values of type `O`, may fail with errors of type `E`, and uses an environment of type `R`. 

One way to think of `ZStream` is as a `ZIO` program that could emit multiple values. As we know, a `ZIO[R, E, A]` data type, is a functional effect which is a description of a program that needs an environment of type `R`, it may end with an error of type `E`, and in case of success, it returns a value of type `A`. The important note about `ZIO` effects is that in the case of success they always end with exactly one value. There is no optionality here, no multiple infinite values, we always get exact value:

```scala mdoc:invisible
import zio.{ZIO, Task, ZManaged}
```

```scala mdoc:silent:nest
val failedEffect: ZIO[Any, String, Nothing]       = ZIO.fail("fail!")
val oneIntValue : ZIO[Any, Nothing, Int]          = ZIO.succeed(3)
val oneListValue: ZIO[Any, Nothing, List[Int]]    = ZIO.succeed(List(1, 2, 3))
val oneOption   : ZIO[Any, Nothing , Option[Int]] = ZIO.succeed(None)
```

A functional stream is pretty similar, it is a description of a program that requires an environment of type `R` and it may signal with errors of type `E` and it yields `O`, but the difference is that it will yield zero or more values. 

So a `ZStream` represents one of the following cases in terms of its elements:
- **An Empty Stream** — It might end up empty; which represent an empty stream, e.g. `ZStream.empty`.
- **One Element Stream** — It can represent a stream with just one value, e.g. `ZStream.succeed(3)`.
- **Multiple Finite Element Stream** — It can represent a stream of finite values, e.g. `ZStream.range(1, 10)`
- **Multiple Infinite Element Stream** — It can even represent a stream that _never ends_ as an infinite stream, e.g. `ZStream.iterate(1)(_ + 1)`.

```scala mdoc:silent:nest
import zio.stream.ZStream
val emptyStream         : ZStream[Any, Nothing, Nothing]   = ZStream.empty
val oneIntValueStream   : ZStream[Any, Nothing, Int]       = ZStream.succeed(4)
val oneListValueStream  : ZStream[Any, Nothing, List[Int]] = ZStream.succeed(List(1, 2, 3))
val finiteIntStream     : ZStream[Any, Nothing, Int]       = ZStream.range(1, 10)
val infiniteIntStream   : ZStream[Any, Nothing, Int]       = ZStream.iterate(1)(_ + 1)
```

Another example of a stream is when we're pulling a Kafka topic or reading from a socket. There is no inherent definition of an end there. Stream elements arrive at some point, or even they might never arrive at any point.

## Creation

There are several ways to create ZIO Stream. In this section, we are going to enumerate some of the important ways of creating `ZStream`. 

### Common Constructors

**ZStream.apply** — Creates a pure stream from a variable list of values:

```scala mdoc:silent:nest
val stream: ZStream[Any, Nothing, Int] = ZStream(1, 2, 3)
```

**ZStream.unit** — A stream that contains a single `Unit` value:

```scala mdoc:silent:nest
val unit: ZStream[Any, Nothing, Unit] = ZStream.unit
```

**ZStream.never** — A stream that produces no value or fails with an error:

```scala mdoc:silent:nest
val never: ZStream[Any, Nothing, Nothing] = ZStream.never
```

**ZStream.repeat** — Takes an initial value and applies the given function to the initial value iteratively. The initial value is the first value produced by the stream, followed by f(init), f(f(init)), ...

```scala mdoc:silent:nest
val nats: ZStream[Any, Nothing, Int] = 
  ZStream.iterate(1)(_ + 1) // 1, 2, 3, ...
```

**ZStream.range** — A stream from a range of integers `[min, max)`:

```scala mdoc:silent:nest
val range: ZStream[Any, Nothing, Int] = ZStream.range(1, 5) // 1, 2, 3, 4
```

### From Success and Failure

Similar to `ZIO` data type, we can create a `ZStream` using `fail` and `succeed` methods:

```scala mdoc:silent:nest
val s1: ZStream[Any, String, Nothing] = ZStream.fail("Uh oh!")
val s2: ZStream[Any, Nothing, Int]    = ZStream.succeed(5)
```

### From Iterators

Iterators are data structures that allow us to iterate over a sequence of elements. Similarly, we can think of ZIO Streams as effectual Iterators; every `ZStream` represents a collection of one or more, but effectful values. 

**ZStream.fromIteratorTotal** — We can convert an iterator that does not throw exception to `ZStream` by using `ZStream.fromIteratorTotal`:

```scala mdoc:silent:nest
val s1: ZStream[Any, Throwable, Int] = ZStream.fromIterator(Iterator(1, 2, 3))
val s2: ZStream[Any, Throwable, Int] = ZStream.fromIterator(Iterator.range(1, 4))
val s3: ZStream[Any, Throwable, Int] = ZStream.fromIterator(Iterator.continually(0))
```

Also, there is another constructor called **`ZStream.fromIterator`** that creates a stream from an iterator which may throw an exception.

**ZStream.fromIteratorEffect** — If we have an effectful Iterator that may throw Exception, we can use `fromIteratorEffect` to convert that to the ZIO Stream:

```scala mdoc:silent:nest
import scala.io.Source
val lines: ZStream[Any, Throwable, String] = 
  ZStream.fromIteratorEffect(Task(Source.fromFile("file.txt").getLines()))
```

Using this method is not good for resourceful effects like above, so it's better to rewrite that using `ZStream.fromIteratorManaged` function.

**ZStream.fromIteratorManaged** — Using this constructor we can convert a managed iterator to ZIO Stream:

```scala mdoc:silent:nest
val lines: ZStream[Any, Throwable, String] = 
  ZStream.fromIteratorManaged(
    ZManaged.fromAutoCloseable(
      Task(scala.io.Source.fromFile("file.txt"))
    ).map(_.getLines())
  )
```

**ZStream.fromJavaIterator** — It is the Java version of these constructors which create a stream from Java iterator that may throw an exception. We can convert any Java collection to an iterator and then lift them to the ZIO Stream.

For example, to convert the Java Stream to the ZIO Stream, `ZStream` has a `fromJavaStream` constructor which convert the Java Stream to the Java Iterator and then convert that to the ZIO Stream using `ZStream.fromJavaIterator` constructor:

```scala mdoc:silent:nest
def fromJavaStream[R, A](stream: => java.util.stream.Stream[A]): ZStream[R, Throwable, A] =
  ZStream.fromJavaIterator(stream.iterator())
```

Similarly, `ZStream` has `ZStream.fromJavaIteratorTotal`, `ZStream.fromJavaIteratorEffect` and `ZStream.fromJavaIteratorManaged` constructors.

### From Iterables

**ZStream.fromIterable** — We can create a stream from `Iterable` collection of values:

```scala mdoc:silent
val list = ZStream.fromIterable(List(1, 2, 3))
```

**ZStream.fromIterableM** — If we have an effect producing a value of type `Iterable` we can use `fromIterableM` constructor to create a stream of that effect.

Assume we have a database that returns a list of users using `Task`:

```scala mdoc:invisible
import zio._
case class User(name: String)
```

```scala mdoc:silent:nest
trait Database {
  def getUsers: Task[List[User]]
}

object Database {
  def getUsers: ZIO[Has[Database], Throwable, List[User]] = 
    ZIO.serviceWith[Database](_.getUsers)
}
```

As this operation is effectful, we can use `ZStream.fromIterableM` to convert the result to the `ZStream`:

```scala mdoc:silent:nest
val users: ZStream[Has[Database], Throwable, User] = 
  ZStream.fromIterableM(Database.getUsers)
```

### From Repetition

**ZStream.repeat** — Repeats the provided value infinitely:

```scala mdoc:silent:nest
val repeatZero: ZStream[Any, Nothing, Int] = ZStream.repeat(0)
```

**ZStream.repeatWith** — This is another variant of `repeat`, which repeats according to the provided schedule. For example, the following stream produce zero value every second:

```scala mdoc:silent:nest
import zio.clock._
import zio.duration._
import zio.random._
import zio.Schedule
val repeatZeroEverySecond: ZStream[Clock, Nothing, Int] = 
  ZStream.repeatWith(0, Schedule.spaced(1.seconds))
```

**ZStream.repeatEffect** — Assume we have an effectful API, and we need to call that API and create a stream from the result of that. We can create a stream from that effect that repeats forever.

Let's see an example of creating a stream of random numbers:

```scala mdoc:silent:nest
val randomInts: ZStream[Random, Nothing, Int] =
  ZStream.repeatEffect(zio.random.nextInt)
```

There are some other variant of repetition API like `repeatEffectWith`, `repeatEffectOption`, `repeatEffectChunk` and `repeatEffectChunkOption`.

### From Unfolding

In functional programming, `unfold` is dual to `fold`. 

With `fold` we can process a data structure and build a return value. For example, we can process a `List[Int]` and return the sum of all its elements. 

The `unfold` represents an operation that takes an initial value and generates a recursive data structure, one-piece element at a time by using a given state function. For example, we can create a natural number by using `one` as the initial element and the `inc` function as the state function.

**ZStream.unfold** — `ZStream` has `unfold` function, which is defined as follows:

```scala
object ZStream {
  def unfold[S, A](s: S)(f: S => Option[(A, S)]): ZStream[Any, Nothing, A] = ???
}
```

- **s** — An initial state value
- **f** — A state function `f` that will be applied to the initial state `s`. If the result of this application is `None` the stream will end, otherwise the result is `Some`, so the next element in the stream would be `A` and the current state of transformation changed to the new `S`, this new state is the basis of the next unfold process.

For example, we can a stream of natural numbers using `ZStream.unfold`:

```scala mdoc:silent
val nats: ZStream[Any, Nothing, Int] = ZStream.unfold(0)(n => Some((n, n + 1)))
```

We can write `countdown` function using `unfold`:

```scala mdoc:silent
def countdown(n: Int) = ZStream.unfold(n) {
  case 0 => None
  case s => Some((s, s - 1))
}
```

Running this function with an input value of 3 returns a `ZStream` which contains 3, 2, 1 values.

**ZStream.unfoldM** — `unfoldM` is an effectful version of `unfold`. It helps us to perform _effectful state transformation_ when doing unfold operation.

Let's write a stream of lines of input from a user until the user enters the `exit` command:

```scala mdoc:silent
import java.io.IOException
import zio.console.Console
val inputs: ZStream[Console, IOException, String] = ZStream.unfoldM(()) { _ =>
  zio.console.getStrLn.map {
    case "exit"  => None
    case i => Some((i, ()))
  } 
}   
```

`ZStream.unfoldChunk`, and `ZStream.unfoldChunkM` are other variants of `unfold` operations but for `Chunk` data type.

### From Java IO

**ZStream.fromFile** — Create ZIO Stream from a file:

```scala mdoc:silent:nest
val file: ZStream[Blocking, Throwable, Byte] = 
  ZStream.fromFile(Path.of("file.txt"))
```

**ZStream.fromInputStream** — Creates a stream from a `java.io.InputStream`:

```scala mdoc:silent:nest
val stream: ZStream[Blocking, IOException, Byte] = 
  ZStream.fromInputStream(new FileInputStream("file.txt"))
```

Note that the InputStream will not be explicitly closed after it is exhausted. Use `ZStream.fromInputStreamEffect`, or `ZStream.fromInputStreamManaged` instead.

**ZStream.fromInputStreamEffect** — Creates a stream from a `java.io.InputStream`. Ensures that the InputStream is closed after it is exhausted:

```scala mdoc:silent:nest
val stream: ZStream[Blocking, IOException, Byte] = 
  ZStream.fromInputStreamEffect(
    ZIO.effect(new FileInputStream("file.txt"))
      .refineToOrDie[IOException]
  )
```

**ZStream.fromInputStreamManaged** — Creates a stream from a managed `java.io.InputStream` value:

```scala mdoc:silent:nest
val managed: ZManaged[Any, IOException, FileInputStream] =
  ZManaged.fromAutoCloseable(
    ZIO.effect(new FileInputStream("file.txt"))
  ).refineToOrDie[IOException]

val stream: ZStream[Blocking, IOException, Byte] = 
  ZStream.fromInputStreamManaged(managed)
```

**ZStream.fromResource** — Create a stream from resource file:
```scala mdoc:silent:nest
val stream: ZStream[Blocking, IOException, Byte] =
  ZStream.fromResource("file.txt")
```

**ZStream.fromReader** — Creates a stream from a `java.io.Reader`:

```scala mdoc:silent:nest
val stream: ZStream[Blocking, IOException, Char] = 
   ZStream.fromReader(new FileReader("file.txt"))
```

ZIO Stream also has `ZStream.fromReaderEffect` and `ZStream.fromReaderManaged` variants.

## Transforming a Stream

ZIO Stream supports many standard transforming functions like `map`, `partition`, `grouped`, `groupByKey`, `groupedWithin`
and many others. Here are examples of how to use them.   

### map
```scala mdoc:silent
import zio.stream._

val intStream: Stream[Nothing, Int] = Stream.fromIterable(0 to 100)
val stringStream: Stream[Nothing, String] = intStream.map(_.toString)
```
### partition
`partition` function splits the stream into tuple of streams based on predicate. The first stream contains all
element evaluated to true and the second one contains all element evaluated to false.
The faster stream may advance by up to `buffer` elements further than the slower one. Two streams are 
wrapped by `ZManaged` type. In the example below, left stream consists of even numbers only.

```scala mdoc:silent
import zio._
import zio.stream._

val partitionResult: ZManaged[Any, Nothing, (ZStream[Any, Nothing, Int], ZStream[Any, Nothing, Int])] =
  Stream
    .fromIterable(0 to 100)
    .partition(_ % 2 == 0, buffer = 50)
```

### grouped
To partition the stream results with the specified chunk size, you can use `grouped` function.

```scala mdoc:silent
import zio._
import zio.stream._

val groupedResult: ZStream[Any, Nothing, Chunk[Int]] =
  Stream
    .fromIterable(0 to 100)
    .grouped(50)
```

### groupByKey
To partition the stream by function result you can use `groupByKey` or `groupBy`. In the example below
exam results are grouped into buckets and counted. 

```scala mdoc:silent
import zio._
import zio.stream._

  case class Exam(person: String, score: Int)

  val examResults = Seq(
    Exam("Alex", 64),
    Exam("Michael", 97),
    Exam("Bill", 77),
    Exam("John", 78),
    Exam("Bobby", 71)
  )

  val groupByKeyResult: ZStream[Any, Nothing, (Int, Int)] =
    Stream
      .fromIterable(examResults)
      .groupByKey(exam => exam.score / 10 * 10) {
        case (k, s) => ZStream.fromEffect(s.runCollect.map(l => k -> l.size))
      }
```

### groupedWithin
`groupedWithin` allows to group events by time or chunk size, whichever is satisfied first. In the example below
every chunk consists of 30 elements and is produced every 3 seconds.

```scala mdoc:silent
import zio._
import zio.stream._
import zio.duration._
import zio.clock.Clock

val groupedWithinResult: ZStream[Any with Clock, Nothing, Chunk[Int]] =
  Stream.fromIterable(0 to 10)
    .repeat(Schedule.spaced(1.seconds))
    .groupedWithin(30, 10.seconds)
```

## Consuming a Stream

```scala mdoc:silent
import zio._
import zio.console._
import zio.stream._

val result: RIO[Console, Unit] = Stream.fromIterable(0 to 100).foreach(i => putStrLn(i.toString))
```

### Using a Sink

A `Sink[E, A0, A, B]` consumes values of type `A`, ultimately producing
either an error of type `E`, or a value of type `B` together with a remainder
of type `A0`.

You can for example reduce a `Stream` to a `ZIO` value using `Sink.foldLeft` : 

```scala mdoc:silent
import zio._
import zio.stream._

def streamReduce(total: Int, element: Int): Int = total + element
val resultFromSink: UIO[Int] = Stream(1,2,3).run(Sink.foldLeft(0)(streamReduce))
```

## Working on several streams

You can merge several streams using the `merge` method :

```scala mdoc:silent
import zio.stream._

val merged: Stream[Nothing, Int] = Stream(1,2,3).merge(Stream(2,3,4))
```

Or zipping streams : 

```scala mdoc:silent
import zio.stream._

val zippedStream: Stream[Nothing, (Int, Int)] = Stream(1,2,3).zip(Stream(2,3,4))
```

Then you would be able to reduce the stream into to a `ZIO` value : 

```scala mdoc:silent
import zio._

def tupleStreamReduce(total: Int, element: (Int, Int)) = {
  val (a,b) = element
  total + (a + b)
} 

val reducedResult: UIO[Int] = zippedStream.run(Sink.foldLeft(0)(tupleStreamReduce))
```

## Compressed streams

### Decompression

If you read `Content-Encoding: deflate`, `Content-Encoding: gzip` or streams other such streams of compressed data, following transducers can be helpful:
* `inflate` transducer allows to decompress stream of _deflated_ inputs, according to [RFC 1951](https://tools.ietf.org/html/rfc1951).
* `gunzip` transducer can be used to decompress stream of _gzipped_ inputs, according to [RFC 1952](https://tools.ietf.org/html/rfc1952).

Both decompression methods will fail with `CompressionException` when input wasn't properly compressed.

```scala mdoc:silent
import zio.stream.ZStream
import zio.stream.Transducer.{ gunzip, inflate }
import zio.stream.compression.CompressionException

def decompressDeflated(deflated: ZStream[Any, Nothing, Byte]): ZStream[Any, CompressionException, Byte] = {
  val bufferSize: Int = 64 * 1024 // Internal buffer size. Few times bigger than upstream chunks should work well.
  val noWrap: Boolean = false     // For HTTP Content-Encoding should be false.
  deflated.transduce(inflate(bufferSize, noWrap))
}

def decompressGzipped(gzipped: ZStream[Any, Nothing, Byte]): ZStream[Any, CompressionException, Byte] = {
  val bufferSize: Int = 64 * 1024 // Internal buffer size. Few times bigger than upstream chunks should work well.
  gzipped.transduce(gunzip(bufferSize))
}

```

### Compression

The `deflate` transducer compresses a stream of bytes as specified by [RFC 1951](https://tools.ietf.org/html/rfc1951).

```scala mdoc:silent
import zio.stream.ZStream
import zio.stream.Transducer.deflate
import zio.stream.compression.{CompressionLevel, CompressionStrategy, FlushMode}

def compressWithDeflate(clearText: ZStream[Any, Nothing, Byte]): ZStream[Any, Nothing, Byte] = {
  val bufferSize: Int = 64 * 1024 // Internal buffer size. Few times bigger than upstream chunks should work well.
  val noWrap: Boolean = false // For HTTP Content-Encoding should be false.
  val level: CompressionLevel = CompressionLevel.DefaultCompression
  val strategy: CompressionStrategy = CompressionStrategy.DefaultStrategy
  val flushMode: FlushMode = FlushMode.NoFlush
  clearText.transduce(deflate(bufferSize, noWrap, level, strategy, flushMode))
}

def deflateWithDefaultParameters(clearText: ZStream[Any, Nothing, Byte]): ZStream[Any, Nothing, Byte] =
  clearText.transduce(deflate())
```