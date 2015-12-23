import org.apache.hadoop.io
import org.apache.hadoop.io.{BytesWritable, Text}

import cascading.tap.SinkMode
import com.twitter.algebird.{Aggregator, Semigroup}
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedPipe

class SequenceFileExample(args: Args) extends Job(args) {
  /**
    * override configurations to customize compressions
    */
  override def config: Map[AnyRef, AnyRef] =
    super.config ++ Map("mapred.output.fileoutputformat.compress" -> "true",
      "mapred.output.fileoutputformat.compress.type" -> "BLOCK",
      "mapred.output.compression.type" -> "BLOCK",
      "mapred.output.compress" -> "true",
      "mapred.map.output.compress" -> "true"
    )

  object Agg extends Aggregator[(String, Int), Int, Int] {
    def prepare(v: (String, Int)) = v._2

    val semigroup = Semigroup.from { (l: Int, r: Int) => l + r }

    def present(v: Int) = v
  }


  TypedPipe.from(WritableSequenceFile[BytesWritable, BytesWritable](args("input")))
    .flatMap {
      case (k, v) =>
        val line = new String(v.getBytes, 0, v.getLength)
        line.split("\\s").map(w => (w, 2))
    }
    .groupBy(_._1).aggregate(Agg) // aggregate by key
    .toTypedPipe
    .map { case (k, v) => (new Text(k), new io.IntWritable(v)) }

    // reduce is a special case of aggragator, aggragator[A, B, C], reduce = aggragator[T, T, T]
    // .reduce((a, b) => (a._1, a._2 + b._2))
    // .toTypedPipe
    // .map { case (k, (_, v)) => println(s"$k, $v"); (new Text(k), new io.IntWritable(v)) }

    // adjust SinkMode to choose write override or write fail, keep -> write fail on exist, relapce => override on exist
    .write(WritableSequenceFile(args("output"), Dsl.intFields(0 to 1), sinkMode = SinkMode.KEEP))
}
