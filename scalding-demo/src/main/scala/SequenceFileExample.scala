import org.apache.hadoop.io.{LongWritable, Text, IntWritable, BytesWritable}

import cascading.tap.SinkMode
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding._

/**
  * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved.
  * Authors: du00 <duninglin@xiaomi.com>
  */

/**
  * Created by du00 on 15-12-23.
  */
class SequenceFileExample(args: Args) extends Job(args) {
  // customize sink mode to override fail
  val sink = WritableSequenceFile[Text, LongWritable](args("output"), Dsl.intFields(0 to 1), sinkMode = SinkMode.KEEP)

  TypedPipe.from(WritableSequenceFile[BytesWritable, BytesWritable](args("input")))
    .flatMap { case (_, value) =>
      new String(value.getBytes, 0, value.getLength).split("\\s")
    }
    .groupBy(w => w).size
    .toTypedPipe
    .map { case (w, freq) => (new Text(w), new LongWritable(freq)) }
    //  .write(WritableSequenceFile[Text, LongWritable](args("output")))
    .write(sink)
}

object SequenceFileExample extends App {
  Tool.main(getClass.getCanonicalName.stripSuffix("$") +: args)
}
