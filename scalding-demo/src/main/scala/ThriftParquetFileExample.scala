/**
  * Copyright (c) 2014 XiaoMi Inc. All Rights Reserved.
  * Authors: du00 <duninglin@xiaomi.com>
  */

import parquet.thrift.test.Name
import com.twitter.scalding._
import com.twitter.scalding.parquet.thrift.FixedPathParquetThrift


class ThriftParquetFileExample(args: Args) extends Job(args) {

  val source = new FixedPathParquetThrift[Name](args("input"))
  val sink = new FixedPathParquetThrift[Name](args("output"))

  TypedPipe.from(source)
    .map { name => println(name); name }
    .write(sink)

  /**
    * Do not sepcify a compression codec, o
    * parquet.hadoop.codec.CodecConfig: codec defined in hadoop config is not supported by parquet
    * [org.apache.hadoop.io.compress.DefaultCodec] and will use UNCOMPRESSED
    *
    * SNAPPY config refer to http://comments.gmane.org/gmane.comp.cloud.hadoop.scalding.devel/77
    *
    */
  override def config: Map[AnyRef, AnyRef] =
    super.config ++ Map(
      // JOB OUTPUT
      "mapred.output.fileoutputformat.compress" -> "true",
      "mapred.output.fileoutputformat.compress.codec" -> "parquet.hadoop.codec.SnappyCodec",
      "mapred.output.fileoutputformat.compress.type" -> "BLOCK",
      "mapred.output.compression.type" -> "BLOCK",
      "mapred.output.compress" -> "true",
      "mapred.output.compression.codec" -> "parquet.hadoop.codec.SnappyCodec",
      // MAP OUTPUT
      "mapred.map.output.compress" -> "true",
      "mapred.map.output.compress.codec" -> "parquet.hadoop.codec.SnappyCodec"
    )
}
