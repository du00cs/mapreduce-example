import org.apache.spark.{SparkContext, SparkConf}

/**
  * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved.
  * Authors: du00 <duninglin@xiaomi.com>
  */

/**
  * Created by du00 on 15-12-23.
  */
object GenerateFiles {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app")
      .setMaster("local[1]")

    val sc = new SparkContext(conf)

    val lines = List("a b c", "a b", "a")

    sc.parallelize(lines.map(s => (Array[Byte](), s.getBytes)))
      .saveAsSequenceFile("seq")

    sc.stop()
  }
}
