import com.twitter.scalding._

/**
  * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved.
  * Authors: du00 <duninglin@xiaomi.com>
  */

class TextFileExample(args: Args) extends Job(args) {
  TypedPipe.from(TextLine(args("input")))
    .flatMap(line => line.split("\\s"))
    .groupBy(w => w)
    .size
    .write(TypedTsv[(String, Long)](args("output")))
}

object TextFileExample extends App {
  Tool.main(getClass.getCanonicalName.stripSuffix("$") +: args)
}
