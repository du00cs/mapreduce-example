import cascading.flow.{Flow, FlowListener}
import com.twitter.scalding._

class TextFileExample(args: Args) extends Job(args) {
  val key = StatKey("word", "udc")
  val stat = Stat(key)

  TypedPipe.from(TextLine(args("input")))
    .flatMap(line => line.split("\\s"))
    .map { word => stat.inc; (word, 1L) }
    .sumByKey // reduce num not set
    .write(TypedTsv[(String, Long)](args("output")))

  /**
    * "copy" from https://itellity.wordpress.com/2014/10/29/counters-using-cascading-flow-listeners-in-scalding/
    */
  override def listeners = super.listeners ++ List(new FlowListener {
    override def onStarting(flow: Flow[_]): Unit = {}

    override def onCompleted(flow: Flow[_]) {
      try {
        val fs = flow.getFlowStats
        println(key.group, key.counter, fs.getCounterValue(key.group, key.counter))
//        fs.getCounterGroups.foreach { group =>
//          fs.getCountersFor(group).foreach { counter =>
//            println(group + "::" + counter + ":" + fs.getCounterValue(group, counter))
//          }
//        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

    override def onThrowable(flow: Flow[_], e: Throwable): Boolean = {
      e.printStackTrace()
      true
    }

    override def onStopping(flow: Flow[_]): Unit = {}
  })
}

// object TextFileExample extends App {
//   Tool.main(getClass.getCanonicalName.stripSuffix("$") +: args)
// }
