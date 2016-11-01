package org.apache.flink.runtime.progress

import java.io.PrintWriter

import akka.actor.Actor
import org.apache.flink.runtime.progress.messages.ProgressMetricsReport
import org.slf4j.{Logger, LoggerFactory}

class ProgressMetricsLogger(numWindows: Int, parallelism: Int, winSize: Long, outputDir: String) extends Actor {
  
  private var LOG: Logger = LoggerFactory.getLogger(classOf[ProgressMetricsLogger])

  val printWriter = new PrintWriter(s"$outputDir/$numWindows-$parallelism-$winSize-${System.currentTimeMillis()}.csv")
  printWriter.println("num_Windows,parallelism,win_Size,ctxid,operatorID,instanceID,window_Start,local_End,window_End")

  print(self)

  def receive(): Receive = {
    case report: ProgressMetricsReport =>
      printWriter.println(s"$numWindows,$parallelism,$winSize,${report.context.get(0)},${report.operatorId}," +
        s"${report.instanceId},${report.startTS},${report.localEndTS},${report.endTS}")
      printWriter.flush();
  }

  override def aroundPostStop(): Unit = {
    printWriter.flush()
    printWriter.close()
    super.aroundPostStop()
  }

}
