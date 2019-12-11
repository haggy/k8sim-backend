package subsystem.storage.hdd

import java.util.concurrent.LinkedBlockingQueue

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import subsystem.storage.StorageInterface._
import subsystem.workqueue.SimpleWorkQueue
import subsystem.workqueue.SimpleWorkQueue._

import scala.concurrent.duration._



object SpindleBasedStorage {

  final case class SpindleBasedConfig(processingFrequency: Frequency = Frequency(5, 250.millis), requestCapacity: Int = 25)

  trait NeedsReply[T] {
    val replyTo: ActorRef[T]
  }
  private sealed trait SpindleStorageOp extends StorageOp with NeedsReply[StorageEvent]

  private final case class WriteBlock(path: StoragePath, data: Array[Byte], replyTo: ActorRef[StorageEvent]) extends SpindleStorageOp {
    override protected val cost: Int = 10
  }
  private final case class ReadBlock(path: StoragePath, replyTo: ActorRef[StorageEvent]) extends SpindleStorageOp {
    override protected val cost: Int = 7
  }

  def apply(conf: SpindleBasedConfig): Behavior[StorageCommand] = behavior(conf, new LinkedBlockingQueue[SpindleStorageOp]())

  private def behavior(config: SpindleBasedConfig, workQueue: LinkedBlockingQueue[SpindleStorageOp]): Behavior[StorageCommand] =
    Behaviors.setup { context =>

      val workQueue = context.spawnAnonymous(SimpleWorkQueue[SpindleStorageOp](WorkQueueConfig(Some(config.requestCapacity), config.processingFrequency)))
      val workQueueEventHandler = context.spawnAnonymous(workQueueEventHandlerBehavior())

      Behaviors.receiveMessage {
        case wd: WriteData =>
          val writeBlock = WriteBlock(wd.path, wd.data, wd.replyTo)
          context.log.debug("Enqueing write at [{}] with cost [{}]", wd.path.key, writeBlock.getCost)
          workQueue ! PushWork(writeBlock, writeBlock.getCost, workQueueEventHandler)
          Behaviors.same
        case rd: ReadData =>
          val readBlock = ReadBlock(rd.path, rd.replyTo)
          context.log.debug("Enqueing read at [{}] with cost [{}]", rd.path.key, readBlock.getCost)
          workQueue ! PushWork(readBlock, readBlock.getCost, workQueueEventHandler)
          Behaviors.same
      }
    }

  private def workQueueEventHandlerBehavior(): Behavior[WorkQueueEvent] =
    Behaviors.receiveMessagePartial[WorkQueueEvent] {
      case WorkReady(elem) => elem match {
        case PushWork(work: ReadBlock, _, _) =>
          work.replyTo ! DataRetrieved(work.path, Array.emptyByteArray)
          Behaviors.same
        case PushWork(work: WriteBlock, _, _) =>
          work.replyTo ! DataWritten(work.path)
          Behaviors.same
      }
      case QueueFull(work: ReadBlock) =>
        work.replyTo ! DataReadFailed(work.path, "Disk Saturated")
        Behaviors.same
      case QueueFull(work: WriteBlock) =>
        work.replyTo ! DataWriteFailed(work.path, "Disk Saturated")
        Behaviors.same
      case _ => Behaviors.same
    }
}
