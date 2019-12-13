package subsystem.storage.hdd

import java.util.concurrent.LinkedBlockingQueue

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import subsystem.storage.StorageInterface._
import subsystem.util.AkkaUtils.NeedsReply
import subsystem.workqueue.SimpleWorkQueue
import subsystem.workqueue.SimpleWorkQueue._
import subsystem.workqueue.models.{PushWork, QueueFull, WorkQueueEvent, WorkReady}

import scala.concurrent.duration._

object GenericStorageDrive {

  final case class Costs(write: Int, read: Int)
  final case class GenericStorageDriveConfig(processingFrequency: Frequency = Frequency(50, 250.millis),
                                             requestCapacity: Int = 200,
                                             costs: Costs = Costs(100, 7)
                                            )

  private sealed trait GenericStorageDriveOp extends StorageOp with NeedsReply[StorageEvent]

  private final case class WriteBlock(path: StoragePath, data: Array[Byte], cost: Int, replyTo: ActorRef[StorageEvent]) extends GenericStorageDriveOp
  private final case class ReadBlock(path: StoragePath, cost: Int, replyTo: ActorRef[StorageEvent]) extends GenericStorageDriveOp

  def apply(conf: GenericStorageDriveConfig): Behavior[StorageCommand] = behavior(conf, new LinkedBlockingQueue[GenericStorageDriveOp]())

  private def behavior(config: GenericStorageDriveConfig, workQueue: LinkedBlockingQueue[GenericStorageDriveOp]): Behavior[StorageCommand] =
    Behaviors.setup { context =>

      val workQueue = context.spawnAnonymous(SimpleWorkQueue[GenericStorageDriveOp](SimpleWorkQueueConfig(Some(config.requestCapacity), config.processingFrequency)))
      val workQueueEventHandler = context.spawnAnonymous(workQueueEventHandlerBehavior())

      Behaviors.receiveMessage {
        case wd: WriteData =>
          val writeBlock = WriteBlock(wd.path, wd.data, config.costs.write, wd.replyTo)
          context.log.debug("Enqueing write at [{}] with cost [{}]", wd.path.key, writeBlock.getCost)
          workQueue ! PushWork(writeBlock, writeBlock.getCost, workQueueEventHandler)
          Behaviors.same
        case rd: ReadData =>
          val readBlock = ReadBlock(rd.path, config.costs.read, rd.replyTo)
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
