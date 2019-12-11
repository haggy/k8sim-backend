package subsystem.storage

import akka.actor.typed.ActorRef

import scala.concurrent.duration.FiniteDuration

object StorageInterface {

  case class Frequency(perTick: Int, tickSleepDuration: FiniteDuration)

  final case class BlockAddress(pointer: Long)

  final case class StoragePath(key: String) {
    lazy val address = BlockAddress(key.hashCode) // This can have collisions. Let's not care.. for now
  }

  trait StorageOp {
    protected val cost: Int
    def getCost: Int = cost
  }

  trait StorageCommand
  trait StorageEvent

  final case class WriteData(path: StoragePath, data: Array[Byte], replyTo: ActorRef[StorageEvent]) extends StorageCommand
  final case class DataWritten(path: StoragePath) extends StorageEvent
  final case class DataWriteFailed(path: StoragePath, reason: String) extends StorageEvent

  final case class ReadData(path: StoragePath, replyTo: ActorRef[StorageEvent]) extends StorageCommand
  final case class DataRetrieved(path: StoragePath, data: Array[Byte]) extends StorageEvent
  final case class DataReadFailed(path: StoragePath, reason: String) extends StorageEvent
}
trait StorageInterface {

}
