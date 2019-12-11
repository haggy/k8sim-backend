package subsystem

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import subsystem.storage.StorageInterface.StorageCommand
import subsystem.storage.hdd.SpindleBasedStorage

object SubsystemManager {
  final case class SubsystemManagerConfig(disk: SpindleBasedStorage.SpindleBasedConfig)

  sealed trait SubSysCommand
  sealed trait SubSysEvent

  final case class Start(config: SubsystemManagerConfig, replyTo: ActorRef[SubSysEvent]) extends SubSysCommand
  final case object InitSucceeded extends SubSysEvent
  final case class InitFailed(cause: Throwable) extends SubSysEvent

  final case object Shutdown extends SubSysCommand

  final case class DiskSubsystemRequest(cmd: StorageCommand) extends SubSysCommand

  def apply(): Behavior[SubsystemManager.SubSysCommand] = notStarted()

  private def notStarted(): Behavior[SubsystemManager.SubSysCommand] =
    Behaviors.setup { context =>

      Behaviors.receiveMessage {
        case Start(config, replyTo) =>
          context.log.info("Starting subsystem manager")
          replyTo ! InitSucceeded
          running(
            config,
            context.spawn(SpindleBasedStorage(config.disk), "diskSubsystem")
          )

        case _ =>
          context.log.warn("Subsystem Manager has not been started. Ignoring command")
          Behaviors.same
      }
    }

  private def running(config: SubsystemManagerConfig, diskSubsystem: ActorRef[StorageCommand]): Behavior[SubSysCommand] =
    Behaviors.receive { (context, message) =>
      message match {
        case Shutdown =>
          context.log.info("Shutting down subsystem manager")
          Behaviors.stopped
        case _: Start =>
          context.log.warn("Received event to start subsystem but it is already started")
          Behaviors.same

        case diskReq: DiskSubsystemRequest =>
          diskSubsystem ! diskReq.cmd
          Behaviors.same
      }
    }
}
