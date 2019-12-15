package subsystem.components

import java.util.UUID

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import subsystem.SubsystemManager
import subsystem.storage.StorageInterface
import subsystem.util.AkkaUtils.NeedsReply

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object ContainerWorkload {
  type WorkloadId = UUID
  final case class WorkloadMeta(id: WorkloadId)
  final case class WorkloadInternalsConfig(myPodRef: ActorRef[Pod.PodCommand],
                                           subsysMgr: ActorRef[SubsystemManager.SubSysCommand],
                                           askTimeout: Timeout,
                                           tickInterval: FiniteDuration = 100.millis)
  final case class NewContainerWorkloadConfig()

  sealed trait ContainerWorkloadCommand
  sealed trait ContainerWorkloadEvent

  final case class StartWorkload(conf: NewContainerWorkloadConfig, replyTo: ActorRef[ContainerWorkloadEvent]) extends ContainerWorkloadCommand with NeedsReply[ContainerWorkloadEvent]
  final case class WorkloadStarted(meta: WorkloadMeta) extends ContainerWorkloadEvent
  final case object StopWorkload extends ContainerWorkloadCommand

  private final case class HandleStorageSubsysEvent(ev: StorageInterface.StorageEvent) extends ContainerWorkloadCommand
  private final case class HandleStorageSubsysFailure(cause: Throwable) extends ContainerWorkloadCommand

  private final case object Tick extends ContainerWorkloadCommand

  def apply(config: WorkloadInternalsConfig): Behavior[ContainerWorkloadCommand] = initialBehavior(config)

  private def initialBehavior(internalConf: WorkloadInternalsConfig): Behavior[ContainerWorkloadCommand] = Behaviors.receive { (context, message) =>
    message match {
      case StartWorkload(newConf, replyTo) =>
        val myId = UUID.randomUUID()
        context.log.info("Starting workload with ID [{}]", myId)
        replyTo ! WorkloadStarted(WorkloadMeta(myId))
        running(myId, internalConf, newConf)
    }
  }

  private def running(myId: UUID,
                      internalConf: WorkloadInternalsConfig,
                      config: NewContainerWorkloadConfig): Behavior[ContainerWorkloadCommand] = Behaviors.setup { context =>
    val subsystemManager = internalConf.subsysMgr
    val myPod = internalConf.myPodRef
    implicit val askTimeout: Timeout = internalConf.askTimeout

    Behaviors.withTimers[ContainerWorkloadCommand] { timerCtx =>
      timerCtx.startTimerWithFixedDelay(myId, Tick, internalConf.tickInterval)

      Behaviors.receiveMessagePartial {
        case Tick =>
          context.ask(subsystemManager, (ref: ActorRef[StorageInterface.StorageEvent]) =>  SubsystemManager.DiskSubsystemRequest(StorageInterface.ReadData(
            StorageInterface.StoragePath("/dev/null"),
            ref
          ))) {
            case Success(ev) => HandleStorageSubsysEvent(ev)
            case Failure(t) => HandleStorageSubsysFailure(t)
          }
          Behaviors.same

        case HandleStorageSubsysEvent(ev) =>
          context.log.info("Received event from storage subsystem: [{}]", ev)
          Behaviors.same

        case HandleStorageSubsysFailure(cause) =>
          context.log.error("Received error from storage subsystem", cause)
          Behaviors.same

        case StopWorkload => Behaviors.stopped
      }
    }
  }
}
