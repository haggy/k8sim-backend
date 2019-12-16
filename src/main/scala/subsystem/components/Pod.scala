package subsystem.components

import java.util.UUID

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import subsystem.SubsystemManager
import subsystem.SubsystemManager.SubsystemManagerConfig
import subsystem.simulation.StatsCollector
import subsystem.util.AkkaUtils.NeedsReply

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object Pod {
  type PodId = UUID
  final case class PodMeta(id: PodId)

  final case class WorkloadConfig(tickInterval: FiniteDuration = 100.millis)
  final case class PodConfig(subsystemManagerConfig: SubsystemManagerConfig, workload: WorkloadConfig = WorkloadConfig())
  sealed trait PodCommand
  sealed trait PodEvent

  final case class StartPod(config: PodConfig, statsCollector: ActorRef[StatsCollector.StatsCollectorCommand], replyTo: ActorRef[PodEvent]) extends PodCommand with NeedsReply[PodEvent]
  final case class PodStarted(meta: PodMeta) extends PodEvent
  final case class PodStartupFailed(meta: PodMeta, cause: Throwable) extends PodEvent
  final case class GetStatus(replyTo: ActorRef[PodEvent]) extends PodCommand with NeedsReply[PodEvent]
  final case class CurrentPodStatus(status: PodStatus) extends PodEvent

  private final case class HandleSubsytemStartupComplete(replyTo: ActorRef[PodEvent]) extends PodCommand with NeedsReply[PodEvent]
  private final case class HandleSubsytemStartupFailed(replyTo: ActorRef[PodEvent], cause: Throwable) extends PodCommand with NeedsReply[PodEvent]

  final case class StartWorkload(workloadConfig: ContainerWorkload.NewContainerWorkloadConfig, replyTo: ActorRef[PodEvent]) extends PodCommand with NeedsReply[PodEvent]
  final case class StopWorkload(id: ContainerWorkload.WorkloadId, replyTo: ActorRef[PodEvent]) extends PodCommand with NeedsReply[PodEvent]
  final case object WorkloadStopped extends PodEvent
  final case class WorkloadNotFound(id: ContainerWorkload.WorkloadId) extends PodEvent

  final case class WorkloadStarted(meta: ContainerWorkload.WorkloadMeta) extends PodEvent
  final case class WorkloadStartupFailure(cause: Throwable) extends PodEvent

  private final case class HandleContainerWorkloadEvent(ev: ContainerWorkload.ContainerWorkloadEvent,
                                                        workloadRef: ActorRef[ContainerWorkload.ContainerWorkloadCommand],
                                                        originalRequester: ActorRef[PodEvent]) extends PodCommand
  private final case class HandleContainerWorkloadFailure(cause: Throwable,
                                                          workloadRef: ActorRef[ContainerWorkload.ContainerWorkloadCommand],
                                                          originalRequester: ActorRef[PodEvent]) extends PodCommand

  sealed trait PodStatus extends PodEvent{ val asString: String}
  final case object Stopped extends PodStatus { val asString = "stopped" }
  final case object Running extends PodStatus { val asString = "running" }
  final case object Failed extends PodStatus { val asString = "failed" }

  private final case class WorkloadTracker(private val activeWorkloads: Map[ContainerWorkload.WorkloadId, ActorRef[ContainerWorkload.ContainerWorkloadCommand]] = Map.empty) {
    def add(id: ContainerWorkload.WorkloadId, ref: ActorRef[ContainerWorkload.ContainerWorkloadCommand]): WorkloadTracker =
      copy(activeWorkloads + (id -> ref))

    def get(id: ContainerWorkload.WorkloadId): Option[ActorRef[ContainerWorkload.ContainerWorkloadCommand]] = activeWorkloads.get(id)

    def remove(id: ContainerWorkload.WorkloadId): WorkloadTracker =
      copy(activeWorkloads - id)
  }

  def apply()(implicit askTimeout: Timeout): Behavior[PodCommand] = Behaviors.receive { (context, message) =>
    message match {
      case StartPod(conf, statsCollector, originalSender) =>

        val myId = UUID.randomUUID()
        val meta = PodMeta(myId)
        context.log.info("Starting Pod with ID [{}]", myId)

        val subsysMgr = context.spawn(SubsystemManager(), "subsystemManager")

        context.ask(
          subsysMgr, (ref: ActorRef[SubsystemManager.SubSysEvent]) => SubsystemManager.Start(conf.subsystemManagerConfig, ref)) {
          case Success(SubsystemManager.InitSucceeded) => HandleSubsytemStartupComplete(originalSender)
          case Success(SubsystemManager.InitFailed(t)) => HandleSubsytemStartupFailed(originalSender, t)
          case Failure(t) => HandleSubsytemStartupFailed(originalSender, t)
        }

        Behaviors.receiveMessagePartial {
          case HandleSubsytemStartupComplete(replyTo) =>
            context.log.info("Pod running with ID [{}]", meta.id)
            replyTo ! PodStarted(meta)
            podRunning(meta, conf, WorkloadTracker(), subsysMgr, statsCollector)

          case HandleSubsytemStartupFailed(replyTo, cause) =>
            replyTo ! PodStartupFailed(meta, cause)
            Behaviors.stopped
        }

      case GetStatus(replyTo) =>
        replyTo ! Stopped
        Behaviors.same

      case cmd =>
        context.log.warn("Received command that is invalid while stopped: [{}]", cmd)
        Behaviors.same
    }
  }

  private def podRunning(selfMeta: PodMeta,
                         config: PodConfig,
                         workloadTracker: WorkloadTracker,
                         subsystemMgr: ActorRef[SubsystemManager.SubSysCommand],
                         statsCollector: ActorRef[StatsCollector.StatsCollectorCommand])(implicit askTimeout: Timeout): Behavior[PodCommand] = Behaviors.setup { context =>
    Behaviors.receiveMessagePartial {
      case GetStatus(replyTo) =>
        replyTo ! Running
        Behaviors.same

      case StartWorkload(workloadConfig, replyTo) =>
        val workload = context.spawnAnonymous(ContainerWorkload(ContainerWorkload.WorkloadInternalsConfig(
          context.self,
          subsystemMgr,
          askTimeout,
          statsCollector,
          config.workload.tickInterval
        )))
        context.ask(workload, (ref: ActorRef[ContainerWorkload.ContainerWorkloadEvent]) => ContainerWorkload.StartWorkload(workloadConfig, ref)) {
          case Success(ev) => HandleContainerWorkloadEvent(ev, workload, replyTo)
          case Failure(t) => HandleContainerWorkloadFailure(t, workload, replyTo)
        }
        Behaviors.same

      case HandleContainerWorkloadEvent(ContainerWorkload.WorkloadStarted(wlMeta), workloadRef, replyTo) =>
        replyTo ! WorkloadStarted(wlMeta)
        podRunning(selfMeta, config, workloadTracker.add(wlMeta.id, workloadRef), subsystemMgr, statsCollector)

      case HandleContainerWorkloadFailure(cause, workloadRef, replyTo) =>
        context.stop(workloadRef)
        replyTo ! WorkloadStartupFailure(cause)
        Behaviors.same

      case StopWorkload(id, replyTo) =>
        val event = workloadTracker.get(id).fold(WorkloadNotFound(id): PodEvent) { workloadRef =>
          workloadRef ! ContainerWorkload.StopWorkload
          WorkloadStopped
        }
        replyTo ! event
        podRunning(selfMeta, config, workloadTracker.remove(id), subsystemMgr, statsCollector)
    }
  }
}
