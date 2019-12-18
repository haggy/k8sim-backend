package subsystem.simulation

import java.util.UUID

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import subsystem.components.{ContainerWorkload, Pod}
import subsystem.util.AkkaUtils.NeedsReply

import scala.util.{Failure, Success}
import scala.concurrent.duration._

object SimulationManager {
  type SimulationId = UUID
  final case class SimManagerMeta(id: SimulationId)

  final case class SimulationConfig(messageTimeout: Timeout = Timeout(3.seconds))

  sealed trait SimManagerCommand
  sealed trait SimManagerEvent

  final case class StartSimulationManager(config: SimulationConfig, replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]
  final case class SimulationStarted(meta: SimManagerMeta) extends SimManagerEvent
  final case class CreatePod(config: Pod.PodConfig, replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]
  final case class PodCreated(meta: Pod.PodMeta) extends SimManagerEvent
  final case class PodCreateFailed(meta: Option[Pod.PodMeta], cause: Throwable) extends SimManagerEvent
  final case object EndSimulation extends SimManagerCommand
  final case class PodNotFound(id: Pod.PodId) extends SimManagerEvent

  private final case class HandlePodCreated(meta: Pod.PodMeta, podRef: ActorRef[Pod.PodCommand], replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]
  private final case class HandlePodCreateFailure(meta: Pod.PodMeta, cause: Throwable, replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]
  private final case class HandleUnknownPodCreateFailure(cause: Throwable, replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]

  final case class StartPodWorkload(podId: Pod.PodId, config: ContainerWorkload.NewContainerWorkloadConfig, replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]
  final case class StopPodWorkload(podId: Pod.PodId, workloadId: ContainerWorkload.WorkloadId, replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]
  final case class PodWorkloadStarted(meta: ContainerWorkload.WorkloadMeta, workloadIds: List[ContainerWorkload.WorkloadId]) extends SimManagerEvent
  final case object PodWorkloadStopped extends SimManagerEvent
  final case class PodWorkloadStartFailed(cause: Throwable) extends SimManagerEvent
  private final case class HandlePodWorkloadEvent(ev: Pod.PodEvent, originalRequester: ActorRef[SimManagerEvent]) extends SimManagerCommand
  private final case class HandlePodWorkloadFailure(cause: Throwable, originalRequester: ActorRef[SimManagerEvent]) extends SimManagerCommand

  final case class GetAllStatistics(replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]
  final case class RetrievedStatistics(allStats: StatsCollector.AllStatsFlushed) extends SimManagerEvent
  final case class GetStatisticsFailed(cause: Throwable) extends SimManagerEvent

  private final case class HandleStatsEvent(ev: StatsCollector.StatsCollectorEvent, originalRequester: ActorRef[SimManagerEvent]) extends SimManagerCommand
  private final case class HandleStatsError(cause: Throwable, originalRequester: ActorRef[SimManagerEvent]) extends SimManagerCommand

  private final case class ComponentTracker(private val pods: Map[Pod.PodId, ActorRef[Pod.PodCommand]] = Map.empty) {
    def addPod(id: Pod.PodId, newPod: ActorRef[Pod.PodCommand]): ComponentTracker = copy(pods + (id -> newPod))
    def getPod(id: Pod.PodId): Option[ActorRef[Pod.PodCommand]] = pods.get(id)
    def removePod(id: Pod.PodId): ComponentTracker = copy(pods - id)
  }

  private val ignoringBehaviorWithSelfKill: Behavior[Pod.PodEvent] = Behaviors.receiveMessagePartial {
    case _ => Behaviors.stopped
  }

  def apply(): Behavior[SimManagerCommand] = initialBehavior

  private def initialBehavior: Behavior[SimManagerCommand] = Behaviors.receive { (context, message) =>
    message match {
      case StartSimulationManager(config, replyTo) =>
        val meta = SimManagerMeta(UUID.randomUUID())
        val statsCollector = context.spawnAnonymous(StatsCollector())
        context.log.info("Starting simulation manager with ID [{}]", meta.id)
        replyTo ! SimulationStarted(meta)
        running(config, meta, ComponentTracker(), statsCollector)

      case msg =>
        context.log.warn("Ignoring message since Simulation has not been started: [{}]", msg)
        Behaviors.same
    }
  }

  private def running(config: SimulationConfig,
                      simMeta: SimManagerMeta,
                      compTracker: ComponentTracker,
                      statsCollector: ActorRef[StatsCollector.StatsCollectorCommand]): Behavior[SimManagerCommand] = Behaviors.setup { context =>
    implicit val askTimeout: Timeout = config.messageTimeout

    Behaviors.receiveMessagePartial {
      case CreatePod(podConf, originalSender) =>
        val pod = context.spawnAnonymous(Pod())
        context.ask(pod, (ref: ActorRef[Pod.PodEvent]) =>  Pod.StartPod(podConf, statsCollector, ref)) {
          case Success(started: Pod.PodStarted) => HandlePodCreated(started.meta, pod, originalSender)
          case Success(failed: Pod.PodStartupFailed) => HandlePodCreateFailure(failed.meta, failed.cause, originalSender)
          case Success(other) => HandleUnknownPodCreateFailure(new Exception(s"Unknown response received: $other"), originalSender)
          case Failure(t) => HandleUnknownPodCreateFailure(t, originalSender)
        }
        Behaviors.same

      case HandlePodCreated(podMeta, podRef, replyTo) =>
        replyTo ! PodCreated(podMeta)
        running(config, simMeta, compTracker.addPod(podMeta.id, podRef), statsCollector)

      case HandlePodCreateFailure(meta, cause, replyTo) =>
        replyTo ! PodCreateFailed(Some(meta), cause)
        Behaviors.same
      case HandleUnknownPodCreateFailure(cause, replyTo) =>
        replyTo ! PodCreateFailed(None, cause)
        Behaviors.same

      case StartPodWorkload(podId, workloadConfig, replyTo) =>
        compTracker.getPod(podId) match {
          case None => replyTo ! PodNotFound(podId)
          case Some(podRef) =>
            context.ask(podRef, (ref: ActorRef[Pod.PodEvent]) => Pod.StartWorkload(workloadConfig, ref)) {
              case Success(ev) => HandlePodWorkloadEvent(ev, replyTo)
              case Failure(t) => HandlePodWorkloadFailure(t, replyTo)
            }
        }
        Behaviors.same

      case StopPodWorkload(podId, workloadId, replyTo) =>
        compTracker.getPod(podId) match {
          case None => replyTo ! PodNotFound(podId)
          case Some(podRef) =>
            podRef ! Pod.StopWorkload(workloadId, context.spawnAnonymous(ignoringBehaviorWithSelfKill))
            replyTo ! PodWorkloadStopped
        }
        Behaviors.same

      case HandlePodWorkloadEvent(Pod.WorkloadStarted(meta, workloadIds), replyTo) =>
        replyTo ! PodWorkloadStarted(meta, workloadIds)
        Behaviors.same

      case HandlePodWorkloadEvent(Pod.WorkloadStartupFailure(cause), replyTo) =>
        replyTo ! PodWorkloadStartFailed(cause)
        Behaviors.same

      case HandlePodWorkloadFailure(cause, replyTo) =>
        replyTo ! PodWorkloadStartFailed(cause)
        Behaviors.same

      case GetAllStatistics(replyTo) =>
        context.ask(statsCollector, StatsCollector.FlushAllStats) {
          case Success(ev) => HandleStatsEvent(ev, replyTo)
          case Failure(t) => HandleStatsError(t, replyTo)
        }
        Behaviors.same

      case HandleStatsEvent(stats: StatsCollector.AllStatsFlushed, replyTo) =>
        replyTo ! RetrievedStatistics(stats)
        Behaviors.same

      case HandleStatsError(cause, replyTo) =>
        replyTo ! GetStatisticsFailed(cause)
        Behaviors.same

      case EndSimulation => Behaviors.stopped
    }
  }
}
