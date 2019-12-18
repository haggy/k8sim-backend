package subsystem

import java.util.UUID

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import org.slf4j.Logger
import subsystem.components.{ContainerWorkload, Pod}
import subsystem.config.AppConfig
import subsystem.http.Server
import subsystem.simulation.{SimulationManager, StatsCollector}
import subsystem.util.AkkaUtils.NeedsReply

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object SimulationsManager {

  final case class SimulationsManagerConfig(messageTimeout: Timeout = Timeout(3.seconds))

  sealed trait SimsManagerCommand
  sealed trait SimsManagerEvent
  final case class CreateSimulation(conf: SimulationManager.SimulationConfig, replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand with NeedsReply[SimsManagerEvent]
  final case class SimulationCreated(meta: SimulationManager.SimManagerMeta) extends SimsManagerEvent
  final case class EndSimulation(simId: SimulationManager.SimulationId, replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand with NeedsReply[SimsManagerEvent]
  final case class NoSimulationFound(simId: SimulationManager.SimulationId) extends SimsManagerEvent
  private final case class HandleSimStartEvent(resp: SimulationManager.SimulationStarted, simMgr: ActorRef[SimulationManager.SimManagerCommand], replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand
  private final case class HandleSimulationStartFailure(cause: Throwable, replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand with NeedsReply[SimsManagerEvent]

  final case object SimulationEnded extends SimsManagerEvent
  final case class CreatePod(simId: SimulationManager.SimulationId, podConf: Pod.PodConfig, replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand with NeedsReply[SimsManagerEvent]
  private final case class HandlePodCreateResp(resp: SimsManagerEvent, originalSender: ActorRef[SimsManagerEvent]) extends SimsManagerCommand
  final case class PodCreated(meta: Pod.PodMeta) extends SimsManagerEvent
  final case class PodCreateFailed(cause: Throwable) extends SimsManagerEvent

  final case class StartWorkload(simId: SimulationManager.SimulationId, podId: Pod.PodId, config: ContainerWorkload.NewContainerWorkloadConfig, replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand with NeedsReply[SimsManagerEvent]
  final case class StopWorkload(simId: SimulationManager.SimulationId, podId: Pod.PodId, workloadId: ContainerWorkload.WorkloadId, replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand with NeedsReply[SimsManagerEvent]
  private final case class HandleWorkloadEvent(ev: SimulationManager.SimManagerEvent, originalSender: ActorRef[SimsManagerEvent]) extends SimsManagerCommand
  private final case class HandleWorkloadFailure(cause: Throwable, originalSender: ActorRef[SimsManagerEvent]) extends SimsManagerCommand
  final case class WorkloadStarted(meta: ContainerWorkload.WorkloadMeta, workloadIds: List[ContainerWorkload.WorkloadId]) extends SimsManagerEvent
  final case class WorkloadStartFailed(cause: Throwable) extends SimsManagerEvent
  final case object WorkloadStopped extends SimsManagerEvent

  final case class GetAllStatistics(simId: SimulationManager.SimulationId, replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand with NeedsReply[SimsManagerEvent]
  final case class RetrievedStatistics(allStats: StatsCollector.AllStatsFlushed) extends SimsManagerEvent
  final case class GetStatisticsFailed(cause: Throwable) extends SimsManagerEvent

  private final case class HandleStatsEvent(ev: SimulationManager.SimManagerEvent, originalRequester: ActorRef[SimsManagerEvent]) extends SimsManagerCommand
  private final case class HandleStatsError(cause: Throwable, originalRequester: ActorRef[SimsManagerEvent]) extends SimsManagerCommand

  private final case class SimulationTracker(private val activeSims: Map[SimulationManager.SimulationId, ActorRef[SimulationManager.SimManagerCommand]] = Map.empty) {
    def add(id: SimulationManager.SimulationId, ref: ActorRef[SimulationManager.SimManagerCommand]): SimulationTracker =
      copy(activeSims + (id -> ref))

    def get(id: SimulationManager.SimulationId): Option[ActorRef[SimulationManager.SimManagerCommand]] = activeSims.get(id)

    def remove(id: SimulationManager.SimulationId): SimulationTracker =
      copy(activeSims - id)
  }

  def apply(simsConfig: SimulationsManagerConfig): Behavior[SimsManagerCommand] = running(simsConfig, SimulationTracker())

  def running(simsConfig: SimulationsManagerConfig, tracker: SimulationTracker): Behavior[SimsManagerCommand] = Behaviors.setup { context =>
    implicit val askTimeout: Timeout = simsConfig.messageTimeout

    Behaviors.receiveMessagePartial {
      case CreateSimulation(conf, replyTo) =>

        val sim = context.spawnAnonymous(SimulationManager())
        context.ask(sim, SimulationManager.StartSimulationManager(conf,_)) {
          case Success(started: SimulationManager.SimulationStarted) => HandleSimStartEvent(started, sim, replyTo)
          case Failure(t) => HandleSimulationStartFailure(t, replyTo)
        }
        Behaviors.same

      case HandleSimStartEvent(started: SimulationManager.SimulationStarted, simMgr, replyTo) =>
        replyTo ! SimulationCreated(started.meta)
        running(simsConfig, tracker.add(started.meta.id, simMgr))

      case EndSimulation(id, replyTo) =>
        val resp = tracker.get(id).fold(NoSimulationFound(id): SimsManagerEvent) { simRef =>
          simRef ! SimulationManager.EndSimulation
          SimulationEnded
        }
        replyTo ! resp
        running(simsConfig, tracker.remove(id))

      case CreatePod(simId, podConf, replyTo) =>
        tracker.get(simId) match {
          case None => replyTo ! NoSimulationFound(simId)
          case Some(simMgrRef) =>
            context.ask(simMgrRef, SimulationManager.CreatePod(podConf, _)) {
              case Success(SimulationManager.PodCreated(meta)) => HandlePodCreateResp(PodCreated(meta), replyTo)
              case Success(SimulationManager.PodCreateFailed(_, cause)) => HandlePodCreateResp(PodCreateFailed(cause), replyTo)
              case Failure(t) => HandlePodCreateResp(PodCreateFailed(t), replyTo)
            }
        }
        Behaviors.same

      case HandlePodCreateResp(resp, replyTo) =>
        replyTo ! resp
        Behaviors.same

      case StartWorkload(simId, podId, config, replyTo) =>
        tracker.get(simId) match {
          case None => replyTo ! NoSimulationFound(simId)
          case Some(simMgrRef) =>
            context.ask(simMgrRef, SimulationManager.StartPodWorkload(podId, config, _)) {
              case Success(ev) => HandleWorkloadEvent(ev, replyTo)
              case Failure(t) => HandleWorkloadFailure(t, replyTo)
            }
        }
        Behaviors.same

      case StopWorkload(simId, podId, workloadId, replyTo) =>
        tracker.get(simId) match {
          case None => replyTo ! NoSimulationFound(simId)
          case Some(simMgrRef) =>
            context.ask(simMgrRef, SimulationManager.StopPodWorkload(podId, workloadId, _)) {
              case Success(ev) => HandleWorkloadEvent(ev, replyTo)
              case Failure(t) => HandleWorkloadFailure(t, replyTo)
            }
        }
        Behaviors.same

      case HandleWorkloadEvent(SimulationManager.PodWorkloadStarted(meta, workloadIds), replyTo) =>
        replyTo ! WorkloadStarted(meta, workloadIds)
        Behaviors.same

      case HandleWorkloadEvent(SimulationManager.PodWorkloadStartFailed(cause), replyTo) =>
        replyTo ! WorkloadStartFailed(cause)
        Behaviors.same

      case HandleWorkloadEvent(SimulationManager.PodWorkloadStopped, replyTo) =>
        replyTo ! WorkloadStopped
        Behaviors.same

      case HandleWorkloadFailure(cause, replyTo) =>
        replyTo ! WorkloadStartFailed(cause)
        Behaviors.same

      case GetAllStatistics(simId: SimulationManager.SimulationId, replyTo) =>
        tracker.get(simId) match {
          case None => replyTo ! NoSimulationFound(simId)
          case Some(simMgrRef) =>
            context.ask(simMgrRef, SimulationManager.GetAllStatistics) {
              case Success(ev) => HandleStatsEvent(ev, replyTo)
              case Failure(t) => HandleStatsError(t, replyTo)
            }
        }
        Behaviors.same

      case HandleStatsEvent(ev: SimulationManager.RetrievedStatistics, replyTo) =>
        replyTo ! RetrievedStatistics(ev.allStats)
        Behaviors.same

      case HandleStatsError(cause, replyTo) =>
        replyTo ! GetStatisticsFailed(cause)
        Behaviors.same
    }
  }
}

object Main extends App {

  val appConfig = AppConfig.inst

  private def emptyReplyTo[T](context: ActorContext[_]): ActorRef[T] = context.spawn(Behaviors.ignore[T], s"ignoringRef-${UUID.randomUUID()}")

  private def messageLoggingBehavior[T](log: Logger): Behavior[T] = Behaviors.receiveMessagePartial[T] {
    case any =>
      log.info("Received message and stopping: [{}]", any)
      Behaviors.stopped
  }

//  def simsMgrHandlerBehavior(): Behavior[SimulationsManager.HandleSimStartEvent] = Behaviors.setup {
//
//    Behaviors.receiveMessagePartial {
//      case SimulationsManager.SimulationCreated(meta) =>
//
//    }
//  }

  val simsConf = SimulationsManager.SimulationsManagerConfig()
  val b = ActorSystem[SimulationsManager.SimsManagerCommand](
    SimulationsManager(simsConf),
    "system"
  )

  implicit val ec = b.executionContext
  implicit val scheduler = b.scheduler

  new Server(
    Server.HttpServerConfig(appConfig.http, Server.AkkaInterop(appConfig.globalAkkaSettings.defaultAskTimeoutDuration)),
    b
  ).start()
}
