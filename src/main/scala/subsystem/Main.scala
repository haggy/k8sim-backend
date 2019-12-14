package subsystem

import java.util.UUID

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import org.slf4j.Logger
import subsystem.SimulationsManager.CreateSimulation
import subsystem.components.Pod
import subsystem.config.AppConfig
import subsystem.http.Server
import subsystem.simulation.SimulationManager
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
  final case object SimulationEnded extends SimsManagerEvent
  final case class CreatePod(simId: SimulationManager.SimulationId, podConf: Pod.PodConfig, replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand with NeedsReply[SimsManagerEvent]
  final case class HandlePodCreateResp(resp: SimsManagerEvent, originalSender: ActorRef[SimsManagerEvent]) extends SimsManagerCommand
  final case class PodCreated(meta: Pod.PodMeta) extends SimsManagerEvent
  final case class PodCreateFailed(cause: Throwable) extends SimsManagerEvent

  private final case class HandleSimStartEvent(resp: SimulationManager.SimulationStarted, simMgr: ActorRef[SimulationManager.SimManagerCommand], replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand
  private final case class HandleSimulationStartFailure(cause: Throwable, replyTo: ActorRef[SimsManagerEvent]) extends SimsManagerCommand with NeedsReply[SimsManagerEvent]
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
