package subsystem.simulation

import java.util.UUID

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import subsystem.components.Pod
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

  private final case class HandlePodCreated(meta: Pod.PodMeta, replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]
  private final case class HandlePodCreateFailure(meta: Pod.PodMeta, cause: Throwable, replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]
  private final case class HandleUnknownPodCreateFailure(cause: Throwable, replyTo: ActorRef[SimManagerEvent]) extends SimManagerCommand with NeedsReply[SimManagerEvent]

  private final case class ComponentTracker(pods: Map[Pod.PodId, ActorRef[Pod.PodCommand]] = Map.empty) {
    def addPod(id: Pod.PodId, newPod: ActorRef[Pod.PodCommand]): ComponentTracker = copy(pods + (id -> newPod))
  }

  def apply(): Behavior[SimManagerCommand] = initialBehavior

  private def initialBehavior: Behavior[SimManagerCommand] = Behaviors.receive { (context, message) =>
    message match {
      case StartSimulationManager(config, replyTo) =>
        val meta = SimManagerMeta(UUID.randomUUID())
        replyTo ! SimulationStarted(meta)
        running(config, meta, ComponentTracker())

      case msg =>
        context.log.warn("Ignoring message since Simulation has not been started: [{}]", msg)
        Behaviors.same
    }
  }

  private def running(config: SimulationConfig, simMeta: SimManagerMeta, compTracker: ComponentTracker): Behavior[SimManagerCommand] = Behaviors.setup { context =>
    implicit val askTimeout: Timeout = config.messageTimeout
    context.log.info("Started simulation manager with ID [{}]", simMeta.id)
    Behaviors.receiveMessagePartial {
      case CreatePod(podConf, originalSender) =>
        val pod = context.spawnAnonymous(Pod())
        context.ask(pod, (ref: ActorRef[Pod.PodEvent]) =>  Pod.StartPod(podConf, ref)) {
          case Success(started: Pod.PodStarted) => HandlePodCreated(started.meta, originalSender)
          case Success(failed: Pod.PodStartupFailed) => HandlePodCreateFailure(failed.meta, failed.cause, originalSender)
          case Success(other) => HandleUnknownPodCreateFailure(new Exception(s"Unknown response received: $other"), originalSender)
          case Failure(t) => HandleUnknownPodCreateFailure(t, originalSender)
        }
        Behaviors.same

      case HandlePodCreated(podMeta, replyTo) =>
        replyTo ! PodCreated(podMeta)
        Behaviors.same
      case HandlePodCreateFailure(meta, cause, replyTo) =>
        replyTo ! PodCreateFailed(Some(meta), cause)
        Behaviors.same
      case HandleUnknownPodCreateFailure(cause, replyTo) =>
        replyTo ! PodCreateFailed(None, cause)
        Behaviors.same

      case EndSimulation => Behaviors.stopped
    }
  }
}
