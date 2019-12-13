package subsystem.components

import java.util.UUID

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import subsystem.SubsystemManager
import subsystem.SubsystemManager.SubsystemManagerConfig
import subsystem.util.AkkaUtils.NeedsReply

import scala.util.{Failure, Success}

object Pod {
  type PodId = UUID
  final case class PodMeta(id: PodId)
  final case class PodConfig(initMessageTimeout: Timeout, subsystemManagerConfig: SubsystemManagerConfig)
  sealed trait PodCommand
  sealed trait PodEvent

  final case class StartPod(config: PodConfig, replyTo: ActorRef[PodEvent]) extends PodCommand with NeedsReply[PodEvent]
  final case class PodStarted(meta: PodMeta) extends PodEvent
  final case class PodStartupFailed(meta: PodMeta, cause: Throwable) extends PodEvent
  final case class GetStatus(replyTo: ActorRef[PodEvent]) extends PodCommand with NeedsReply[PodEvent]
  final case class CurrentPodStatus(status: PodStatus) extends PodEvent

  private final case class HandleSubsytemStartupComplete(replyTo: ActorRef[PodEvent]) extends PodCommand with NeedsReply[PodEvent]
  private final case class HandleSubsytemStartupFailed(replyTo: ActorRef[PodEvent], cause: Throwable) extends PodCommand with NeedsReply[PodEvent]

  sealed trait PodStatus extends PodEvent{ val asString: String}
  final case object Stopped extends PodStatus { val asString = "stopped" }
  final case object Running extends PodStatus { val asString = "running" }
  final case object Failed extends PodStatus { val asString = "failed" }

  def apply(): Behavior[PodCommand] = Behaviors.receive { (context, message) =>
    message match {
      case StartPod(conf, originalSender) =>
        implicit val askTimeout: Timeout = conf.initMessageTimeout

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
            replyTo ! PodStarted(meta)
            podRunning(meta, conf, subsysMgr)

          case HandleSubsytemStartupFailed(replyTo, cause) =>
            replyTo ! PodStartupFailed(meta, cause)
            Behaviors.stopped
        }

      case GetStatus(replyTo) =>
        replyTo ! Stopped
        Behaviors.same
    }
  }

  private def podRunning(meta: PodMeta, config: PodConfig,
                         subsystemMgr: ActorRef[SubsystemManager.SubSysCommand]): Behavior[PodCommand] = Behaviors.setup { context =>
    context.log.info("Pod running with ID [{}]", meta.id)
    Behaviors.receiveMessagePartial {
      case GetStatus(replyTo) =>
        replyTo ! Running
        Behaviors.same
    }
  }
}
