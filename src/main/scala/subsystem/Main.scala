package subsystem

import java.util.UUID

import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import subsystem.SubsystemManager.{DiskSubsystemRequest, SubSysEvent, SubsystemManagerConfig}
import subsystem.storage.StorageInterface.{StorageEvent, StoragePath, WriteData}
import subsystem.storage.hdd.SpindleBasedStorage.SpindleBasedConfig

import scala.concurrent.duration._

object Main extends App {

  sealed trait EventLoopCmd
  final case object Tick extends EventLoopCmd

  private def emptyReplyTo[T](context: ActorContext[_]): ActorRef[T] = context.spawn(Behaviors.ignore[T], s"ignoringRef-${UUID.randomUUID()}")

  private def apply(subsystemManagerConfig: SubsystemManagerConfig): Behavior[EventLoopCmd] = Behaviors.setup { context =>
    val subsysMgr = context.spawn(SubsystemManager(), "subsystemManager")

    def diskReq() = {
      DiskSubsystemRequest(WriteData(
        StoragePath(UUID.randomUUID().toString),
        "waaaazuuupp".getBytes(),
        context.spawn(
          Behaviors.receive[StorageEvent] { (ctx, message) =>
            ctx.log.info("Received response from disk subsystem: [{}]", message)
            Behaviors.stopped
          },
          s"diskResponseHandler-${UUID.randomUUID()}"
        )
      ))
    }

    subsysMgr ! SubsystemManager.Start(subsystemManagerConfig, emptyReplyTo[SubSysEvent](context))
    subsysMgr ! diskReq()

    Behaviors.withTimers[EventLoopCmd] { timerCtx =>
      timerCtx.startTimerWithFixedDelay(Tick, 500.millis)
      Behaviors.receiveMessagePartial {
        case Tick =>
          subsysMgr ! diskReq()
          Behaviors.same
      }
    }

  }

  ActorSystem(Main(
    SubsystemManagerConfig(SpindleBasedConfig())
  ), "system")
}
