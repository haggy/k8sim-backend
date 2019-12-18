package subsystem.components

import java.util.UUID

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import subsystem.SubsystemManager
import subsystem.simulation.StatsCollector
import subsystem.storage.StorageInterface
import subsystem.util.AkkaUtils.NeedsReply

import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}

object ContainerWorkload {
  type WorkloadId = UUID
  type WorkloadGroupId = UUID
  final case class WorkloadMeta(id: WorkloadId, groupRoleName: String)
  final case class WorkloadInternalsConfig(myPodRef: ActorRef[Pod.PodCommand],
                                           subsysMgr: ActorRef[SubsystemManager.SubSysCommand],
                                           askTimeout: Timeout,
                                           statsCollector: ActorRef[StatsCollector.StatsCollectorCommand],
                                           randInst: Random,
                                           tickInterval: FiniteDuration = 100.millis)
  final case class NewContainerWorkloadConfig(groupRoleName: String, numContainers: Int)

  sealed trait ContainerWorkloadCommand
  sealed trait ContainerWorkloadEvent

  final case class StartWorkload(conf: NewContainerWorkloadConfig, replyTo: ActorRef[ContainerWorkloadEvent]) extends ContainerWorkloadCommand with NeedsReply[ContainerWorkloadEvent]
  final case class WorkloadStarted(meta: WorkloadMeta) extends ContainerWorkloadEvent
  final case object StopWorkload extends ContainerWorkloadCommand

  private final case class HandleStorageSubsysEvent(ev: StorageInterface.StorageEvent) extends ContainerWorkloadCommand
  private final case class HandleStorageSubsysFailure(cause: Throwable) extends ContainerWorkloadCommand

  private final case object Tick extends ContainerWorkloadCommand

  def apply(config: WorkloadInternalsConfig): Behavior[ContainerWorkloadCommand] = initialBehavior(config.copy(
    askTimeout = Timeout(FiniteDuration(config.tickInterval.length * 10, config.tickInterval.unit)) // Need this in case queues are large and overloaded
  ))

  private def initialBehavior(internalConf: WorkloadInternalsConfig): Behavior[ContainerWorkloadCommand] = Behaviors.receive { (context, message) =>
    message match {
      case StartWorkload(newConf, replyTo) =>
        val myId = UUID.randomUUID()
        context.log.info("Starting workload with ID [{}]", myId)
        replyTo ! WorkloadStarted(WorkloadMeta(myId, newConf.groupRoleName))
        running(myId, internalConf, newConf)

      case other =>
        context.log.debug("Command [{}] ignored since workload has not been started", other)
        Behaviors.same
    }
  }

  private def running(myId: UUID,
                      internalConf: WorkloadInternalsConfig,
                      config: NewContainerWorkloadConfig): Behavior[ContainerWorkloadCommand] = Behaviors.setup { context =>
    implicit val askTimeout: Timeout = internalConf.askTimeout

    val randInst = internalConf.randInst
    val subsystemManager = internalConf.subsysMgr
    val myPod = internalConf.myPodRef
    val statsCollector = internalConf.statsCollector

    Behaviors.withTimers[ContainerWorkloadCommand] { timerCtx =>
      scheduleNextTickWithJitter(myId, randInst, internalConf.tickInterval, timerCtx)

      Behaviors.receiveMessagePartial {
        case Tick =>
          context.ask(subsystemManager, (ref: ActorRef[StorageInterface.StorageEvent]) =>  SubsystemManager.DiskSubsystemRequest(StorageInterface.ReadData(
            StorageInterface.StoragePath("/dev/null"),
            ref
          ))) {
            case Success(ev) => HandleStorageSubsysEvent(ev)
            case Failure(t) => HandleStorageSubsysFailure(t)
          }
          scheduleNextTickWithJitter(myId, randInst, internalConf.tickInterval, timerCtx)
          Behaviors.same

        case HandleStorageSubsysEvent(ev) =>
          context.log.trace("Received event from storage subsystem: [{}]", ev)
          statsCollector ! StatsCollector.RecordStorageOpMetric(myId, ev)
          Behaviors.same

        case HandleStorageSubsysFailure(cause) =>
          context.log.error("Received error from storage subsystem", cause)
          Behaviors.same

        case StopWorkload => Behaviors.stopped
      }
    }
  }

  private def scheduleNextTickWithJitter(key: UUID,rand: Random, tickInterval: FiniteDuration, timerCtx: TimerScheduler[ContainerWorkloadCommand]): Unit = {
    val currentLength = tickInterval.length
    val percentage = rand.nextFloat()
    val coeff = if(rand.nextFloat() < 0.5) 1 else -1
    val delay = FiniteDuration(Math.round(currentLength.toDouble + (currentLength * percentage * coeff)), tickInterval.unit)
    timerCtx.startSingleTimer(key, Tick, delay)
  }
}
