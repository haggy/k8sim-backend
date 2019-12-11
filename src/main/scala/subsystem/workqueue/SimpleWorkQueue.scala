package subsystem.workqueue

import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.actor.typed.scaladsl.Behaviors
import subsystem.storage.StorageInterface.Frequency

object SimpleWorkQueue {
  final case class WorkQueueConfig(capacity: Option[Int], processSpeed: Frequency)

  sealed trait WorkQueueCommand
  sealed trait WorkQueueEvent

  final case class PushWork[T](work: T, cost: Int, replyTo: ActorRef[WorkQueueEvent]) extends WorkQueueCommand {
    private val remainingCost = new AtomicInteger(cost)
    def decreaseRemainingCost(by: Int): Unit = remainingCost.addAndGet(-by)
    def workComplete: Boolean = remainingCost.get() <= 0
  }

  final case class WorkQueued[T](queuedWork: T) extends WorkQueueEvent
  final case class QueueFull[T](requestedWork: T) extends WorkQueueEvent
  final case class WorkReady[T](work: PushWork[T]) extends WorkQueueEvent
  private final case class ProcessWork[T](work: PushWork[T]) extends WorkQueueCommand


  private sealed trait QueueWorkerCommand
  private sealed trait QueueWorkerEvent
  private final case object Tick extends QueueWorkerCommand

  def apply[T](config: WorkQueueConfig): Behavior[WorkQueueCommand] = behavior[T](config, getQueue(config.capacity))

  private def behavior[T](config: WorkQueueConfig, queue: LinkedBlockingQueue[PushWork[T]]): Behavior[WorkQueueCommand] = Behaviors.setup { context =>
    var queueSize = 0
    context.watch(context.spawnAnonymous(
      Behaviors.supervise(queueWorkerBehavior(queue, context.self, config.processSpeed))
        .onFailure(SupervisorStrategy.restart)
    ))

    Behaviors.receiveMessagePartial {
      case pw: PushWork[T] =>
        val work = pw.work
        val cost = pw.cost
        val event =
          if(config.capacity.isEmpty || config.capacity.exists(capacity => queueSize + cost <= capacity)) {
            queue.offer(pw)
            queueSize += cost
            WorkQueued(work)
          } else QueueFull(work)

        pw.replyTo ! event
        Behaviors.same

      case pw: ProcessWork[T] =>
        queueSize -= pw.work.cost
        pw.work.replyTo ! WorkReady(pw.work)
        Behaviors.same
    }
  }

  private def getQueue[T](capacity: Option[Int]) =
    capacity.fold(new LinkedBlockingQueue[PushWork[T]]())(s => new LinkedBlockingQueue[PushWork[T]](s))

  private def queueWorkerBehavior[T](queue: LinkedBlockingQueue[PushWork[T]], replyTo: ActorRef[WorkQueueCommand], frequency: Frequency): Behavior[Tick.type] =
    Behaviors.withTimers[Tick.type] { timerCtx =>
      val timerKey = UUID.randomUUID().toString
      timerCtx.startTimerWithFixedDelay(timerKey, Tick, frequency.tickSleepDuration)
      Behaviors.receiveMessagePartial {
        case Tick =>
          Option(queue.peek()) match {
            case None => Behaviors.same // NOOP
            case Some(work) =>
              work.decreaseRemainingCost(frequency.perTick)
              if(work.workComplete) {
                queue.poll()
                replyTo ! ProcessWork(work)
              }
              Behaviors.same
          }
      }
    }

}
