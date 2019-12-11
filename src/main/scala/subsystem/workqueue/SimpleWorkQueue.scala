package subsystem.workqueue

import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import subsystem.storage.StorageInterface.Frequency
import subsystem.workqueue.models._

import scala.annotation.tailrec

object SimpleWorkQueue {
  final case class SimpleWorkQueueConfig(capacity: Option[Int], processSpeed: Frequency)

  private sealed trait QueueWorkerCommand
  private sealed trait QueueWorkerEvent
  private final case object Tick extends QueueWorkerCommand

  def apply[T](config: SimpleWorkQueueConfig): Behavior[WorkQueueCommand] = behavior[T](config, getQueue(config.capacity))

  private def behavior[T](config: SimpleWorkQueueConfig, queue: LinkedBlockingQueue[PushWork[T]]): Behavior[WorkQueueCommand] = Behaviors.setup { context =>
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

      @tailrec
      def processQueue(remainingCapacity: Int): Unit = remainingCapacity match {
        case 0 => // NOOP
        case remaining =>
          Option(queue.peek()) match {
            case None => Behaviors.same // NOOP
            case Some(work) =>
              val workValAfterProcessing = work.decreaseRemainingCost(remaining)

              if(work.workComplete) {
                queue.poll()
                replyTo ! ProcessWork(work)
              }

              if(workValAfterProcessing < 0) {
                val newRemaining = workValAfterProcessing * -1
                processQueue(newRemaining)
              }
          }
      }

      val timerKey = UUID.randomUUID().toString
      timerCtx.startTimerWithFixedDelay(timerKey, Tick, frequency.tickSleepDuration)
      Behaviors.receiveMessagePartial {
        case Tick =>
          processQueue(frequency.perTick)
          Behaviors.same
      }
    }

}
