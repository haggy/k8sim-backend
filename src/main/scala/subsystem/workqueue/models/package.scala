package subsystem.workqueue

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.typed.ActorRef

package object models {
  sealed trait WorkQueueCommand
  sealed trait WorkQueueEvent

  final case class PushWork[T](work: T, cost: Int, replyTo: ActorRef[WorkQueueEvent]) extends WorkQueueCommand {
    private val remainingCost = new AtomicInteger(cost)
    def decreaseRemainingCost(by: Int): Int = remainingCost.addAndGet(-by)
    def workComplete: Boolean = remainingCost.get() <= 0
  }

  final case class WorkQueued[T](queuedWork: T) extends WorkQueueEvent
  final case class QueueFull[T](requestedWork: T) extends WorkQueueEvent
  final case class WorkReady[T](work: PushWork[T]) extends WorkQueueEvent
  final case class ProcessWork[T](work: PushWork[T]) extends WorkQueueCommand
}
