package subsystem.simulation

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import subsystem.storage.StorageInterface
import subsystem.util.AkkaUtils.NeedsReply

object StatsCollector {

  final case class StatsConfig()

  sealed trait StatsCollectorCommand
  sealed trait StatsCollectorEvent

  final case class RecordStorageOpMetric(componentId: UUID, op: StorageInterface.StorageEvent) extends StatsCollectorCommand
  final case class StorageStats(total: Int, readSuccess: Int, readFailure: Int, writeSuccess: Int, writeFailure: Int)

  final case class FlushAllStats(replyTo: ActorRef[StatsCollectorEvent]) extends StatsCollectorCommand with NeedsReply[StatsCollectorEvent]

  final case class FlushedStats(storage: StorageStats)
  final case class FlushedStatsForComponent(id: UUID, stats: FlushedStats)

  final case class AllStatsFlushed(allStats: List[FlushedStatsForComponent]) extends StatsCollectorEvent

  private final class StorageStatTracker() {
    private val total = new AtomicInteger(0)
    private val readSuccess = new AtomicInteger(0)
    private val readFailure = new AtomicInteger(0)
    private val writeSuccess = new AtomicInteger(0)
    private val writeFailure = new AtomicInteger(0)

    def incrementTotal(): Unit = total.incrementAndGet()

    def incrementReadSuccess(): Unit = readSuccess.incrementAndGet()

    def incrementReadFailure(): Unit = readFailure.incrementAndGet()

    def incrementWriteSuccess(): Unit = writeSuccess.incrementAndGet()

    def incrementWriteFailure(): Unit = writeFailure.incrementAndGet()

    def dump(): StorageStats = {
      val stats = StorageStats(
        total.get(),
        readSuccess.get(),
        readFailure.get(),
        writeSuccess.get(),
        writeFailure.get()
      )
      clear()
      stats
    }

    private def clear(): Unit = {
      total.set(0)
      readSuccess.set(0)
      readFailure.set(0)
      writeSuccess.set(0)
      writeFailure.set(0)
    }
  }

  private final class ComponentTrackers(val storage: StorageStatTracker) {
    def this() {
      this(new StorageStatTracker())
    }
  }

  private final class ComponentTrackersState(private val trackerMap: collection.mutable.HashMap[UUID, ComponentTrackers]) {
    def this() {
      this(collection.mutable.HashMap.empty[UUID, ComponentTrackers])
    }

    def add(id: UUID, trackers: ComponentTrackers): Option[ComponentTrackers] = trackerMap.put(id, trackers)
    def get(id: UUID): Option[ComponentTrackers] = trackerMap.get(id)
    def remove(id: UUID): Option[ComponentTrackers] = trackerMap.remove(id)

    def flushAll: List[FlushedStatsForComponent] = trackerMap.map { case (compId, compTrackers) =>
      FlushedStatsForComponent(compId, FlushedStats(
        compTrackers.storage.dump()
      ))
    }.toList
  }

  def apply(): Behavior[StatsCollectorCommand] = running(Map.empty)

  def running(trackerState: Map[UUID, ComponentTrackers]): Behavior[StatsCollectorCommand] = Behaviors.setup { context =>

    Behaviors.receiveMessagePartial {
      case FlushAllStats(replyTo) =>
        replyTo ! AllStatsFlushed(trackerState.map { case (compId, compTrackers) =>
          FlushedStatsForComponent(compId, FlushedStats(
            compTrackers.storage.dump()
          ))
        }.toList)
        Behaviors.same

      case RecordStorageOpMetric(compId, op) =>
        val (tracker, isNew) = trackerState.get(compId).fold {
          (new ComponentTrackers(), true)
        }((_, false))

        tracker.storage.incrementTotal()
        op match {
          case _: StorageInterface.DataRetrieved => tracker.storage.incrementReadSuccess()
          case _: StorageInterface.DataReadFailed => tracker.storage.incrementReadFailure()
          case _: StorageInterface.DataWritten => tracker.storage.incrementWriteSuccess()
          case _: StorageInterface.DataWriteFailed => tracker.storage.incrementWriteFailure()
        }

        if(!isNew) {
          running(trackerState)
        } else {
          running(trackerState + (compId -> tracker))
        }
    }
  }

}
