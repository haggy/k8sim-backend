package subsystem.util

import akka.actor.typed.ActorRef

object AkkaUtils {

  trait NeedsReply[T] {
    val replyTo: ActorRef[T]
  }

}
