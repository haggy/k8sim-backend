package subsystem.http

import java.util.UUID

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import io.finch._
import io.finch.circe._
import io.finch.syntax._
import io.circe.generic.auto._
import subsystem.util.JsonUtils._
import subsystem.SimulationsManager
import subsystem.http.Server._
import subsystem.simulation.SimulationManager
import subsystem.util.FutureUtils._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import akka.actor.typed.scaladsl.AskPattern._
import com.twitter.finagle.{Http, ListeningServer}
import com.twitter.util.{Await, Future}
import subsystem.components.Pod
import subsystem.config.HttpConfig
import subsystem.util.HttpUtils._

object Server {
  final case class AkkaInterop(askTimeout: FiniteDuration)
  final case class HttpServerConfig(httpSettings: HttpConfig, akkaInterop: AkkaInterop)

  final case class HealthcheckResponse(allOk: Boolean)

  final case class NewSimulationReq()
  final case class NewSimulationResp(meta: SimulationManager.SimManagerMeta)

  final case class NewPodReq(podConfig: Pod.PodConfig)
  final case class NewPodResp(podMeta: Pod.PodMeta)

}
class Server(config: HttpServerConfig, simsMgrRef: ActorRef[SimulationsManager.SimsManagerCommand])(implicit ec: ExecutionContext, scheduler: Scheduler) {

  implicit val askTimeout: Timeout = Timeout(config.akkaInterop.askTimeout)

  private val SimulationRoot = "simulation"
  private val PodRoot = "pod"

  private val healthcheck: Endpoint[HealthcheckResponse] = get("healthcheck") { () =>
    Future.value(Ok(HealthcheckResponse(allOk = true)))
  }

  private val createSimulation: Endpoint[NewSimulationResp] = post(SimulationRoot :: jsonBody[NewSimulationReq]) { req: NewSimulationReq =>
    simsMgrRef.ask[SimulationsManager.SimsManagerEvent] { ref =>
      SimulationsManager.CreateSimulation(SimulationManager.SimulationConfig(), ref)
    }
      .mapTo[SimulationsManager.SimulationCreated]
      .map(sc => Ok(NewSimulationResp(sc.meta)))
      .asTwitter
  }

  private val stopSimulation: Endpoint[Unit] = put(SimulationRoot :: path[UUID] :: "stop") { simId: UUID =>
    simsMgrRef.ask[SimulationsManager.SimsManagerEvent] { ref =>
      SimulationsManager.EndSimulation(simId, ref)
    }
      .map {
        case SimulationsManager.SimulationEnded => Ok({})
        case SimulationsManager.NoSimulationFound(_) => NotFound(new Exception("No simulation found"))
        case other => unknownManagerResponse(other)
      }
      .asTwitter
  }

  private val createPod: Endpoint[NewPodResp] = post(SimulationRoot :: path[UUID] :: PodRoot :: jsonBody[NewPodReq]) { (simId: UUID, req: NewPodReq) =>
    simsMgrRef.ask[SimulationsManager.SimsManagerEvent] { ref =>
      SimulationsManager.CreatePod(simId, req.podConfig, ref)
    }
      .map {
        case SimulationsManager.PodCreated(meta) => Ok(NewPodResp(meta))
        case SimulationsManager.PodCreateFailed(cause) => BadRequest(new Exception(cause))
        case SimulationsManager.NoSimulationFound(_) => NotFound(new Exception("No simulation found"))
        case other => unknownManagerResponse(other)
      }
      .asTwitter
  }

  private def unknownManagerResponse(resp: SimulationsManager.SimsManagerEvent) =
    InternalServerError(new Exception("Invalid simulation manager response"))

  private val api = healthcheck :+: createSimulation :+: stopSimulation :+: createPod

  def start(): ListeningServer = {
    Await.ready(Http.server.serve(
      s":${config.httpSettings.port}",
      api.toServiceAs[Application.Json]
    ))
  }
}
