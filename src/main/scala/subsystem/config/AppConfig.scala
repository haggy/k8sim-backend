package subsystem.config

import com.typesafe.config.{Config, ConfigFactory}
import io.circe.config.syntax._
import io.circe.generic.auto._

import scala.concurrent.duration.FiniteDuration

final case class HttpConfig(port: Int)
final case class GlobalAkkaSettings(defaultAskTimeoutDuration: FiniteDuration)
final case class AppConfig(http: HttpConfig, globalAkkaSettings: GlobalAkkaSettings)

object AppConfig {
  val AppName = "subsystem"
  private val rawConfig: Config = ConfigFactory.load().resolve()
  val inst: AppConfig = rawConfig.as[AppConfig](AppName).fold(throw _, identity)
}
