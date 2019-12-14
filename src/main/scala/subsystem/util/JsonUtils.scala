package subsystem.util

import java.util.concurrent.TimeUnit

import io.circe.Decoder

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object JsonUtils {

  implicit val decodeFiniteDuration: Decoder[FiniteDuration] = Decoder.decodeString.emap(
    string2finiteDurationReady(_).left.map(_ => "FiniteDuration")
  )

  private def string2finiteDurationReady(s: String): Either[Exception, FiniteDuration] = for {
    split <- s.split(" ").toList match {
      case lengthStr :: unitStr :: Nil => Right((lengthStr, unitStr))
      case _ => Left(new Exception(s"Invalid duration-ready string $s"))
    }
    parsed <- {
      val (lengthStr, unitStr) = split
      for {
        length <- Try(lengthStr.toLong).toEither.left.map(new Exception(_))
        unit <- Try(TimeUnit.valueOf(unitStr)).toEither.left.map(new Exception(_))
      } yield FiniteDuration(length, unit)
    }
  } yield parsed
}
