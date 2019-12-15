package subsystem.http.filters

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.Future
import org.slf4j.LoggerFactory
import subsystem.http.filters.CorsFilter.CorsConfig

/**
  * Given a list of allowed origins, this filter optionally applies the allowed origin headers
  * based on the incoming origin.
  * For example:
  *   IncomingOrigin = "http://localhost:8100"
  *   AllowedOriginsList = ["http://localhost:8100", "http://localhost:9000", ...]
  *   OriginAllowed? Yes
  *
  * @param config App config
  */
class CorsFilter(config: CorsConfig)
  extends SimpleFilter[Request, Response] {
  private val log = LoggerFactory.getLogger(classOf[CorsFilter])
  private val allowedOrigins = config.`allowed-origins`
  private val allowAll = config.`allow-all-origins`

  def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
    service(request).map { resp =>
      val originHeader = request.headerMap.get("Origin")
      log.debug("Request Origin: [{}]", originHeader.getOrElse(""))

      originHeader match {
        case None => resp
        case Some(originHdr) if allowAll || allowedOrigins.contains(originHdr) =>
          resp.headerMap.put("Access-Control-Allow-Origin", originHdr)
          resp.headerMap.put("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS")
          resp.headerMap.put("Access-Control-Allow-Headers", "POST, content-type")
          resp.headerMap.put("Access-Control-Allow-Credentials", true.toString)
          // NOTE: Need this for Chrome's CORB
          // @see https://www.chromestatus.com/feature/5629709824032768
          resp.headerMap.put("Content-Type", "application/json")
          resp
        case _ => resp
      }
    }
  }
}

object CorsFilter {
  case class CorsConfig(`allow-all-origins`: Boolean, `allowed-origins`: Set[String])
}
