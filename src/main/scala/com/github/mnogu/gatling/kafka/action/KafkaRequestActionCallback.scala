package com.github.mnogu.gatling.kafka.action

import com.github.mnogu.gatling.kafka.protocol.KafkaProtocol
import com.github.mnogu.gatling.kafka.request.builder.KafkaAttributes
import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.DefaultClock
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

object KafkaRequestActionCallback {
  val clock = new DefaultClock

  def apply(session: Session,
            requestName: String,
            coreComponents: CoreComponents,
            throttled: Boolean,
            next: Action) =
    new KafkaRequestActionCallback(
      session,
      requestName,
      coreComponents,
      throttled,
      next
    )
}

class KafkaRequestActionCallback(session: Session,
                                 requestName: String,
                                 coreComponents: CoreComponents,
                                 throttled: Boolean,
                                 next: Action) extends Callback with StrictLogging {

  val requestStartDate: Long = KafkaRequestActionCallback.clock.nowMillis
  val statsEngine: StatsEngine = coreComponents.statsEngine

  private def logResponse(exception: Exception): Unit = {
    statsEngine.logResponse(
      session.scenario,
      session.groups,
      requestName,
      startTimestamp = requestStartDate,
      endTimestamp = KafkaRequestActionCallback.clock.nowMillis,
      if (exception == null) OK else KO,
      None,
      if (exception == null) None else Some(exception.getMessage)
    )
  }

  private def throttle(): Unit = {
    coreComponents.throttler match {
      case Some(t) => t.throttle(session.scenario, () => next ! session)
      case None => logger.error("Unable to retrieve throttler")
    }
  }

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    logResponse(exception)
    if (throttled) throttle() else next ! session
  }

}
