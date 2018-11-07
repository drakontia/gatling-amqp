package io.gatling.amqp

import io.gatling.amqp.Predef._
import io.gatling.amqp.config._
import io.gatling.amqp.data._
import io.gatling.core.Predef._

import scala.concurrent.duration._
import collection.mutable._
import scala.collection.JavaConverters._

class PublishingSimulationWithExternal extends Simulation {
  private val exchangePubSim: AmqpExchange = exchange("gatlingPublishingSimulation", "fanout", durable = true, autoDelete = false)
  private val queueQ1: AmqpQueue = queue("q1", durable = true, autoDelete = false)
  implicit val amqpProtocol: AmqpProtocol = amqp
    // .host("localhost")
    // .port(5672)
    // .vhost("/")
    // .auth("guest", "guest")
    .uriString("amqp://guest:guest@localhost:5672")
    .vhost("/v1/gw")
    .poolSize(3)
    .externalConn
    .serverCert("server.pfx")
    .caCert("cacertjks")
    .declare(exchangePubSim)
    .declare(queueQ1)
    .bind(exchangePubSim, queueQ1)
    .confirmMode()

  // val body = Array.fill[Byte](1000*10)(1) // 1KB data for test
  val body = "{'x':1, bar: ${mykey}}}"
  // val req = PublishRequestAsync("q1", body).persistent

  // val b: Boolean = true
  val scn  = scenario("AMQP Publish(ack)").repeat(1000) {
      var t: AnyRef = new java.lang.Boolean(true)
      var m: java.util.Map[String, Object] = HashMap("head" -> t).asJava
      exec(session => {session.set("mykey", "2")}).
      exec(amqp("Publish").publish("gw", body = Right(body), headers = m))
  }

  setUp(scn.inject(rampUsers(3) over (1 seconds))).protocols(amqpProtocol)
}
