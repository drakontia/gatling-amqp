package io.gatling.amqp.config

import akka.actor.ActorSystem
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultSaslConfig
import com.typesafe.scalalogging.StrictLogging
import io.gatling.amqp.data._
import io.gatling.amqp.event._
import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.protocol.{Protocol, ProtocolKey}
import io.gatling.core.stats.StatsEngine
import java.security.KeyStore
import javax.net.ssl._
import java.security.cert.{CertificateFactory, X509Certificate}
import java.security.{KeyPair, KeyStore, Security}

object AmqpProtocol {
  val AmqpProtocolKey = new ProtocolKey {

    override type Protocol = AmqpProtocol
    override type Components = AmqpComponents
    def protocolClass: Class[io.gatling.core.protocol.Protocol] = classOf[AmqpProtocol].asInstanceOf[Class[io.gatling.core.protocol.Protocol]]

    def defaultProtocolValue(configuration: GatlingConfiguration): AmqpProtocol = AmqpProtocol(configuration)

    def newComponents(system: ActorSystem, coreComponents: CoreComponents): AmqpProtocol => AmqpComponents = {
      amqpProtocol => {
        val amqpComponents = AmqpComponents(amqpProtocol)
        val statsEngine: StatsEngine = coreComponents.statsEngine
        amqpProtocol.setupVariables(system, statsEngine)
        amqpProtocol.warmUp(system, statsEngine, coreComponents.throttler)
        amqpComponents
      }
    }
  }

  def apply(conf: GatlingConfiguration, connection: Connection, preparings: List[AmqpChannelCommand]): AmqpProtocol = new AmqpProtocol(connection, preparings)

  def apply(conf: GatlingConfiguration): AmqpProtocol = AmqpProtocol(connection = null, preparings = null)
}

/**
 * Wraps a AMQP protocol configuration
 */
case class AmqpProtocol(
  connection: Connection,
  preparings: List[AmqpChannelCommand]
) extends Protocol with AmqpVariables with AmqpPreparation with AmqpTermination with AmqpRunner with StrictLogging {
  lazy val event: AmqpEventBus = new AmqpEventBus()  // not used yet cause messages seems in random order in the bus

  /**
   * create new AMQP connection
   */
  def newConnection: com.rabbitmq.client.Connection = {
    import connection._
    val factory = new ConnectionFactory()
    factory.setHost(host)
    factory.setPort(port)
    factory.setVirtualHost(vhost)
    factory.setUri(uriString)
    isExternal match {
      case true =>
        factory.setSaslConfig(DefaultSaslConfig.EXTERNAL)
        factory.useSslProtocol(getSSLContext)
        factory.enableHostnameVerification
      case false =>
        factory.setUsername(user)
        factory.setPassword(password)
    }
    factory.newConnection
  }

  /**
   * validate variables
   */
  def validate(): Unit = {
    connection.validate()
  }

  /**
   * Whether is AMQP channel used for confirmation mode? (RabbitMQ feature)
   */
  def isConfirmMode: Boolean = connection.confirm

  /**
   * Whether is AMQP channel used for external connection? (RabbitMQ feature)
   */
  def isExternal: Boolean = connection.external

  /**
   * warmUp AMQP protocol (invoked by gatling framework)
   */
  def warmUp(system: ActorSystem, statsEngine: StatsEngine, throttler: Throttler): Unit = {
    logger.info("amqp: warmUp start")
    awaitPreparation()
  }

  /**
   * Build SSLContext with server key, certificate and CA certificate
   */
  def getSSLContext: SSLContext = {

    // Create and initialize the SSLContext with key material
    val passphrase = "changeit".toCharArray()
    // First initialize the key and trust material
    val ks = KeyStore.getInstance("PKCS12")
    val keystoreResource = this.getClass.getClassLoader.getResourceAsStream(connection.servercert)
    ks.load(keystoreResource, passphrase)

    // KeyManagers decide which key material to us
    val kmf = KeyManagerFactory.getInstance("SunX509")
    kmf.init(ks, passphrase)

    // TrustManagers decide whether to allow connections
    val tks = KeyStore.getInstance("JKS");
    val trustPassphrase = "changeit".toCharArray()
    val rootCaKeystoreResource = this.getClass.getClassLoader.getResourceAsStream(connection.cacert)
    tks.load(rootCaKeystoreResource, trustPassphrase)

    val tmf = TrustManagerFactory.getInstance("SunX509")
    tmf.init(tks)

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(kmf.getKeyManagers, tmf.getTrustManagers, null)
    sslContext
  }

  override def toString: String = {
    s"AmqpProtocol(hashCode=$hashCode)"
  }
}
