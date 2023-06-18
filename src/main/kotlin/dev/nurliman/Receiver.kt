package dev.nurliman

import org.slf4j.LoggerFactory
import reactor.rabbitmq.RabbitFlux

fun main() {
    val QUEUE = "demo-queue"
    val logger = LoggerFactory.getLogger("Main")
    val receiver = RabbitFlux.createReceiver()
    val sender = RabbitFlux.createSender()

    val disposable =
            receiver.consumeAutoAck(QUEUE).subscribe { m ->
                logger.info("Received message {}", String(m.getBody()))
            }

    Runtime.getRuntime()
            .addShutdownHook(
                    Thread {
                        disposable.dispose()
                        sender.close()
                        receiver.close()
                        logger.info("RabbitMQ connection closed")
                    }
            )
}
