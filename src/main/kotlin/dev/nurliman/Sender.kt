package dev.nurliman

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.rabbitmq.OutboundMessage
import reactor.rabbitmq.QueueSpecification
import reactor.rabbitmq.RabbitFlux

fun main() {
    val QUEUE = "demo-queue"
    val COUNT = 20
    val logger = LoggerFactory.getLogger("Main")
    val sender = RabbitFlux.createSender()
    val latch = CountDownLatch(COUNT)

    val confirmations =
            sender.sendWithPublishConfirms(
                    Flux.range(1, COUNT).map { i ->
                        OutboundMessage("", QUEUE, "Message_$i".toByteArray())
                    }
            )

    sender.declareQueue(QueueSpecification.queue(QUEUE))
            .thenMany(confirmations)
            .doOnError { e -> logger.error("Send failed", e) }
            .subscribe { r ->
                if (r.isAck) {
                    logger.info("Message {} sent successfully", String(r.outboundMessage.body))
                }
                latch.countDown()
            }

    latch.await(10, TimeUnit.SECONDS)
    sender.close()
    logger.info("RabbitMQ connection closed")
}
