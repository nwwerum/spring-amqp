/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verify that a channel transacted {@link RabbitTemplate} used within a
 * {@code PROPAGATION_REQUIRES_NEW} transaction in a listener does not take over the
 * consumer channel of the container: the inner transaction must be independent from
 * the listener's own transaction and must neither close, commit nor roll back the
 * consumer channel.
 *
 * @author Norbert Wirges
 *
 * @since 4.2
 */
class RequiresNewTransactionConsumerChannelTests extends AbstractTestContainerTests {

	private static final String IN = "test.requires.new.in";

	private static final String OUT = "test.requires.new.out";

	private final CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost", amqpPort());

	private final RabbitAdmin admin = new RabbitAdmin(this.connectionFactory);

	private final ExternalTxManagerTests.DummyTxManager transactionManager =
			new ExternalTxManagerTests.DummyTxManager();

	private final TransactionTemplate requiresNew = new TransactionTemplate(this.transactionManager);

	private final RabbitTemplate template = new RabbitTemplate(this.connectionFactory);

	@BeforeEach
	void setUp() {
		// With a cache of one channel, closing a channel physically closes it, when the cache is full.
		this.connectionFactory.setChannelCacheSize(1);
		this.admin.deleteQueue(IN);
		this.admin.deleteQueue(OUT);
		this.admin.declareQueue(new Queue(IN));
		this.admin.declareQueue(new Queue(OUT));
		this.requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
		this.template.setChannelTransacted(true);
	}

	@AfterEach
	void tearDown() {
		this.admin.deleteQueue(IN);
		this.admin.deleteQueue(OUT);
		this.connectionFactory.destroy();
	}

	@Test
	void consumerChannelIsNotClosedByInnerTransaction() throws InterruptedException {
		AtomicInteger deliveries = new AtomicInteger();
		AtomicInteger redeliveries = new AtomicInteger();
		SimpleMessageListenerContainer container = createContainer((message, channel) -> {
			deliveries.incrementAndGet();
			if (message.getMessageProperties().isRedelivered()) {
				redeliveries.incrementAndGet();
			}
			this.requiresNew.executeWithoutResult(status -> this.template.convertAndSend(OUT, "inner"));
		});
		container.start();
		try {
			await().until(() -> container.getActiveConsumerCount() == 1);
			// returns a transactional channel to the cache while the consumer holds its own, which makes it full
			this.template.execute(channel -> null);
			new RabbitTemplate(this.connectionFactory).convertAndSend(IN, "test");

			Message received = new RabbitTemplate(this.connectionFactory).receive(OUT, 10_000);
			assertThat(received).isNotNull();
			// A closed consumer channel fails the ack of the listener's transaction and
			// the message is redelivered after the container restarted the consumer.
			await().during(Duration.ofSeconds(2))
					.atMost(Duration.ofSeconds(10))
					.untilAsserted(() -> {
						assertThat(deliveries).hasValue(1);
						assertThat(redeliveries).hasValue(0);
					});
		}
		finally {
			container.stop();
		}
	}

	@Test
	void innerTransactionDoesNotCommitWorkOfListenerTransaction() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = createContainer((message, channel) -> {
			// sent in the listener's transaction, which is rolled back below
			this.template.convertAndSend(OUT, "outer");
			this.requiresNew.executeWithoutResult(status -> this.template.convertAndSend(OUT, "inner"));
			latch.countDown();
			throw new AmqpRejectAndDontRequeueException("intentional: roll back the listener's transaction");
		});
		container.start();
		try {
			new RabbitTemplate(this.connectionFactory).convertAndSend(IN, "test");
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

			RabbitTemplate receiver = new RabbitTemplate(this.connectionFactory);
			Message inner = receiver.receive(OUT, 10_000);
			assertThat(inner).isNotNull();
			assertThat(inner.getBody()).asString().isEqualTo("inner");
			assertThat(receiver.receive(OUT, 1000))
					.withFailMessage("the work of the rolled back listener transaction was committed")
					.isNull();
		}
		finally {
			container.stop();
		}
	}

	private SimpleMessageListenerContainer createContainer(ChannelAwareMessageListener listener) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
		container.setQueueNames(IN);
		container.setChannelTransacted(true);
		container.setTransactionManager(this.transactionManager);
		container.setRecoveryBackOff(new FixedBackOff(200, FixedBackOff.UNLIMITED_ATTEMPTS));
		container.setMessageListener(listener);
		container.afterPropertiesSet();
		return container;
	}

}
