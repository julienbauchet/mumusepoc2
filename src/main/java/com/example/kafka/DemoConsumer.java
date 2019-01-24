package com.example.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DemoConsumer implements InitializingBean{
	private static final Logger LOG = LoggerFactory.getLogger(DemoConsumer.class);

	private static final int CAPACITY = 10;

	@Autowired
	private  KafkaConfig config;

	private ExecutorService executor;

	private final AtomicBoolean running = new AtomicBoolean();

	private CountDownLatch stopLatch;

	private KafkaConsumer<String, String> consumer;

	public DemoConsumer(KafkaConfig config) {
		this.config = config;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		executor = Executors.newSingleThreadExecutor();
		executor.submit(this::loop);
		running.set(true);
		stopLatch = new CountDownLatch(1);
	}

	private void loop() {
		try {
			LOG.info("starting " + config.getTopic());
			Properties properties = config.getProperties();
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getConsumerGroup());
			properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
			
			properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
			properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

			consumer = new KafkaConsumer<>(properties);
			
			

			consumer.subscribe(Collections.singletonList(config.getTopic()));
			LOG.info("started " + config.getTopic());

			do {
				ConsumerRecords<String, String> records;

				records = consumer.poll(100);

				for (ConsumerRecord<String, String> record : records) {
					System.out.println("message reçu : topic :" + record.topic() + " msg : " + record.value());
					LOG.debug("offset={}, key={}, value={}", record.offset(), record.key(), record.value());
					
					Thread.sleep(1000);
				
					consumer.commitSync();
				}
			} while (running.get());

			LOG.info("closing consumer");
			consumer.close();
			stopLatch.countDown();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}

	


}
