package com.example.kafka;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

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

import com.example.Ple;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class DemoConsumer implements InitializingBean{
	private static final Logger LOG = LoggerFactory.getLogger(DemoConsumer.class);

	private static final int CAPACITY = 10;

	@Autowired
	private DataSource dataSource;
	
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
			ObjectMapper mapper = new ObjectMapper();
			do {
				ConsumerRecords<String, String> records;

				records = consumer.poll(100);

				for (ConsumerRecord<String, String> record : records) {
					System.out.println("message reçu : topic :" + record.topic() + " msg : " + record.value());
					LOG.debug("offset={}, key={}, value={}", record.offset(), record.key(), record.value());
					
					Thread.sleep(1000);
					
					
					Ple ple = mapper.readValue(record.value(), Ple.class);
					
					Connection connection = dataSource.getConnection();

					PreparedStatement stmt = connection.prepareStatement("insert into salesforce.ple_idx (siren,sfid, step, state, ididx) values (?,?,?,?,?)" );
					stmt.setString(1, ple.getInsee());
					stmt.setString(2, ple.getSfid());
					stmt.setString(3, "pricing");
					stmt.setString(4,"ok");
					stmt.setString(5, ple.getIdxDate()+"");
					int resutl = stmt.executeUpdate();
					
				//	connection.commit();
				
					
				
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
