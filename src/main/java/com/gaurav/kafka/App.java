package com.gaurav.kafka;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.gaurav.kafka.constants.IKafkaConstants;
import com.gaurav.kafka.consumer.ConsumerCreator;
import com.gaurav.kafka.producer.ProducerCreator;

public class App {
	public static void main(String[] args) throws InterruptedException {
//		runProducer();
//		runConsumer();
		System.out.println("hello");
	}

	static void runConsumer() throws InterruptedException {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

		int noMessageToFetch = 0;

		while (true) {
			Random ram = new Random();
			Thread.sleep(ram.nextInt(2000));
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}

	static void runProducer() {
		Producer<Long, String> producer = ProducerCreator.createProducer();
		Long key_ = Long.valueOf(0);
		for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
//			int partition_number = index%3;
			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,"This is record no. "+ index);
			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
	}
}

//kafka-lag-exporter {
//		reporters.prometheus.port = 8000
//		reporters.prometheus.port = ${?KAFKA_LAG_EXPORTER_PORT}
//		poll-interval = 30 seconds
//		poll-interval = ${?KAFKA_LAG_EXPORTER_POLL_INTERVAL_SECONDS}
//		lookup-table-size = 60
//		lookup-table-size = ${?KAFKA_LAG_EXPORTER_LOOKUP_TABLE_SIZE}
//		client-group-id = "kafkalagexporter"
//		client-group-id = ${?KAFKA_LAG_EXPORTER_CLIENT_GROUP_ID}
//		kafka-client-timeout = 10 seconds
//		kafka-client-timeout = ${?KAFKA_LAG_EXPORTER_KAFKA_CLIENT_TIMEOUT_SECONDS}
//		clusters = [
//					{
//					name = "WATCHER"
//					bootstrap-brokers = "localhost:9092"
//					topic-whitelist = [
//					"demo-3"
//					]
//					consumer-properties = {
//					client.id = "consumer-client-id"
//					}
//					admin-client-properties = {
//					client.id = "admin-client-id"
//					}
//					labels = {
//					location = "ny"
//					zone = "us-east"
//					}
//					}
//				]
//		clusters = ${?KAFKA_LAG_EXPORTER_CLUSTERS}
//		watchers = {
//		strimzi = "false"
//		strimzi = ${?KAFKA_LAG_EXPORTER_STRIMZI}
//		}
//		metric-whitelist = [".*"]
//		}
//
//		akka {
//		loggers = ["akka.event.slf4j.Slf4jLogger"]
//		loglevel = "DEBUG"
//		logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
//		}
//
