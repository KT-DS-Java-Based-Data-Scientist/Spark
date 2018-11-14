package com.ktds.streaming;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

// Kafka에서 batch로 데이터 보내는
public class KafkaSender {

	private static Properties makeProps() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "kafka-consumer-group-batch");
		props.put("value.serializer", StringSerializer.class);
		props.put("key.serializer", StringSerializer.class);

		return props;
	}

	private static KafkaProducer<String, String> makeProducer() {
		KafkaProducer<String, String> producer = new KafkaProducer<>(makeProps());
		return producer;
	}

	public static void send(String message) {
		KafkaProducer<String, String> producer = makeProducer();
		//ProducerRecord<String, String> record = new ProducerRecord<>("logTopic", message);
		ProducerRecord<String, String> record = new ProducerRecord<>("BatchTopic", message);

		producer.send(record);
		producer.close();
	}
	
	public static void send(String topic, String message) {
		KafkaProducer<String, String> producer = makeProducer();
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

		producer.send(record);
		producer.close();
	}

}