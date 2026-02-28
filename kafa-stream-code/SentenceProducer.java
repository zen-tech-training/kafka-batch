package com.zensar;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SentenceProducer {

	private static void simpleProducer() {
		Producer<String, String> kafkaProducer = 
				new KafkaProducer<String, String>(getProperties());
		ProducerRecord<String, String> message_1 = 
				new ProducerRecord<String, String>("SentenceTopic", "zensar.messages", "Hello all, how is the Kafka");
		ProducerRecord<String, String> message_2 = 
				new ProducerRecord<String, String>("SentenceTopic", "zensar.messages", "Hi all, Kafka is very simple");
		kafkaProducer.send(message_1);
		kafkaProducer.send(message_2);
		kafkaProducer.close();
		System.out.println("Messages sentences produced");
	}
	
	public static void main(String[] args) {
		simpleProducer();
	}
	private static Properties getProperties() {
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092"); //List of message brokers
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 return props;
	}

}
