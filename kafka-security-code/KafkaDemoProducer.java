package com.zensar;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaDemoProducer {

	
	private static Properties getProperties() {
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092"); //List of message brokers
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 
		 return props;
	}

	private static Properties getSecureProperties() {
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("sasl.mechanism", "PLAIN");
		 props.put("security.protocol", "SASL_PLAINTEXT");
		 System.setProperty("java.security.auth.login.config", "src/main/resources/kafka_client_jaas.conf");
		 return props;
	}
	
	private static void asynchronousProducer() throws Exception {
		Callback callback = new MessageAcknowledgementCallback();
		Producer<String, String> kafkaProducer = 
				new KafkaProducer<String, String>(getProperties());
		ProducerRecord<String, String> message_1 = 
				new ProducerRecord<String, String>("ZenTopic", "zensar.messages", "Async message 1");
		ProducerRecord<String, String> message_2 = 
				new ProducerRecord<String, String>("ZenTopic", "zensar.messages", "Async message 2");
		
		kafkaProducer.send(message_1, callback);
		kafkaProducer.send(message_2, callback);
		
		kafkaProducer.close();
	}
	
	static class MessageAcknowledgementCallback implements Callback {

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			System.out.println("Callback Message acknowledgement - offset=" 
					+ metadata.offset() + "\tpartition=" + metadata.partition());
		}
		
	}
	private static void synchronousProducer() throws Exception {
		Producer<String, String> kafkaProducer = 
				new KafkaProducer<String, String>(getProperties());
		ProducerRecord<String, String> message_1 = 
				new ProducerRecord<String, String>("ZenTopic", "zensar.messages", "Sync message 1");
		ProducerRecord<String, String> message_2 = 
				new ProducerRecord<String, String>("ZenTopic", "zensar.messages", "Sync message 2");
		Future<RecordMetadata> futureAck_1 = kafkaProducer.send(message_1);
		Future<RecordMetadata> futureAck_2 = kafkaProducer.send(message_2);
		
		RecordMetadata recordMetadata_1 = futureAck_1.get(); //synchronous call
		RecordMetadata recordMetadata_2 = futureAck_2.get(); //synchronous call
		
		System.out.println("Message 1 acknowledgement - offset=" 
				+ recordMetadata_1.offset() + "\tpartition=" + recordMetadata_1.partition());
		System.out.println("Message 2 acknowledgement - offset=" 
				+ recordMetadata_2.offset() + "\tpartition=" + recordMetadata_2.partition());
		kafkaProducer.close();
	}

	private static void secureProducer() {
		Producer<String, String> kafkaProducer = 
				new KafkaProducer<String, String>(getSecureProperties());
		ProducerRecord<String, String> message_1 = 
				new ProducerRecord<String, String>("ZenTopic", "zensar.messages", "Hello from producer 1");;
		ProducerRecord<String, String> message_2 = 
				new ProducerRecord<String, String>("ZenTopic", "zensar.messages", "How is Kafka training going?");;
		kafkaProducer.send(message_1);
		kafkaProducer.send(message_2);
		kafkaProducer.close();
		System.out.println("Secure Messages produced");
	}
	
	
	private static void simpleProducer() {
		Producer<String, String> kafkaProducer = 
				new KafkaProducer<String, String>(getProperties());
		ProducerRecord<String, String> message_1 = 
				new ProducerRecord<String, String>("ZenTopic", "zensar.messages", "Hello from producer 1");;
		ProducerRecord<String, String> message_2 = 
				new ProducerRecord<String, String>("ZenTopic", "zensar.messages", "How is Kafka training going?");;
		kafkaProducer.send(message_1);
		kafkaProducer.send(message_2);
		kafkaProducer.close();
		System.out.println("Messages produced");
	}
	
	
	public static void main(String[] args) throws Exception {
		secureProducer();
		//simpleProducer();
		//synchronousProducer();
		//asynchronousProducer();
	}

}
