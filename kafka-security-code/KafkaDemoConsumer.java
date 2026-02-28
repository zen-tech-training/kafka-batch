package com.zensar;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaDemoConsumer {

	private static void simpleConsumer() {
		Consumer<String, String> kafkaConsumer = 
				new KafkaConsumer<String, String>(getSecureProperties());
		
		kafkaConsumer.subscribe(Arrays.asList("ZenTopic"));
		//OR
//		TopicPartition partition_0 = new TopicPartition("ZenTopic", 0);
//		TopicPartition partition_1 = new TopicPartition("ZenTopic", 1);
//		kafkaConsumer.assign(Arrays.asList(partition_0, partition_1));
		
		System.out.println("Consumer 1 started for ZenTopic");
		
		for(;;) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
			Iterator<ConsumerRecord<String, String>> itr = records.iterator();
			while(itr.hasNext()) {
				ConsumerRecord<String, String> record = itr.next();
				System.out.println("Message received " + record.value() + 
						"\tpartition=" + record.partition() + "\toffset=" + record.offset());
			}
		}
	}
	
	public static void main(String[] args) {
		simpleConsumer();
	}

	private static Properties getSecureProperties() {
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		 props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		 props.put("group.id", "test");
		 props.put("sasl.mechanism", "PLAIN");
		 props.put("security.protocol", "SASL_PLAINTEXT");
		 System.setProperty("java.security.auth.login.config", "src/main/resources/kafka_client_jaas.conf");
		 return props;
	}
	
	private static Properties getProperties() {
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092"); //List of message brokers
		 props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		 props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		 props.put("group.id", "consumer-group-1");
		 return props;
	}
	
}
