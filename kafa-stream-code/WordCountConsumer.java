package com.zensar;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class WordCountConsumer {

	private static void simpleConsumer() {
		Consumer<String, Long> kafkaConsumer = 
				new KafkaConsumer<String, Long>(getProperties());
		
		kafkaConsumer.subscribe(Arrays.asList("WordCountTopic"));
		
		System.out.println("WordCountTopic Consumer started...");
		
		for(;;) {
			ConsumerRecords<String, Long> records = kafkaConsumer.poll(Duration.ofSeconds(1));
			Iterator<ConsumerRecord<String, Long>> itr = records.iterator();
			while(itr.hasNext()) {
				ConsumerRecord<String, Long> record = itr.next();
				System.out.println(record.key() + " - " + record.value()
						);
			}
		}
	}
	
	public static void main(String[] args) {
		simpleConsumer();
	}
	private static Properties getProperties() {
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092"); //List of message brokers
		 props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		 props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		 props.put("group.id", "consumer-group-2");
		 return props;
	}

}
