package com.zensar;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class WordCountProcessor {

	public static void main(String[] args) {
		StreamsBuilder streamBuilder = new StreamsBuilder();
		KStream<String, String> kstream = streamBuilder.stream("SentenceTopic"); //Source Topic
		
		//processing code
		KStream<String, Long> output_kstream = 
			kstream
			.flatMapValues((sentence)->Arrays.asList(sentence.toLowerCase().split(" ")))
			.groupBy((key, word)->word)
			.count()
			.toStream();
		
		
		output_kstream.to("WordCountTopic"); //Sink Topic
		Produced.with(Serdes.String(), Serdes.Long());
		
		Topology topology = streamBuilder.build();
		KafkaStreams kafkaStreams = new KafkaStreams(topology, getProperties());
		kafkaStreams.start();
		System.out.println("WordCount processor started...");
		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}

	private static Properties getProperties() {
		 Properties props = new Properties();
		 props.put(StreamsConfig.APPLICATION_ID_CONFIG, "WORD_COUNT_PROCESSOR_APP");
		 props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //List of message brokers
		 props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		 props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		 
		 return props;
	}
	
}
