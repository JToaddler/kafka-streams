package com.madlabs.kafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class WordCountStreamApp {

	static final String inputTopic = "stream-plaintext-input";
	static final String outputTopic = "stream-wordcount-output";

	public static void main(String[] args) {

		final Properties streamsConfiguration = getStreamsConfiguration();
		final StreamsBuilder builder = new StreamsBuilder();

		Topology topology = createWordCountStream(builder);
		
		final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

		streams.start();
		System.out.println("Word Stream Topology : "+topology.describe());
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	public static Properties getStreamsConfiguration() {
		Properties prop = new Properties();
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
		prop.put(StreamsConfig.CLIENT_ID_CONFIG, "word-count-app");

		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		prop.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);
		return prop;
	}

	static Topology createWordCountStream(final StreamsBuilder builder) {

		final KStream<String, String> textLines = builder.stream(inputTopic);
		final KTable<String, Long> wordCount = textLines.mapValues(value -> value.toLowerCase())
				.flatMapValues(value -> Arrays.asList(value.split(" "))).groupBy((keyIgnore, count) -> {
					System.out.println("Key :"+ keyIgnore + " Count :"+count);
					return count;
				}).count();

		wordCount.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
		return builder.build();

	}

}
