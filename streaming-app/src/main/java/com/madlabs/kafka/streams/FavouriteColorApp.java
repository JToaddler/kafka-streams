package com.madlabs.kafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

public class FavouriteColorApp {

	static final String inputTopic = "fav-color-input";

	static final String userColorTopic = "user-color";

	static final String outputTopic = "fav-color-output";

	public static void main(String[] args) {

		Properties config = getStreamConfig();
		final KafkaStreams streams = new KafkaStreams(createFavColorStream(), config);
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	public static Properties getStreamConfig() {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-app");
		config.put(StreamsConfig.CLIENT_ID_CONFIG, "favourite-color-app");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.localhost.com:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1 * 1000);

		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return config;
	}

	public static Topology createFavColorStream() {

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> streams = builder.stream(inputTopic);
		KStream<String, String> userColorStream = streams.filter((key, value) -> value.contains(","))
				.selectKey((key, value) -> value.split(",")[0].toLowerCase())
				.mapValues((readOnlyKey, value) -> value.split(",")[1].toLowerCase())
				.filter((key, value) -> Arrays.asList("green", "blue", "red").contains(value));

		userColorStream.to(userColorTopic, Produced.with(Serdes.String(), Serdes.String()));

		KTable<String, String> userColorTable = builder.table(userColorTopic,
				Consumed.with(Serdes.String(), Serdes.String()));

		KTable<String, Long> colorCount = userColorTable.groupBy((user, color) -> new KeyValue<>(color, color))
				.count(Named.as("CountsByColor"));

		colorCount.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

		Topology topology = builder.build();
		System.out.println("Word Stream Topology : " + topology.describe());
		return builder.build();
	}

}
