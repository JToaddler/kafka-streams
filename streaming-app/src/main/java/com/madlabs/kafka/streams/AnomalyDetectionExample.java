package com.madlabs.kafka.streams;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class AnomalyDetectionExample {

	static final String userClicks = "UserClicks";
	static final String anomalousUsersTopic = "AnomalousUsers";

	public static void main(String[] args) {

		final StreamsBuilder builder = new StreamsBuilder();

		final KStream<String, String> views = builder.stream(userClicks);

		KTable<Windowed<String>, Long> anomalousUsers = views
				.map((ignoreKey, userName) -> new KeyValue<>(userName, userName)).groupByKey()
				.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))).count()
				.filter((windowedUserId, count) -> count >= 3);

		final KStream<String, Long> anomalousUsersForConsole = anomalousUsers.toStream()
				.filter((windowedUserId, count) -> count != null)
				.map((windowedUserId, count) -> new KeyValue<>(windowedUserId.key(), count));

		anomalousUsersForConsole.to(anomalousUsersTopic, Produced.with(Serdes.String(), Serdes.Long()));

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamConfig());

		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	public static Properties getStreamConfig() {
		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "vm.localhost:9092");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-detection-lambda-example");
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "anomaly-detection-lambda-example-client");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
		return props;
	}

}
