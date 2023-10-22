package com.madlabs.kafka.streams;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
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

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

public class PageViewRegionLambdaExample {

	static final String bootstrapServers = "vm.localhost:9092";
	static final String schemaRegistryUrl = "http://vm.localhost:8081";

	public static void main(String[] args) throws IOException {

		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();

		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, GenericRecord> views = builder.stream("PageViews");

		final KStream<String, GenericRecord> viewsByUser = views
				.map((dummy, record) -> new KeyValue<>(record.get("user").toString(), record));

		final KTable<String, GenericRecord> userProfiles = builder.table("UserProfiles");

		final KTable<String, String> userRegions = userProfiles.mapValues(record -> record.get("region").toString());

		final InputStream pageViewRegionSchema = PageViewRegionLambdaExample.class.getClassLoader()
				.getResourceAsStream("avro/io/confluent/examples/streams/pageviewregion.avsc");
		final Schema schema = new Schema.Parser().parse(pageViewRegionSchema);

		final KTable<Windowed<String>, Long> viewsByRegion = viewsByUser.leftJoin(userRegions, (view, region) -> {
			final GenericRecord viewRegion = new GenericData.Record(schema);
			viewRegion.put("user", view.get("user"));
			viewRegion.put("page", view.get("page"));
			viewRegion.put("region", region);
			return viewRegion;
		}).map((user, viewRegion) -> new KeyValue<>(viewRegion.get("region").toString(), viewRegion)).groupByKey()
				.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
				.count();

		final KStream<String, Long> viewsByRegionForConsole = viewsByRegion
				.toStream((windowedRegion, count) -> windowedRegion.toString());

		viewsByRegionForConsole.to("PageViewsByRegion", Produced.with(stringSerde, longSerde));

		final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamConfig());
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	public static Properties getStreamConfig() {
		final Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pageview-region-lambda-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "pageview-region-lambda-example-client");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		streamsConfiguration.put("schema.registry.url", schemaRegistryUrl);
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
		streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		return streamsConfiguration;
	}
}
