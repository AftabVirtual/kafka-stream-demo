package com.github.simplesteph.udemy.kafka.streams;

import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class WordCountApp {
	public static void main(String[] args) {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		KStreamBuilder builder = new KStreamBuilder();

		KStream<String, String> textLines = builder.stream("input.topic");
		KTable<String, Long> wordCounts = textLines.mapValues(textLine -> textLine.toLowerCase())
				.flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+"))).selectKey((key, word) -> word)
				.groupByKey().count("Counts");
		wordCounts.to(Serdes.String(), Serdes.Long(), "output.topic");
		KafkaStreams streams = new KafkaStreams(builder, config);
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		while (true) {
			//System.out.println(streams.toString());
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				break;
			}
		}

	}
}
