package com.learning.kafkastreams.voiceparser;

import org.apache.kafka.streams.KafkaStreams;

import com.learning.kafkastreams.voiceparser.services.MockSttClient;
import com.learning.kafkastreams.voiceparser.services.MockTranslateClient;

import configuration.StreamsConfiguration;

public class VoiceCommandParserApp {
	public static void main(String[] args) {
		var voiceParserTopology=new VoiceCommandParserTopology(new MockSttClient(), new MockTranslateClient());
		var streamsConfiguration = new StreamsConfiguration();
		var kafkaStreams = new KafkaStreams(voiceParserTopology.createTopology(), streamsConfiguration .streamsConfiguration());

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}
}
