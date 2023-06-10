package com.learning.kafkastreams.voiceparser;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import com.learning.kafkastreams.voiceparser.model.ParsedVoiceCommand;
import com.learning.kafkastreams.voiceparser.model.VoiceCommand;
import com.learning.kafkastreams.voiceparser.serdes.JsonSerde;
import com.learning.kafkastreams.voiceparser.services.SpeechToTextService;
import com.learning.kafkastreams.voiceparser.services.TranslateService;

import lombok.var;

public class VoiceCommandParserTopology {
	 public static final double THRESHOLD = 0.85;
	public static final String VOICE_COMMANDS_TOPIC = "voice-commands";
	public static final String RECOGNIZED_COMMANDS_TOPIC = "recognized-commands";
	public static final String UNRECOGNIZED_COMMANDS_TOPIC = "unrecognized-commands";
	private SpeechToTextService speechToTextService;
	private TranslateService translateService;
	
	public VoiceCommandParserTopology(SpeechToTextService speechToTextService,TranslateService translateService) {
		super();
		this.speechToTextService = speechToTextService;
		this.translateService=translateService;
	}


	public Topology createTopology() {
		var streamsBuilder = new StreamsBuilder();
		
		var voiceCommandsJsonSerde = new JsonSerde<>(VoiceCommand.class);
		var parsedVoiceCommandJsonSerde = new JsonSerde<>(ParsedVoiceCommand.class);
		var branches = streamsBuilder.stream(VOICE_COMMANDS_TOPIC,Consumed.with(Serdes.String(),voiceCommandsJsonSerde))
						.filter((key,value)->value.getAudio().length >= 10)
						.mapValues((readOnlyKey,value)->speechToTextService.speechToText(value))
						.split(Named.as("branches-"))
						.branch((key,value)->value.getProbability() > THRESHOLD, Branched.as("recognized"))
						.defaultBranch(Branched.as("unrecognized"));
		branches.get("branches-unrecognized")
				.to(UNRECOGNIZED_COMMANDS_TOPIC,Produced.with(Serdes.String(), parsedVoiceCommandJsonSerde));
		var streamsMap = branches.get("branches-recognized").split(Named.as("language-"))
						.branch((key,value)->value.getLanguage().startsWith("en"),Branched.as("english"))
						.defaultBranch(Branched.as("non-english"));
		streamsMap.get("language-non-english")
						.mapValues((readOnlyKey,value)-> translateService.translate(value))
						.merge(streamsMap.get("language-english"))
						.to(RECOGNIZED_COMMANDS_TOPIC,Produced.with(Serdes.String(), parsedVoiceCommandJsonSerde));
		return streamsBuilder.build();
	}
}
