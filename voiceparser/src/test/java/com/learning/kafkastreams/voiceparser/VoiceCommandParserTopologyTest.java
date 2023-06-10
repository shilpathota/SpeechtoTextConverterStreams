package com.learning.kafkastreams.voiceparser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.BDDMockito;

import com.learning.kafkastreams.voiceparser.model.ParsedVoiceCommand;
import com.learning.kafkastreams.voiceparser.model.VoiceCommand;
import com.learning.kafkastreams.voiceparser.serdes.JsonSerde;
import com.learning.kafkastreams.voiceparser.services.SpeechToTextService;
import com.learning.kafkastreams.voiceparser.services.TranslateService;

import lombok.var;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;


@ExtendWith(MockitoExtension.class)
public class VoiceCommandParserTopologyTest {
	
	@Mock
	private SpeechToTextService speechToTextService;
	@Mock
	private TranslateService translateService;
	@InjectMocks
	private VoiceCommandParserTopology voiceCommandParserTopology;
	
	private TopologyTestDriver topologyTestDriver;
	private TestInputTopic<String,VoiceCommand> voiceCommandInputTopic;
	private TestOutputTopic<String,ParsedVoiceCommand> recognizedCommandOutputTopic;
	private TestOutputTopic<String,ParsedVoiceCommand> unrecognizedCommandOutputTopic;


	@BeforeEach
	void setUp() {
		var voiceCommandParserTopology = new VoiceCommandParserTopology(speechToTextService,translateService);
		var topology = voiceCommandParserTopology.createTopology();
		var props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

		topologyTestDriver = new TopologyTestDriver(topology,props);
		var voiceCommandJsonSerde = new JsonSerde<VoiceCommand>(VoiceCommand.class);
		var parsedvoiceCommandJsonSerde = new JsonSerde<ParsedVoiceCommand>(ParsedVoiceCommand.class);

		voiceCommandInputTopic = topologyTestDriver.createInputTopic(VoiceCommandParserTopology.VOICE_COMMANDS_TOPIC, Serdes.String().serializer(), voiceCommandJsonSerde.serializer());
		recognizedCommandOutputTopic = topologyTestDriver.createOutputTopic(VoiceCommandParserTopology.RECOGNIZED_COMMANDS_TOPIC, Serdes.String().deserializer(), parsedvoiceCommandJsonSerde.deserializer());
		unrecognizedCommandOutputTopic = topologyTestDriver.createOutputTopic(VoiceCommandParserTopology.UNRECOGNIZED_COMMANDS_TOPIC, Serdes.String().deserializer(), parsedvoiceCommandJsonSerde.deserializer());

	}
	@Test
	@DisplayName("Given English voice coemmand when processed correctly then I receive ParsedVoiceCommand in the recognized command")
	void testScenario1() {
		//Preconditions(given)
		byte[] randomBytes = new byte[20];
		new Random().nextBytes(randomBytes);
		var id = UUID.randomUUID().toString();
		var voiceCommand = VoiceCommand.builder()
					.id(id)
					.audio(randomBytes)
					.audioCodec("FLAC")
					.language("en-US")
					.build();
		var parsedVoiceCommand1 = ParsedVoiceCommand.builder()
								.id(voiceCommand.getId())
								.language("en-US")
								.text("call John")
								.probability(0.95)
								.build();
		BDDMockito.given(speechToTextService.speechToText(voiceCommand)).willReturn(parsedVoiceCommand1);
		//Actions(when)
		voiceCommandInputTopic.pipeInput(id,voiceCommand);
		//Verification(Then)
		var parsedVoiceCommand = recognizedCommandOutputTopic.readRecord().getValue();
		
		assertTrue(unrecognizedCommandOutputTopic.isEmpty());
		assertEquals(voiceCommand.getId(),parsedVoiceCommand.getId());
		assertEquals("call John",parsedVoiceCommand.getText());
		BDDMockito.verify(translateService, never()).translate(BDDMockito.any(ParsedVoiceCommand.class));
		
	}
	@Test
	@DisplayName("Spanish Voice Command converted to English and then assert it")
	void testScenario2() {
		//Preconditions(given)
				byte[] randomBytes = new byte[20];
				new Random().nextBytes(randomBytes);
				var id  = UUID.randomUUID().toString();
				var voiceCommand = VoiceCommand.builder()
							.id(id)
							.audio(randomBytes)
							.audioCodec("FLAC")
							.language("es-AR")
							.build();
				var parsedVoiceCommand1 = ParsedVoiceCommand.builder()
										.id(voiceCommand.getId())
										.language("es-AR")
										.text("llamar a Juan")
										.probability(0.95)
										.build();
				BDDMockito.given(speechToTextService.speechToText(voiceCommand)).willReturn(parsedVoiceCommand1);
				var translatedparsedVoiceCommand = ParsedVoiceCommand.builder()
						.id(voiceCommand.getId())
						.language("en-US")
						.text("call Juan")
						.probability(0.95)
						.build();
				BDDMockito.given(translateService.translate(parsedVoiceCommand1)).willReturn(translatedparsedVoiceCommand);
				//Actions(when)
				voiceCommandInputTopic.pipeInput(voiceCommand);
				//Verification(Then)
				var parsedVoiceCommand = recognizedCommandOutputTopic.readRecord().value();
				
				assertEquals(voiceCommand.getId(),parsedVoiceCommand.getId());
				assertEquals("call Juan",parsedVoiceCommand.getText());
	}
	@Test
	@DisplayName("Spanish Voice Command converted to English and then assert it")
	void testScenario3() {
		//Preconditions(given)
				byte[] randomBytes = new byte[20];
				new Random().nextBytes(randomBytes);
				var voiceCommand = VoiceCommand.builder()
							.id(UUID.randomUUID().toString())
							.audio(randomBytes)
							.audioCodec("FLAC")
							.language("en-US")
							.build();
				var parsedVoiceCommand1 = ParsedVoiceCommand.builder()
										.id(voiceCommand.getId())
										.language("es-AR")
										.text("llamar  a juan")
										.probability(0.30)
										.build();
				BDDMockito.given(speechToTextService.speechToText(voiceCommand)).willReturn(parsedVoiceCommand1);
				
				//Actions(when)
				voiceCommandInputTopic.pipeInput(voiceCommand);
				//Verification(Then)
				var parsedVoiceCommand = unrecognizedCommandOutputTopic.readRecord().value();
				
				assertEquals(voiceCommand.getId(),parsedVoiceCommand.getId());
				assertTrue(recognizedCommandOutputTopic.isEmpty());
				
				BDDMockito.verify(translateService,never()).translate(BDDMockito.any(ParsedVoiceCommand.class));		

	}
	@Test
	@DisplayName("Non recognized Voice Command ")
	void testScenario4() {
		//Preconditions(given)
		byte[] randomBytes = new byte[9];
		new Random().nextBytes(randomBytes);
		var id = UUID.randomUUID().toString();
		var voiceCommand = VoiceCommand.builder()
					.id(id)
					.audio(randomBytes)
					.audioCodec("FLAC")
					.language("en-US")
					.build();
		//Actions(when)
		voiceCommandInputTopic.pipeInput(id,voiceCommand);
		//Verification(Then)
		
		assertTrue(recognizedCommandOutputTopic.isEmpty());		
		assertTrue(unrecognizedCommandOutputTopic.isEmpty());
		BDDMockito.verify(speechToTextService,never()).speechToText(BDDMockito.any(VoiceCommand.class));
		BDDMockito.verify(translateService,never()).translate(BDDMockito.any(ParsedVoiceCommand.class));		
	}
}
