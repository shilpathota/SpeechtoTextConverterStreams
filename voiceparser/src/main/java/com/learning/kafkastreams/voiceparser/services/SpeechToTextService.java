package com.learning.kafkastreams.voiceparser.services;

import com.learning.kafkastreams.voiceparser.model.ParsedVoiceCommand;
import com.learning.kafkastreams.voiceparser.model.VoiceCommand;

public interface SpeechToTextService {
	ParsedVoiceCommand speechToText(VoiceCommand voiceCommand);
}
