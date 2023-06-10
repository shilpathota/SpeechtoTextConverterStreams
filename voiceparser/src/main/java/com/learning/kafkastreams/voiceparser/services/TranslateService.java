package com.learning.kafkastreams.voiceparser.services;

import com.learning.kafkastreams.voiceparser.model.ParsedVoiceCommand;

public interface TranslateService {
	ParsedVoiceCommand translate(ParsedVoiceCommand original);
}
