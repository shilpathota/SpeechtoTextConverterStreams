package com.learning.kafkastreams.voiceparser.services;

import com.learning.kafkastreams.voiceparser.model.ParsedVoiceCommand;

public class MockTranslateClient implements TranslateService {
    public ParsedVoiceCommand translate(ParsedVoiceCommand original) {
        return ParsedVoiceCommand.builder()
                .id(original.getId())
                .text("call juan")
                .probability(original.getProbability())
                .language(original.getLanguage())
                .build();
    }
}
