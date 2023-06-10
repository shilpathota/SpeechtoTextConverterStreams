package com.learning.kafkastreams.voiceparser.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class VoiceCommand {
	private String id;
	private byte[] audio;
	private String audioCodec;
	private String language;
	
}
