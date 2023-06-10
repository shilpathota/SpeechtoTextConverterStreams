package producer;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learning.kafkastreams.voiceparser.VoiceCommandParserTopology;
import com.learning.kafkastreams.voiceparser.model.VoiceCommand;
import com.learning.kafkastreams.voiceparser.serdes.JsonSerde;

import lombok.SneakyThrows;

public class VoiceCommandProducer {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @SneakyThrows
    public static void main(String[] args) {
        Map<String, Object> props = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        var voiceCommandKafkaProducer = new KafkaProducer<>(props, Serdes.String().serializer(), new JsonSerde<>(VoiceCommand.class).serializer());

        Stream.of(OBJECT_MAPPER.readValue(VoiceCommandProducer.class.getClassLoader().getResourceAsStream("data/test-data.json"), VoiceCommand[].class))
                .map(voiceCommand -> new ProducerRecord<>(VoiceCommandParserTopology.VOICE_COMMANDS_TOPIC, voiceCommand.getId(), voiceCommand))
                .map(voiceCommandKafkaProducer::send)
                .forEach(VoiceCommandProducer::waitForProducer);

    }

    @SneakyThrows
    private static void waitForProducer(Future<RecordMetadata> recordMetadataFuture) {
        recordMetadataFuture.get();
    }
}