package funs;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class StateAndTaskCountSerializationSchema1 implements KafkaRecordSerializationSchema<Tuple6<Integer, Long, Long,Integer,Long, int[]>> {
    private final String topic;

    public StateAndTaskCountSerializationSchema1(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple6<Integer, Long, Long,Integer,Long, int[]> element, KafkaSinkContext context, @Nullable Long timestamp) {
        String subtaskId = String.valueOf(element.f0);
        String time = String.valueOf(element.f1);
        String taskCount = String.valueOf(element.f2);
        String gridSize = String.valueOf(element.f3);
        String indexSize = String.valueOf(element.f4);
        String stateArrayAsString = Arrays.toString(element.f5);
        String combinedString = subtaskId + "," + time + "," + taskCount + ","+ gridSize +"," + indexSize +"," + stateArrayAsString ;

        byte[] value = combinedString.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topic, null, value);
    }
}
