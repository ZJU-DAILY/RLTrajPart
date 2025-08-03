package funs;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import objects.Trajectory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class StateProcess {
    private int parallelism;
    private List<Double> gridBBox;
    private StreamExecutionEnvironment env;
    private Properties kafkaProperties;
    private String stateTopic;

    private Integer stateSize;

    public StateProcess(int parallelism, List<Double> gridBBox,
                        StreamExecutionEnvironment env, Properties kafkaProperties, String stateTopic,Integer stateSize) {
        this.parallelism = parallelism;
        this.gridBBox = gridBBox;
        this.env = env;
        this.stateSize = stateSize;
        this.kafkaProperties = kafkaProperties;
        this.stateTopic = stateTopic;
    }

    public void run(DataStream<Trajectory> trajectoryStream) {


        trajectoryStream = trajectoryStream.filter(value -> value.getTimestamp() >= 1201930244000L).startNewChain();

        DataStream<Tuple5<String, Integer, Long, Integer, Long>> stateStream = trajectoryStream.map(value ->
                new Tuple5<>(
                        value.getGridID(),
                        value.getStateIndex(),
                        value.getTimestamp(),
                        value.getGridSize(),
                        value.getGridMemorySize()
                )
        ).returns(TypeInformation.of(new TypeHint<Tuple5<String, Integer, Long, Integer, Long>>() {}));

        DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Long>> subtaskStream = stateStream
                .keyBy(value -> value.f0)
                .map(new RichMapFunction<Tuple5<String, Integer, Long, Integer, Long>, Tuple6<Integer, Integer, Long, Long, Integer, Long>>() {
                    @Override
                    public Tuple6<Integer, Integer, Long, Long, Integer, Long> map(Tuple5<String, Integer, Long, Integer, Long> value) throws Exception {
                        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
                        return new Tuple6<>(subTaskId, value.f1, value.f2, 1L, value.f3, value.f4);
                    }
                });


        List<Tuple6<Integer, Integer, Long, Long, Integer, Long>> initialData = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            initialData.add(new Tuple6<>(i, -1, 1201930244000L, 0L, 0, 0L));
        }

        DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Long>> initialDataStream = env.fromCollection(initialData);

        DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Long>> initialSubtaskStream = subtaskStream.union(initialDataStream)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple6<Integer, Integer, Long, Long, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1)) 
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple6<Integer, Integer, Long, Long, Integer, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple6<Integer, Integer, Long, Long, Integer, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                                .withIdleness(Duration.ofSeconds(1))
                );


        DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Long>> heartbeatStream = initialSubtaskStream
                .keyBy(value -> value.f0)
                .process(new OptimizedHeartbeatProcessFunction(10000));

        SingleOutputStreamOperator<Tuple6<Integer, Long, Long, Integer, Long, int[]>> stateAndTaskStream = heartbeatStream.keyBy(value -> value.f0)
                .process(new SubtaskAndStateCountFunction(stateSize));

        kafkaProperties.setProperty("batch.size", "0");
        kafkaProperties.setProperty("linger.ms", "0");


        KafkaSink<Tuple6<Integer, Long, Long, Integer, Long, int[]>> stateAndTaskKafkaSink = KafkaSink.<Tuple6<Integer, Long, Long, Integer, Long, int[]>>builder()
                .setBootstrapServers(kafkaProperties.getProperty("bootstrap.servers"))
                .setKafkaProducerConfig(kafkaProperties)
                .setRecordSerializer(new StateAndTaskCountSerializationSchema1(stateTopic))
                .build();

        stateAndTaskStream.sinkTo(stateAndTaskKafkaSink);

    }
}


