package job;

import funs.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import index.GridIndex;
import index.UpdatableGridIndex;
import objects.Trajectory;

import tasks.cluster.ClusterSearch;
import utils.Params;
import utils.RateLimiterFunction;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

public class TraCluSearch {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "8082");
        final StreamExecutionEnvironment env;
        Params params = new Params();
        System.out.println(params);
        boolean onCluster = params.clusterMode;
        String bootStrapServers = params.kafkaBootStrapServers;

        String inputTopicName = params.inputTopicName;
        String dateFormatStr = params.dateFormatStr;
        List<Double> gridBBox = params.gridBBox;


        int parallelism = params.parallelism;
        double radius = params.queryRadius; 

        /* Windows */
        int windowSize = params.windowInterval;

        double gridMinX = gridBBox.get(0);
        double gridMinY = gridBBox.get(1);
        double gridMaxX = gridBBox.get(2);
        double gridMaxY = gridBBox.get(3);
        String stateTopic = "StateTopic";

        DateFormat inputDateFormat;

        if (dateFormatStr.equals("null"))
            inputDateFormat = null;
        else
            inputDateFormat = new SimpleDateFormat(dateFormatStr);

        if (onCluster) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();

        } else {
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        }

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);
        kafkaProperties.setProperty("group.id", "messageStream");

        env.setParallelism(parallelism);
        String topic = "GridSizeTopic";


        KafkaSource<String> updateSignalKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaProperties.getProperty("bootstrap.servers"))
                .setTopics(topic)
                .setGroupId(kafkaProperties.getProperty("group.id"))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<String> updateSignalStream = env.fromSource(updateSignalKafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source Update Signal");


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        env.setBufferTimeout(0);

        env.getConfig().enableObjectReuse();


        KafkaSource<String> rawDataKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaProperties.getProperty("bootstrap.servers"))
                .setTopics("InputTopic")
                .setGroupId(kafkaProperties.getProperty("group.id"))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<String> rawDataStream = env.fromSource(rawDataKafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source Raw Data");
        config.setString("udf.limit.rate", "500000");
        env.getConfig().setGlobalJobParameters(config);
        DataStream<String> rateLimitedStream = rawDataStream
                .map(new RateLimiterFunction<>()).disableChaining();

        ConnectedStreams<String, String> connectedStreams = updateSignalStream.connect(rateLimitedStream);
        MapStateDescriptor<String, UpdatableGridIndex> uGridStateDescriptor = new MapStateDescriptor<>(
                "uGridState",
                Types.STRING,
                TypeInformation.of(new TypeHint<UpdatableGridIndex>() {
                }));
        int stateSize = 64;
        UpdatableGridIndex updatableUGrid = new UpdatableGridIndex(100, gridMinX, gridMaxX, gridMinY, gridMaxY);
        GridIndex stateUGrid = new GridIndex(stateSize, gridMinX, gridMaxX, gridMinY, gridMaxY);
        DataStream<UpdatableGridIndex> uGridUpdateStream = connectedStreams.flatMap(new CoFlatMapFunction<String, String, UpdatableGridIndex>() {
            @Override
            public void flatMap1(String value, Collector<UpdatableGridIndex> out) throws Exception {
                int gridSize = Integer.parseInt(value);
                updatableUGrid.updateGridSize(gridSize);
                updatableUGrid.updateSerializedSize();
                System.out.println(updatableUGrid.getGrid().getCellLength());
                out.collect(updatableUGrid);
            }
            @Override
            public void flatMap2(String value, Collector<UpdatableGridIndex> out) throws ParseException {
                long newTimeStamp = inputDateFormat.parse(value.split(",")[1]).getTime() + 10000;
                updatableUGrid.setTimeStamp(newTimeStamp);
            }
        });

        BroadcastStream<UpdatableGridIndex> uGridBroadcastStream = uGridUpdateStream.broadcast(uGridStateDescriptor);

        SingleOutputStreamOperator<Trajectory> preTrajectoryStream = rateLimitedStream
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value.split(",")[0];
                    }
                })
                .connect(uGridBroadcastStream)
                .process(new StringToTraGrid(inputDateFormat, uGridStateDescriptor, windowSize * 1000L,radius,stateUGrid,2,new HashSet<>(Arrays.asList("null")))).setParallelism(parallelism);
        DataStream<Trajectory> trajectoryStream = preTrajectoryStream
                .rebalance()
                .process(new TraReplicate1()).setParallelism(parallelism);
        int minPts = 5;
        ClusterSearch trajectoryCluSearch = new ClusterSearch();
        trajectoryCluSearch.run(trajectoryStream, radius, minPts,bootStrapServers);

        StateProcess stateProcess = new StateProcess(parallelism,gridBBox,env,kafkaProperties,stateTopic,stateSize);
        stateProcess.run(preTrajectoryStream);

        env.execute("TrajFlink");
    }
}
