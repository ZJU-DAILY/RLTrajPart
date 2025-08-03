package tasks.cluster;

import funs.DBSCANWindowFunction;
import funs.TrajectoryEdgeWindowFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import objects.Cluster;
import objects.Edge;
import objects.Trajectory;


import java.time.Duration;

public class ClusterSearch {

    public ClusterSearch() {
    }
    public void run(DataStream<Trajectory> trajectoryDataStream, Double epsilon, int minPts,  String bootStrapServers) {

        DataStream<Trajectory> timestampedStream = trajectoryDataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Trajectory>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Trajectory>() {
                                    @Override
                                    public long extractTimestamp(Trajectory element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })
                );

        DataStream<Edge> edges = timestampedStream
                .keyBy(Trajectory::getGridID)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new TrajectoryEdgeWindowFunction(epsilon));

        DataStream<Cluster> clusters = edges
                .keyBy(Edge::getTimestamp)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new DBSCANWindowFunction(minPts));

        clusters.print();

    }
}