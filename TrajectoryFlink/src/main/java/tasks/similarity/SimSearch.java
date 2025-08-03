package tasks.similarity;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import objects.Trajectory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static utils.HelperClass.calculateHausdorffDistance;

public class SimSearch {

    public SimSearch() {
    }


    public void run(DataStream<Trajectory> trajectoryStream, double queryRadius, String queryID, String bootStrapServers) {

        DataStream<Trajectory> timestampedStream = trajectoryStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Trajectory>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        DataStream<Tuple2<Trajectory,Trajectory>> pairedTrajectory = timestampedStream
                .keyBy(trajectory -> trajectory.getGridID())
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(new ProcessWindowFunction<Trajectory, Tuple2<Trajectory, Trajectory>, String, TimeWindow>() { 
                    @Override
                    public void process(String key,  
                                        Context context,
                                        Iterable<Trajectory> elements,
                                        Collector<Tuple2<Trajectory, Trajectory>> out) {

                        List<Trajectory> windowTrajectories = new ArrayList<>();
                        elements.forEach(windowTrajectories::add);

                        Trajectory queryTrajectory = windowTrajectories.stream()
                                .filter(t -> queryID.equals(t.getObjectId()))
                                .findFirst()
                                .orElse(null);
//                        System.out.println(queryTrajectory);

                        if (queryTrajectory != null) {
                            windowTrajectories.stream()
                                    .filter(t -> !queryID.equals(t.getObjectId()))
                                    .forEach(other -> out.collect(Tuple2.of(queryTrajectory, other)));
                        }
                    }
                });


        DataStream<Tuple4<Trajectory, Trajectory, Long, Double>> result = pairedTrajectory
                .keyBy(t -> t.f1.getGridID())
                .process(new KeyedProcessFunction<String, Tuple2<Trajectory, Trajectory>, Tuple4<Trajectory, Trajectory, Long, Double>>() {
                    @Override
                    public void processElement(Tuple2<Trajectory, Trajectory> value,
                                               Context ctx,
                                               Collector<Tuple4<Trajectory, Trajectory, Long, Double>> out) {
                        Trajectory t1 = value.f0;
                        Trajectory t2 = value.f1;

                        double distance = calculateHausdorffDistance(t1, t2);

                        // Check if the distance is within the query radius
                        if (distance <= queryRadius) {
                            out.collect(Tuple4.of(t1, t2, System.currentTimeMillis(), distance));
                        }
                    }
                });


        result.print();
    }




}


