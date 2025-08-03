package funs;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import objects.Edge;
import objects.Trajectory;

import java.util.ArrayList;
import java.util.List;

import static utils.HelperClass.calculateHausdorffDistance;

public class TrajectoryEdgeWindowFunction extends RichWindowFunction<Trajectory, Edge, String, TimeWindow> {

    private final double epsilon;

    public TrajectoryEdgeWindowFunction(double epsilon) {
        this.epsilon = epsilon;
    }

    @Override
    public void apply(String gridId, TimeWindow window, Iterable<Trajectory> input, Collector<Edge> out) throws Exception {
        List<Trajectory> trajectories = new ArrayList<>();
        input.forEach(trajectories::add);
        for (int i = 0; i < trajectories.size(); i++) {
            Trajectory t1 = trajectories.get(i);
            for (int j = i + 1; j < trajectories.size(); j++) {
                Trajectory t2 = trajectories.get(j);
                double distance = calculateHausdorffDistance(t1, t2);

                if (distance <= epsilon) {
                    out.collect(new Edge(t1.getObjectId(), t2.getObjectId(), distance, t1.getTimestamp()));
                }
            }
        }
    }

}

