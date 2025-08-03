package funs;


import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import objects.Trajectory;

import java.util.HashSet;

public class TraReplicate extends ProcessFunction<Trajectory,  Trajectory> {
    private final String queryID;

    public TraReplicate(String queryID) {
        this.queryID = queryID;
    }
    @Override
    public void processElement(Trajectory trajectory, ProcessFunction<Trajectory, Trajectory>.Context ctx, Collector<Trajectory> out) throws Exception {
        HashSet<String> neighboringGrids = trajectory.getNeighboringGrids();
        if (!queryID.equals(trajectory.getObjectId())) {
            for (String gridID : trajectory.getGridIDsSet()){
                trajectory.setGridId(gridID);
                out.collect(trajectory);
            }
        } else {
            for (String neighboringGridID : neighboringGrids) {
                trajectory.setGridID(neighboringGridID);
                out.collect(trajectory);
            }
        }
    }
}
