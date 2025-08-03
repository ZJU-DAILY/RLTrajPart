package funs;


import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import objects.Trajectory;

import java.util.HashSet;

public class TraReplicate1 extends ProcessFunction<Trajectory, Trajectory> {

    @Override
    public void processElement(Trajectory trajectory, Context ctx, Collector<Trajectory> out) throws Exception {
        HashSet<String> allGridIDs = new HashSet<>(trajectory.getGridIDsSet());
        allGridIDs.addAll(trajectory.getNeighboringGrids());

        for (String gridID : allGridIDs) {
            trajectory.setGridID(gridID);
            out.collect(trajectory);
        }
    }
}

