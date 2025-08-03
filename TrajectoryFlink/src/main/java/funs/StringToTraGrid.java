package funs;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import spatialIndices.UniformGrid;
import spatialIndices.UpdatableUniformGrid;
import spatialObjects.Point;
import spatialObjects.Trajectory;
import utils.HelperClass;

import java.text.DateFormat;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class StringToTraGrid extends KeyedBroadcastProcessFunction<String, String, UpdatableUniformGrid, Trajectory> {
    private final DateFormat dateFormat;
    private final MapStateDescriptor<String, UpdatableUniformGrid> uGridStateDescriptor;
    private final long timeWindow;
    private final UniformGrid stateUGrid;

    private final GeometryFactory geofact = new GeometryFactory();
    private final double queryRadius;
    private final int queryType;
    private ValueState<Trajectory> trajectoryState;
    private final Set<String> queryID;


    public StringToTraGrid(DateFormat dateFormat, MapStateDescriptor<String, UpdatableUniformGrid> uGridStateDescriptor, long timeWindow, double queryRadius, UniformGrid stateUGrid, int queryType, Set<String> queryID) {
        this.dateFormat = dateFormat;
        this.uGridStateDescriptor = uGridStateDescriptor;
        this.timeWindow = timeWindow;
        this.stateUGrid = stateUGrid;
        this.queryType = queryType;
        this.queryRadius = queryRadius;
        this.queryID = queryID;

    }
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Trajectory> trajectoryStateDescriptor = new ValueStateDescriptor<>(
                "trajectoryState",
                TypeInformation.of(Trajectory.class)
        );
        trajectoryState = getRuntimeContext().getState(trajectoryStateDescriptor);
    }

    @Override
    public void processElement(String value, KeyedBroadcastProcessFunction<String, String, UpdatableUniformGrid, Trajectory>.ReadOnlyContext ctx, Collector<Trajectory> out) throws Exception {

        UpdatableUniformGrid updatableUGrid = ctx.getBroadcastState(uGridStateDescriptor).get("currentUGrid");
        if (updatableUGrid == null) {
            return;
        }
        long maxTime = updatableUGrid.getTimeStamp();
        String[] fields = value.split(",");
        String oID = fields[0];
        String timestampStr = fields[1];
        double x = Double.parseDouble(fields[2]);
        double y = Double.parseDouble(fields[3]);
        long time = dateFormat.parse(timestampStr).getTime();
        UniformGrid currentUGrid = updatableUGrid.getGrid();
        org.locationtech.jts.geom.Point cPoint = geofact.createPoint(new Coordinate(x, y));
        int stateIndex = HelperClass.assignStateGridCellIndex(cPoint.getCoordinate(), this.stateUGrid);
        if(time < maxTime)
        {
            UpdatableUniformGrid oldUGrid = ctx.getBroadcastState(uGridStateDescriptor).get("oldUGrid");
            if(oldUGrid == null)
                return;
            currentUGrid = oldUGrid.getGrid();
        }
        Point spatialPoint = new Point(oID, x, y, time, currentUGrid);
        double cellLength = currentUGrid.getCellLength();
        int gridSize = currentUGrid.getNumGridPartitions();
        // Manage the trajectory
        Trajectory trajectory = trajectoryState.value();
        if (trajectory == null) {
            trajectory = new Trajectory(spatialPoint.getObjectId(), timeWindow);
        }
        trajectory.addPoint(spatialPoint);
        LinkedList<Point> points = trajectory.getPoints();
        HashSet<String> neighboringGrids = new HashSet<String>();
        for (Point point : points) {
            String gridID = point.gridID;
            if (point.cellLength != cellLength){
                gridID = HelperClass.assignGridCellID(point.point.getCoordinate(), currentUGrid);
                point.gridID = gridID;
                point.cellLength = cellLength;
                trajectory.addGridIDsSet(gridID);
            }


        }
        trajectory.setStateIndex(stateIndex);
        trajectory.setGridSize(gridSize);
        trajectory.setGridMemorySize(updatableUGrid.getSerializedSize());

        if(queryType == 1) {//sim
            if (queryID.contains(spatialPoint.getObjectId())) {
                neighboringGrids = currentUGrid.getNeighboringCells(queryRadius, spatialPoint);
                trajectory.setNeighboringGrids(neighboringGrids);
            }
        }
        else if(queryType == 2){//clu
            neighboringGrids = currentUGrid.getNeighboringCells(queryRadius, spatialPoint);
            trajectory.setNeighboringGrids(neighboringGrids);
        }
        trajectoryState.update(trajectory);
        out.collect(trajectory);


    }
    @Override
    public void processBroadcastElement(UpdatableUniformGrid newUGrid, Context ctx, Collector<Trajectory> out) throws Exception {
        BroadcastState<String, UpdatableUniformGrid> state = ctx.getBroadcastState(uGridStateDescriptor);
        UpdatableUniformGrid currentUGrid = state.get("currentUGrid");
        if (currentUGrid == null || currentUGrid.getGridSize() != newUGrid.getGridSize()) {
            if (currentUGrid != null) {
                state.put("oldUGrid", currentUGrid);
            }
            state.put("currentUGrid", newUGrid);

        }
    }


}