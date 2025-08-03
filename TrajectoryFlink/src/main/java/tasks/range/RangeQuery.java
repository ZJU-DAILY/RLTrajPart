package tasks.range;
import org.locationtech.jts.geom.*;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.LineString;
import objects.Trajectory;

import java.util.Set;

public class RangeQuery {

    public RangeQuery() {

    }

    public void run(DataStream<Trajectory> trajectoryDataStream, Set<Polygon> queryPolygons, String bootStrapServers) {
        DataStream<Tuple4<Trajectory, LineString, Polygon, Long>> resultTrajectories = trajectoryDataStream
                .keyBy(t -> t.getGridID())
                .process(new ProcessFunction<Trajectory, Tuple4<Trajectory, LineString, Polygon, Long>>() {
                    @Override
                    public void processElement(Trajectory value, ProcessFunction<Trajectory, Tuple4<Trajectory, LineString, Polygon, Long>>.Context ctx, Collector<Tuple4<Trajectory, LineString, Polygon, Long>> out) throws Exception {
                        for (Polygon polygon : queryPolygons) {
                            LineString lineString =  value.getLinestring();
                            if (lineString.intersects(polygon) || lineString.within(polygon)) {
                                out.collect(Tuple4.of(value, lineString, polygon, System.currentTimeMillis()));
                                break;
                            }
                        }
                    }
                }).disableChaining();
        resultTrajectories.print();


    }

}
