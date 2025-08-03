package objects;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.io.Serializable;
import java.util.*;

public class Trajectory extends SpatialObject implements Serializable {
    private LinkedList<Point> points = new LinkedList<>();
    private HashSet<String> gridIDsSet = new HashSet<>();
    private String gridID;
    private String objectId;
    private int stateIndex;
    private long timestamp;
    private long processTime;
    private long windowSize;
    private int gridSize;
    HashSet<String> neighboringGrids;
    private long gridMemorySize;
    private LineString lineString;
    private GeometryFactory geometryFactory = new GeometryFactory();
    public Trajectory() {
    }
    public Trajectory(long windowSize) {
        this.windowSize = windowSize;
    }
    public Trajectory(String objectId, long windowSize) {
        this.objectId = objectId;
        this.windowSize = windowSize;
    }

    public void setGridMemorySize(long gridMemorySize) {
        this.gridMemorySize = gridMemorySize;
    }

    public long getGridMemorySize() {
        return this.gridMemorySize;
    }
    public void setStateIndex(int index) {
        this.stateIndex = index;
    }

    public int getStateIndex() {
        return this.stateIndex;
    }
    public void addPoint(Point p) {
        if (points.isEmpty()) {
            points.addLast(p);
        } else {
            ListIterator<Point> pointListIterator = points.listIterator(points.size());
            int idxToInsert = points.size();
            while (pointListIterator.hasPrevious()) {
                Point curP = pointListIterator.previous();
                if (p.getTimeStampMillisec() >= curP.getTimeStampMillisec()) {
                    break;
                }
                idxToInsert--;
            }
            points.add(idxToInsert, p);
            while (points.size() > 1 && points.getLast().getTimeStampMillisec() - points.getFirst().getTimeStampMillisec() >= windowSize) {
                points.removeFirst();
            }
        }
        this.timestamp = p.getTimeStampMillisec();
//        this.processTime = System.nanoTime();
        this.processTime = System.currentTimeMillis();
        updateGridIDs();
        updateLineString();
    }

    private void updateGridIDs() {
        gridIDsSet.clear();
//        Map<String, Integer> gridIDCountMap = new HashMap<>();
        for (Point point : points) {
            String gridID = point.gridID;
            gridIDsSet.add(gridID);
//            gridIDCountMap.put(gridID, gridIDCountMap.getOrDefault(gridID, 0) + 1);
        }
//        this.gridID = Collections.max(gridIDCountMap.entrySet(), Map.Entry.comparingByValue()).getKey();
        this.gridID = points.get(points.size()-1).gridID;
    }
    private void updateLineString() {
        Coordinate[] coordinates;
        if (points.size() == 1) {
            coordinates = new Coordinate[]{ points.getFirst().point.getCoordinate(), points.getFirst().point.getCoordinate() };
        } else {
            coordinates = new Coordinate[points.size()];
            for (int i = 0; i < points.size(); i++) {
                coordinates[i] = points.get(i).point.getCoordinate();
            }
        }
        this.lineString = geometryFactory.createLineString(coordinates);
    }



    public void setGridID(String gridID) {
        this.gridID = gridID;
    }
    public String getGridID() {
        return this.gridID;
    }

    public void setTimestamp(long time){
        this.timestamp = time;
    }
    public long getTimestamp(){
        return this.timestamp;
    }

    public void setProcessTime(long time){
        this.processTime = time;
    }
    public long getProcessTime(){
        return this.processTime;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }
    public String getObjectId() {
        return this.objectId;
    }

    public HashSet<String> getGridIDsSet() {
        return gridIDsSet;
    }

    public void setGridId(String gridID){
        this.gridID = gridID;
    }
    public void setGridIDsSet(HashSet<String> gridIDsSet) {
        this.gridIDsSet = gridIDsSet;
    }
    public int getGridSize() {
        return gridSize;
    }
    public void setGridSize(int gridSize) {
        this.gridSize = gridSize;
    }

    public LinkedList<Point> getPoints() {
        return points;
    }
    public void setPoints(LinkedList<Point> points) {
        this.points = points;
    }

    public LineString getLineString() {
        return lineString;
    }
    public void setNeighboringGrids(HashSet<String> neighboringGrids){
        this.neighboringGrids=neighboringGrids;
    }
    public HashSet<String> getNeighboringGrids(){
        return neighboringGrids;
    }

    public void addGridIDsSet(String gridID) {
        this.gridIDsSet.add(gridID);
    }
    public String toString() {
        //return "[ObjID: " + this.objectId + ", " + this.gridID + ", " + this.points  + ", " + this.timestamp +"]";
        //return "[ObjID: " + this.objectId + ", " + this.gridID + ", " + + this.ingestionTime + "]";
//        return "[ObjID: " + this.objectId  + ", " + this.gridID + ", " + this.points  + ", " + this.points.size()  +", " +this.timestamp+", "+ this.processTime + ", "+ this.t1 +"]";
//        return "[ObjID: " + this.objectId  + ", " + this.gridID + ", " + this.points.size()  +", " +this.timestamp+", "+ this.processTime +"]";
//        return "[ObjID: " + this.objectId  + ", " + this.gridID + ", " + this.points.size()  +", " +this.timestamp+", "+ this.processTime + ", "+ this.t1 +"]";
//        return "[ObjID: " + this.objectId  + ", " + this.gridID + ", " + this.points.size()  +", " +this.timestamp+", "+ this.processTime + ", "+ this.reTime + ", "+ this.t1 +"]";

//        return "[ObjID: " + this.objectId  + ", " + this.gridID + ", " + this.points.size()  +", " +this.timestamp+", "+ this.processTime + ", "+ this.reTime + ", "+ this.t1 + ", "+ this.t2+"]";
//        return "[ObjID: " + this.objectId  + ", " + this.gridID + ", " + this.points  + ", " + this.points.size()  +", " +this.timestamp +"]";
        return "[ObjID: " + this.objectId  + ", "+ this.gridID + ", " + this.timestamp+"]";
//        return "[ObjID: " + this.objectId  +", " + this.gridID + ", " + this.points+"]";
    }


    public LineString getLinestring() {
        return this.lineString;
    }


}


