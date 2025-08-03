
package objects;

import index.GridIndex;
import utils.HelperClass;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.util.Date;

public class Point extends SpatialObject implements Serializable {

    public String gridID;
    public org.locationtech.jts.geom.Point point;
    public long ingestionTime;
    public double cellLength;
    Date date = new Date();

    public Point() {};



    public Point(String objID, double x, double y, long timeStampMillisec, GridIndex uGrid) {
        GeometryFactory geofact = new GeometryFactory();
        point = geofact.createPoint(new Coordinate(x, y));
        this.objID = objID;
        this.timeStampMillisec = timeStampMillisec;
        this.gridID = HelperClass.assignGridCellID(point.getCoordinate(), uGrid);
        this.ingestionTime = date.getTime();
        this.cellLength = uGrid.getCellLength();
    }


    // To print the point coordinates
    @Override
    public String toString() {
        //return "[ObjID: " + this.objID + ", " + point.getX() + ", " + point.getY() + ", " + this.gridID + "]";
        return "[ObjID: " + this.objID + ", " + point.getX() + ", " + point.getY() + ", " + this.gridID + ", " + this.timeStampMillisec  + ", " + this.ingestionTime
                + "]";
        // For DEIM App
        // return "[eventID " + this.eventID + ", deviceID: " + this.deviceID + ", userID " + this.userID + ", " + this.timeStampMillisec + "]";
    }

    public String getObjectId() {
        return this.objID;
    }

    public double getGridSize() {
        return this.cellLength;
    }

    public long getTimeStampMillisec() {
        return this.timeStampMillisec;
    }



}