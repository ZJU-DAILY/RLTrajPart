
package index;

import org.locationtech.jts.geom.*;
import objects.Point;
import utils.DistanceFunctions;
import utils.HelperClass;

import java.util.ArrayList;
import java.util.HashSet;

public class GridIndex implements SpatialIndex {

    double minX;
    double maxX;
    double minY;
    double maxY;

    final private int CELLINDEXSTRLENGTH = 5;
    double cellLength;
    int numGridPartitions;
    HashSet<String> girdCellsSet = new HashSet<String>();



    public GridIndex(double cellLength, double minX, double maxX, double minY, double maxY){

        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;

        this.cellLength = cellLength;
        adjustCoordinatesForSquareGrid();

        double gridLength = DistanceFunctions.getPointPointEuclideanDistance(this.minX, this.minY, this.maxX, this.minY);
        //System.out.println("gridLengthInMeters " + gridLengthInMeters);

        double numGridRows = gridLength/cellLength;
        //System.out.println("numGridRows" + numGridRows);

        if(numGridRows < 1)
            this.numGridPartitions = 1;
        else
            this.numGridPartitions = (int)Math.ceil(numGridRows);

        this.cellLength = (this.maxX - this.minX) / this.numGridPartitions;

        populateGridCells();
    }

    public GridIndex(int newGridPartitions, double minX, double maxX, double minY, double maxY)
    {
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;

        this.numGridPartitions = newGridPartitions;

        this.cellLength = Math.max((this.maxX - this.minX) / newGridPartitions, (this.maxY - this.minY) / newGridPartitions);
        populateGridCells();
    }

    public synchronized void updateGridSize(int newGridPartitions) {
        this.numGridPartitions = newGridPartitions;
        this.cellLength = Math.max((this.maxX - this.minX) / newGridPartitions, (this.maxY - this.minY) / newGridPartitions);
        girdCellsSet.clear();
        populateGridCells();

    }




    private void populateGridCells(){

        // Populating the girdCellset - contains all the cells in the grid
        for (int i = 0; i < this.numGridPartitions; i++) {
            for (int j = 0; j < this.numGridPartitions; j++) {
                String cellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                girdCellsSet.add(cellKey);

                //System.out.println(cellKey);

                // Computing cell boundaries in terms of two extreme coordinates
                //Coordinate minCoordinate = new Coordinate(this.minX + (i * cellLength), this.minY + (j * cellLength), 0);
                //Coordinate maxCoordinate = new Coordinate(this.minX + ((i + 1) * cellLength), this.minY + ((j + 1) * cellLength), 0);

                //Coordinate[] coordinates = {minCoordinate, maxCoordinate};
                //System.out.println(cellKey + ", " + min + ", " + max);
                //cellBoundaries.put(cellKey, coordinates);
            }
        }

        /* Testing the cellBoundaries
        for (Map.Entry<String,Coordinate[]> entry : cellBoundaries.entrySet())
            System.out.println("Key = " + entry.getKey() +
                    ", Value = " + entry.getValue()[0].getX() + ", " + entry.getValue()[0].getY() );
         */
    }
//    public void identifyIntersectingOrContainedGrids(HashSet<Polygon> polygons) {
//        relevantGrids = new HashSet<>();
//        GeometryFactory geometryFactory = new GeometryFactory();
//
//        for (int i = 0; i < numGridPartitions; i++) {
//            for (int j = 0; j < numGridPartitions; j++) {
//                double cellMinX = minX + i * cellLength;
//                double cellMaxX = cellMinX + cellLength;
//                double cellMinY = minY + j * cellLength;
//                double cellMaxY = cellMinY + cellLength;
//
//                Envelope cellEnvelope = new Envelope(cellMinX, cellMaxX, cellMinY, cellMaxY);
//                Geometry cellGeometry = geometryFactory.toGeometry(cellEnvelope);
//
//                String gridID = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) +
//                        HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
//
//                for (Polygon polygon : polygons) {
//                    if (cellGeometry.intersects(polygon) || cellGeometry.within(polygon)) {
//                        relevantGrids.add(gridID);
//                        break; // if a match is found, no need to check other polygons
//                    }
//                }
//            }
//        }
//    }


    private void adjustCoordinatesForSquareGrid(){

        double xAxisDiff = this.maxX - this.minX;
        double yAxisDiff = this.maxY - this.minY;

        // Adjusting coordinates to make square grid cells
        if(xAxisDiff > yAxisDiff)
        {
            double diff = xAxisDiff - yAxisDiff;
            this.maxY += diff / 2;
            this.minY -= diff / 2;
        }
        else if(yAxisDiff > xAxisDiff)
        {
            double diff = yAxisDiff - xAxisDiff;
            this.maxX += diff / 2;
            this.minX -= diff / 2;
        }


    }

    public double getMinX() {return this.minX;}
    public double getMinY() {return this.minY;}
    public double getMaxX() {return this.maxX;}
    public double getMaxY() {return this.maxY;}
    public int getCellIndexStrLength() {return CELLINDEXSTRLENGTH;}

    public int getNumGridPartitions()
    {
        return numGridPartitions;
    }
    public double getCellLength() {return cellLength;}
    public HashSet<String> getGirdCellsSet() {return girdCellsSet;}

    public Coordinate[] getCellMinMaxBoundary(String cellKey){

        ArrayList<Integer> cellIndices = HelperClass.getIntCellIndices(cellKey);

        Coordinate minCoordinate = new Coordinate(this.minX + (cellIndices.get(0) * cellLength), this.minY + (cellIndices.get(1) * this.cellLength), 0);
        Coordinate maxCoordinate = new Coordinate(this.minX + ((cellIndices.get(0) + 1) * cellLength), this.minY + ((cellIndices.get(1) + 1) * this.cellLength), 0);
        Coordinate[] coordinates = {minCoordinate, maxCoordinate};

        return coordinates;
    }


    public boolean validKey(int x, int y){
        if(x >= 0 && y >= 0 && x < numGridPartitions && y < numGridPartitions)
        {return true;}
        else
        {return false;}
    }

    public HashSet<String> getNeighboringCells(double queryRadius, Point queryPoint)
    {
        if(queryRadius == 0){
            return this.girdCellsSet;
        }

        String queryCellID = queryPoint.gridID;
        HashSet<String> neighboringCellsSet = new HashSet<String>();
        neighboringCellsSet.add(queryCellID);
        int numNeighboringLayers = getCandidateNeighboringLayers(queryRadius);

        if(numNeighboringLayers <= 0)
        {
            System.out.println("candidateNeighboringLayers cannot be 0 or less");
            System.exit(1);
        }
        else
        {
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryCellID);

            for(int i = queryCellIndices.get(0) - numNeighboringLayers; i <= queryCellIndices.get(0) + numNeighboringLayers; i++)
                for(int j = queryCellIndices.get(1) - numNeighboringLayers; j <= queryCellIndices.get(1) + numNeighboringLayers; j++)
                {
                    if(validKey(i,j))
                    {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        neighboringCellsSet.add(neighboringCellKey);
                    }
                }

        }
        return neighboringCellsSet;
    }

    public HashSet<String> getNeighboringCells(double queryRadius, String queryCellID)
    {
        // return all the cells in the set
        if(queryRadius == 0){
            return this.girdCellsSet;
        }

        HashSet<String> neighboringCellsSet = new HashSet<String>();
        int numNeighboringLayers = getCandidateNeighboringLayers(queryRadius);

        if(numNeighboringLayers <= 0)
        {
            System.out.println("candidateNeighboringLayers cannot be 0 or less");
            System.exit(1); // Unsuccessful termination
        }
        else //numNeighboringLayers > 0
        {
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryCellID);

            for(int i = queryCellIndices.get(0) - numNeighboringLayers; i <= queryCellIndices.get(0) + numNeighboringLayers; i++)
                for(int j = queryCellIndices.get(1) - numNeighboringLayers; j <= queryCellIndices.get(1) + numNeighboringLayers; j++)
                {
                    if(validKey(i,j))
                    {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        neighboringCellsSet.add(neighboringCellKey);
                    }
                }

        }
        return neighboringCellsSet;
    }



    public int getCandidateNeighboringLayers(double queryRadius)
    {
        int numberOfLayers = (int)Math.ceil(queryRadius/cellLength);
        return numberOfLayers;
    }


}
