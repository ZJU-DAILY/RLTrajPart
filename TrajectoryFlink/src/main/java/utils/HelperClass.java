package utils;

import index.GridIndex;
import objects.Point;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.locationtech.jts.geom.Coordinate;
import objects.Trajectory;


import java.io.IOException;

import java.util.*;


public class HelperClass {


    public static String padLeadingZeroesToInt(int cellIndex, int desiredStringLength)
    {
        return String.format("%0"+ Integer.toString(desiredStringLength) +"d", cellIndex);
    }



    public static int removeLeadingZeroesFromString(String str)
    {
        return Integer.parseInt(str.replaceFirst("^0+(?!$)", ""));
    }


    public static String assignGridCellID(Coordinate coordinate, GridIndex uGrid) {

        int xCellIndex = (int)(Math.floor((coordinate.getX() - uGrid.getMinX())/uGrid.getCellLength()));
        int yCellIndex = (int)(Math.floor((coordinate.getY() - uGrid.getMinY())/uGrid.getCellLength()));

        String gridIDStr = HelperClass.generateCellIDStr(xCellIndex, yCellIndex, uGrid);

        return gridIDStr;
    }
    public static int assignStateGridCellIndex(Coordinate coordinate, GridIndex uGrid) {
        int xCellIndex = (int) (Math.floor((coordinate.getX() - uGrid.getMinX()) / uGrid.getCellLength()));
        int yCellIndex = (int) (Math.floor((coordinate.getY() - uGrid.getMinY()) / uGrid.getCellLength()));
        int numGridPartitions = uGrid.getNumGridPartitions();
        int gridCellIndex = xCellIndex * numGridPartitions + yCellIndex;


        return gridCellIndex;
    }



    public static String generateCellIDStr(int xCellIndex, int yCellIndex, GridIndex uGrid){
        return HelperClass.padLeadingZeroesToInt(xCellIndex, uGrid.getCellIndexStrLength()) + HelperClass.padLeadingZeroesToInt(yCellIndex, uGrid.getCellIndexStrLength());
    }






    public static ArrayList<Integer> getIntCellIndices(String cellID)
    {
        ArrayList<Integer> cellIndices = new ArrayList<Integer>();

        //System.out.println("cellIndices.size() " + cellIndices.size());
        String cellIDX = cellID.substring(0,5);
        String cellIDY = cellID.substring(5);

        cellIndices.add(HelperClass.removeLeadingZeroesFromString(cellIDX));
        cellIndices.add(HelperClass.removeLeadingZeroesFromString(cellIDY));

        return cellIndices;
    }

    public static Integer getCellLayerWRTQueryCell(String queryCellID, String cellID)
    {
        ArrayList<Integer> queryCellIndices = getIntCellIndices(queryCellID);
        ArrayList<Integer> cellIndices = getIntCellIndices(cellID);
        Integer cellLayer;

        if((queryCellIndices.get(0).equals(cellIndices.get(0))) && (queryCellIndices.get(1).equals(cellIndices.get(1)))) {
            return 0; // cell layer is 0
        }
        else if ( Math.abs(queryCellIndices.get(0) - cellIndices.get(0)) == 0){
            return Math.abs(queryCellIndices.get(1) - cellIndices.get(1));
        }
        else if ( Math.abs(queryCellIndices.get(1) - cellIndices.get(1)) == 0){
            return Math.abs(queryCellIndices.get(0) - cellIndices.get(0));
        }
        else{
            return Math.max(Math.abs(queryCellIndices.get(0) - cellIndices.get(0)), Math.abs(queryCellIndices.get(1) - cellIndices.get(1)));
        }
    }



    public static class checkExitControlTuple implements FilterFunction<ObjectNode> {
        @Override
        public boolean filter(ObjectNode json) throws Exception {
            String objType = json.get("value").get("geometry").get("type").asText();
            if (objType.equals("control")) {
                try {
                    throw new IOException();
                } finally {}

            }
            else return true;
        }
    }



    public static double calculateClosestPairDistance(Trajectory traj1, Trajectory traj2) {
        long startTime = System.nanoTime();
        double minDistance = Double.MAX_VALUE;
        for (Point point1 : traj1.getPoints()) {
            for (Point point2 : traj2.getPoints()) {
                double distance = DistanceFunctions.getDistance(point1, point2);
                if (distance < minDistance) {
                    minDistance = distance;
                }
            }
        }
        long endTime = System.nanoTime();

        long timeElapsed = endTime - startTime;
//        System.out.println("time:"+timeElapsed/ 1000+" traj1.size:"+traj1.getPoints().size()+" traj2.size:"+traj2.getPoints().size());
        return minDistance;
    }

    public static double calculateHausdorffDistance(Trajectory traj1, Trajectory traj2) {
        long startTime = System.nanoTime();
        double maxMinDistanceAB = 0; // Maximum of minimum distances from points in traj1 to traj2
        double maxMinDistanceBA = 0; // Maximum of minimum distances from points in traj2 to traj1

        // Calculate the maximum of minimum distances from points in traj1 to traj2
        for (Point point1 : traj1.getPoints()) {
            double minDistance = Double.MAX_VALUE;
            for (Point point2 : traj2.getPoints()) {
                double distance = DistanceFunctions.getDistance(point1, point2);
                if (distance < minDistance) {
                    minDistance = distance;
                }
            }
            if (minDistance > maxMinDistanceAB) {
                maxMinDistanceAB = minDistance;
            }
        }

        // Calculate the maximum of minimum distances from points in traj2 to traj1
        for (Point point2 : traj2.getPoints()) {
            double minDistance = Double.MAX_VALUE;
            for (Point point1 : traj1.getPoints()) {
                double distance = DistanceFunctions.getDistance(point2, point1);
                if (distance < minDistance) {
                    minDistance = distance;
                }
            }
            if (minDistance > maxMinDistanceBA) {
                maxMinDistanceBA = minDistance;
            }
        }

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        // System.out.println("Hausdorff Distance calculation time: " + timeElapsed / 1000 + " microseconds");

        // The Hausdorff distance is the maximum of maxMinDistanceAB and maxMinDistanceBA
        return Math.max(maxMinDistanceAB, maxMinDistanceBA);
    }












}