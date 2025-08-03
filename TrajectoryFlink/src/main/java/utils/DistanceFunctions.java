package utils;


import objects.Point;

public class DistanceFunctions {

    public DistanceFunctions() {}

    // Point-to-Point Distance
    public static double getDistance(Point obj1, Point obj2)
    {
        return obj1.point.distance(obj2.point);
    }



    public static double getPointPointEuclideanDistance(Double lon, Double lat, Double lon1, Double lat1) {

        return Math.sqrt( Math.pow((lat1 - lat),2) + Math.pow((lon1 - lon),2));

    }




}
