package funs;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.util.HashSet;

public class PolygonGenerator {

    public static HashSet<Polygon> generatePolygons(double minX, double maxX, double minY, double maxY) {
        GeometryFactory geometryFactory = new GeometryFactory();
        HashSet<Polygon> polygonSet = new HashSet<>();

        int gridRows = 100;
        int gridCols = 100;
        int sampleRows = 10;
        int sampleCols = 10;

        double xStep = (maxX - minX) / gridCols;
        double yStep = (maxY - minY) / gridRows;

        int rowStep = gridRows / sampleRows;
        int colStep = gridCols / sampleCols;

        for (int i = 0; i < gridRows; i += rowStep) {
            for (int j = 0; j < gridCols; j += colStep) {
                double cellMinY = minY + i * yStep;
                double cellMinX = minX + j * xStep;
                double cellMaxY = cellMinY + yStep;
                double cellMaxX = cellMinX + xStep;
                Polygon polygon = geometryFactory.createPolygon(new Coordinate[]{
                        new Coordinate(cellMinX, cellMinY),
                        new Coordinate(cellMinX, cellMaxY),
                        new Coordinate(cellMaxX, cellMaxY),
                        new Coordinate(cellMaxX, cellMinY),
                        new Coordinate(cellMinX, cellMinY)
                });
                polygonSet.add(polygon);
            }
        }

        return polygonSet;
    }
}
