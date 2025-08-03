package index;

import java.io.IOException;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
public class UpdatableGridIndex implements Serializable {
    private GridIndex grid;
    private long timeStamp;
    private long serializedSize;
    private Boolean reSet;
    public UpdatableGridIndex(int size, double minX, double maxX, double minY, double maxY) {
        this.grid = new GridIndex(size, minX, maxX, minY, maxY);
    }

    public synchronized void updateGridSize(int newSize) {
        long begin = System.currentTimeMillis();
        if(newSize!=this.grid.getNumGridPartitions())
            this.grid.updateGridSize(newSize);
        long end = System.currentTimeMillis();
        //System.out.println("begin:"+begin+"end:"+end+"time:"+(end-begin));

    }
//    public synchronized void createRelevantGrids(HashSet<Polygon> polygons){
//        this.grid.identifyIntersectingOrContainedGrids(polygons);
//    }

    public synchronized GridIndex getGrid() {
        return this.grid;
    }
    public double getGridSize() {
        return this.grid.getNumGridPartitions();
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public Boolean getReSet() {
        return reSet;
    }

    public void setReSet(Boolean reSet) {
        this.reSet = reSet;
    }
    public long getSerializedSize() {
        return serializedSize;
    }
    public void setSerializedSize(long serializedSize) {
        this.serializedSize = serializedSize;
    }
    public void updateSerializedSize() throws IOException {
        this.serializedSize = calculateSerializedSize(this);
    }
    public static long calculateSerializedSize(Object obj) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(obj);
        objectOutputStream.flush();
        objectOutputStream.close();
        return byteArrayOutputStream.size();
    }


    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}

