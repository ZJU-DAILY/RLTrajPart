package objects;

import java.io.Serializable;

public class Edge implements Serializable {
    private String trajectoryAId;
    private String trajectoryBId;
    private double distance;
    private long timestamp;


    public Edge() {
        this.trajectoryAId = "";
        this.trajectoryBId = "";
        this.distance = 0.0;
        this.timestamp = 0L;
    }


    public Edge(String trajectoryAId, String trajectoryBId, double distance, long timestamp) {
        this.trajectoryAId = trajectoryAId;
        this.trajectoryBId = trajectoryBId;
        this.distance = distance;
        this.timestamp = timestamp;
    }

    public String getTrajectoryAId() {
        return trajectoryAId;
    }

    public void setTrajectoryAId(String trajectoryAId) {
        this.trajectoryAId = trajectoryAId;
    }

    public String getTrajectoryBId() {
        return trajectoryBId;
    }

    public void setTrajectoryBId(String trajectoryBId) {
        this.trajectoryBId = trajectoryBId;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


    @Override
    public String toString() {
        return "Edge{" +
                "trajectoryAId='" + trajectoryAId + '\'' +
                ", trajectoryBId='" + trajectoryBId + '\'' +
                ", distance=" + distance +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        Edge edge = (Edge) obj;

        if (Double.compare(edge.distance, distance) != 0) return false;
        if (timestamp != edge.timestamp) return false;
        if (!trajectoryAId.equals(edge.trajectoryAId)) return false;
        return trajectoryBId.equals(edge.trajectoryBId);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = trajectoryAId.hashCode();
        result = 31 * result + trajectoryBId.hashCode();
        temp = Double.doubleToLongBits(distance);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }
}
