package objects;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Cluster implements Serializable {
    private int clusterId;
    private Set<String> coreTrajectories;
    private Set<String> borderTrajectories;
    private long timestamp;

    public Cluster() {
        this.clusterId = -1;
        this.coreTrajectories = new HashSet<>();
        this.borderTrajectories = new HashSet<>();
        this.timestamp = System.currentTimeMillis();
    }


    public Cluster(int clusterId) {
        this.clusterId = clusterId;
        this.coreTrajectories = new HashSet<>();
        this.borderTrajectories = new HashSet<>();
        this.timestamp = System.currentTimeMillis();
    }

    public Cluster(int clusterId, long timestamp) {
        this.clusterId = clusterId;
        this.coreTrajectories = new HashSet<>();
        this.borderTrajectories = new HashSet<>();
        this.timestamp = timestamp;
    }

    public void addCoreTrajectory(String trajectoryId) {
        coreTrajectories.add(trajectoryId);
    }

    public void addBorderTrajectory(String trajectoryId) {
        borderTrajectories.add(trajectoryId);
    }

    public void mergeCluster(Cluster other) {
        this.coreTrajectories.addAll(other.coreTrajectories);
        this.borderTrajectories.addAll(other.borderTrajectories);
        this.timestamp = Math.max(this.timestamp, other.timestamp);
    }


    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public Set<String> getCoreTrajectories() {
        return coreTrajectories;
    }

    public Set<String> getBorderTrajectories() {
        return borderTrajectories;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Cluster{" +
                "clusterId=" + clusterId +
                ", coreTrajectories=" + coreTrajectories +
                ", borderTrajectories=" + borderTrajectories +
                ", timestamp=" + timestamp +
                '}';
    }
}



//public class Cluster implements Serializable {
//    private int clusterId;
//    private List<Trajectory> trajectories;
//    private boolean isCoreCluster;
//    private Long costTime;
//    public Cluster() {
//        this.trajectories = new ArrayList<>();
//    }
//
//    public void addTrajectory(Trajectory trajectory, boolean isCore) {
//        this.trajectories.add(trajectory);
//        this.isCoreCluster = isCore;
//    }
//
//    public void setClusterId(int clusterId) {
//        this.clusterId = clusterId;
//    }
//
//    public int getClusterId() {
//        return clusterId;
//    }
//
//    public List<Trajectory> getTrajectories() {
//        return trajectories;
//    }
//
//    public boolean isCoreCluster() {
//        return isCoreCluster;
//    }
//
//    public void setCostTime(Long time){
//        this.costTime = time;
//    }
//    public Long getCostTime() {
//        return this.costTime;
//    }
//
//
//    public void updateCostTime() {
//        if (trajectories.isEmpty()) {
//            this.costTime = 0L;
//        } else {
//            Long minTime = trajectories.get(trajectories.size()-1).getProcessTime();
////            Long currentTime = System.nanoTime();
//            Long currentTime = System.currentTimeMillis();
//            this.costTime = currentTime - minTime;
//        }
//    }
//}


//public class Cluster {
//    private List<Trajectory> trajectories;
//    private Set<String> objIDs;
//    private Long costTime;
//    private Map<Trajectory, Boolean> isCoreTrajectory = new HashMap<>();
//    public Cluster() {
//        this.trajectories = new ArrayList<>();
//        this.objIDs = new HashSet<>(); 
//    }
//
//    public void addTrajectory(Trajectory trajectory) {
//        this.trajectories.add(trajectory);
//    }
//    public void addTrajectory(Trajectory trajectory, boolean isCore) {
//        if (!objIDs.contains(trajectory.getObjectId())) {
//            this.trajectories.add(trajectory);
//            this.objIDs.add(trajectory.getObjectId());
//            this.isCoreTrajectory.put(trajectory, isCore);
//        }
//    }
//
//    public List<Trajectory> getTrajectories() {
//        return trajectories;
//    }
//    public void setCostTime(Long time){
//        this.costTime = time;
//    }
//    public Long getCostTime() {
//        return this.costTime;
//    }
//
//
//    public void updateCostTime() {
//        if (trajectories.isEmpty()) {
//            this.costTime = 0L;
//        } else {
//            Long minTime = trajectories.get(trajectories.size()-1).getProcessTime();
////            Long currentTime = System.nanoTime();
//            Long currentTime = System.currentTimeMillis();
//            this.costTime = currentTime - minTime;
//        }
//    }
//
////    @Override
////    public String toString() {
////        return trajectories.toString();
////    }
//
//    @Override
//    public String toString() {
//        StringBuilder builder = new StringBuilder();
//        builder.append("Cluster Size: ")
//                .append(trajectories.size())
//                .append("\n");
//        for (Trajectory trajectory : trajectories) {
//            builder.append("ID: ")
//                    .append(trajectory.getObjectId())
//                    .append(", GridID: ")
//                    .append(trajectory.getGridID())
//                    .append(", Time: ")
//                    .append(trajectory.getTimestamp())
//                    .append(", Core: ")
//                    .append(this.isCoreTrajectory.get(trajectory))
//                    .append("\n");
////            builder.append(trajectory.toString());
//        }
//        return builder.toString();
//    }
//
//    public boolean isCoreTrajectory(Trajectory trajectory) {
//        return isCoreTrajectory.get(trajectory);
//    }
//
//    public void mergeWith(Cluster relatedCluster) {
//        for (Trajectory trajectory : relatedCluster.getTrajectories()) {
//            this.addTrajectory(trajectory);
//        }
//    }
//}
