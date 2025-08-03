package objects;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class ClusterInfo implements Serializable {
    private int clusterId;
    private Set<String> trajectoryIds;

    public ClusterInfo(int clusterId) {
        this.clusterId = clusterId;
        this.trajectoryIds = new HashSet<>();
    }

    public int getClusterId() {
        return clusterId;
    }

    public void addTrajectory(Trajectory trajectory) {
        trajectoryIds.add(trajectory.getObjectId());
    }

    public void mergeCluster(ClusterInfo otherCluster) {
        trajectoryIds.addAll(otherCluster.trajectoryIds);
    }

    public Set<String> getTrajectoryIds() {
        return trajectoryIds;
    }
}
