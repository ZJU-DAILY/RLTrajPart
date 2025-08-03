package funs;

import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import objects.Cluster;
import objects.Edge;

import java.util.*;

public class DBSCANWindowFunction extends RichWindowFunction<Edge, Cluster, Long, TimeWindow> {

    private final int minPts;

    public DBSCANWindowFunction(int minPts) {
        this.minPts = minPts;
    }

    @Override
    public void apply(Long key, TimeWindow window, Iterable<Edge> edges, Collector<Cluster> out) throws Exception {
        Set<Edge> uniqueEdges = deduplicateEdges(edges);
//        System.out.println("uniqueEdges for window: " +uniqueEdges);
        long timestamp = uniqueEdges.iterator().hasNext() ? uniqueEdges.iterator().next().getTimestamp() : 0L;
//        System.out.println("Timestamp for window: " + timestamp);
        Map<String, Set<String>> adjacencyList = buildAdjacencyList(uniqueEdges);
//        System.out.println("Adjacency List at timestamp " + timestamp + ": " + adjacencyList);

        Map<String, Boolean> isCorePoint = identifyCorePoints(adjacencyList);
//        System.out.println("Core Points at timestamp " + timestamp + ": " + isCorePoint);

        Map<String, String> nearestCorePointMap = calculateNearestCore(isCorePoint, uniqueEdges);
//        System.out.println("Nearest Core Point Map at timestamp " + timestamp + ": " + nearestCorePointMap);

        List<Cluster> clusters = performClustering(adjacencyList, isCorePoint, nearestCorePointMap, timestamp);
//        System.out.println("Clusters at timestamp " + timestamp + ": " + clusters);

        clusters.forEach(out::collect);
    }


    private Set<Edge> deduplicateEdges(Iterable<Edge> edges) {
        Set<String> seenEdges = new HashSet<>();
        Set<Edge> uniqueEdges = new HashSet<>();

        for (Edge edge : edges) {
            String a = edge.getTrajectoryAId();
            String b = edge.getTrajectoryBId();
            double distance = edge.getDistance();

            String edgeKey = a.compareTo(b) <= 0 ? a + "," + b + "," + distance : b + "," + a + "," + distance;

            if (seenEdges.add(edgeKey)) {
                uniqueEdges.add(edge);
            }
        }
        return uniqueEdges;
    }

    private Map<String, Set<String>> buildAdjacencyList(Iterable<Edge> edges) {
        Map<String, Set<String>> adjacencyList = new HashMap<>();
        for (Edge edge : edges) {
            adjacencyList.computeIfAbsent(edge.getTrajectoryAId(), k -> new HashSet<>()).add(edge.getTrajectoryBId());
            adjacencyList.computeIfAbsent(edge.getTrajectoryBId(), k -> new HashSet<>()).add(edge.getTrajectoryAId());
        }
        return adjacencyList;
    }

    private Map<String, Boolean> identifyCorePoints(Map<String, Set<String>> adjacencyList) {
        Map<String, Boolean> isCorePoint = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : adjacencyList.entrySet()) {
            String point = entry.getKey();
            int degree = entry.getValue().size();
            isCorePoint.put(point, degree >= minPts - 1);
        }
        return isCorePoint;
    }

    private Map<String, String> calculateNearestCore(Map<String, Boolean> isCorePoint, Iterable<Edge> edges) {
        Map<String, String> nearestCorePointMap = new HashMap<>();
        Map<String, Double> nearestCoreDistanceMap = new HashMap<>();

        for (Edge edge : edges) {
            String pointA = edge.getTrajectoryAId();
            String pointB = edge.getTrajectoryBId();
            double distance = edge.getDistance();

            if (isCorePoint.getOrDefault(pointA, false) && isCorePoint.getOrDefault(pointB, false)) {
                continue;
            }

            if (isCorePoint.getOrDefault(pointA, false)) {
                updateNearestCore(nearestCorePointMap, nearestCoreDistanceMap, pointB, pointA, distance);
            }

            if (isCorePoint.getOrDefault(pointB, false)) {
                updateNearestCore(nearestCorePointMap, nearestCoreDistanceMap, pointA, pointB, distance);
            }
        }

        return nearestCorePointMap;
    }

    private void updateNearestCore(Map<String, String> nearestCorePointMap, Map<String, Double> nearestCoreDistanceMap,
                                   String boundaryPoint, String corePoint, double distance) {
        if (!nearestCoreDistanceMap.containsKey(boundaryPoint) || distance < nearestCoreDistanceMap.get(boundaryPoint)) {
            if (!boundaryPoint.equals(corePoint)) {
                nearestCorePointMap.put(boundaryPoint, corePoint);
                nearestCoreDistanceMap.put(boundaryPoint, distance);
            }
        }
    }


    private List<Cluster> performClustering(Map<String, Set<String>> adjacencyList, Map<String, Boolean> isCorePoint,
                                            Map<String, String> nearestCorePointMap, long timestamp) {
        List<Cluster> clusters = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Map<String, Integer> pointToCluster = new HashMap<>();

        int clusterId = 0;

        for (String point : adjacencyList.keySet()) {
            if (visited.contains(point)) {
                continue;
            }

            if (isCorePoint.getOrDefault(point, false)) {
                clusterId++;
                Cluster cluster = new Cluster(clusterId, timestamp);
                bfsCluster(adjacencyList, isCorePoint, visited, point, cluster, pointToCluster, nearestCorePointMap);
                clusters.add(cluster);
            }
        }

        return clusters;
    }


    private void bfsCluster(Map<String, Set<String>> adjacencyList, Map<String, Boolean> isCorePoint, Set<String> visited,
                            String startPoint, Cluster cluster, Map<String, Integer> pointToCluster,
                            Map<String, String> nearestCorePointMap) {
        Queue<String> queue = new LinkedList<>();
        queue.add(startPoint);
        visited.add(startPoint);
        cluster.addCoreTrajectory(startPoint);
        pointToCluster.put(startPoint, cluster.getClusterId());

        while (!queue.isEmpty()) {
            String current = queue.poll();
            Set<String> neighbors = adjacencyList.getOrDefault(current, Collections.emptySet());

            for (String neighbor : neighbors) {
                if (visited.contains(neighbor)) {
                    continue;
                }

                if (isCorePoint.getOrDefault(neighbor, false)) {
                    visited.add(neighbor);
                    cluster.addCoreTrajectory(neighbor);
                    queue.add(neighbor);
                    pointToCluster.put(neighbor, cluster.getClusterId());
                } else if (nearestCorePointMap.containsKey(neighbor)) {
                    visited.add(neighbor);
                    cluster.addBorderTrajectory(neighbor);
                    pointToCluster.put(neighbor, cluster.getClusterId());
                }
            }
        }
    }

}
