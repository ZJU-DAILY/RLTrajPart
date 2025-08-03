package funs;


import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SubtaskAndStateCountFunction extends KeyedProcessFunction<Integer, Tuple6<Integer,Integer,Long,Long,Integer,Long>, Tuple6<Integer, Long, Long,Integer,Long, int[]>> {

    private transient MapState<Integer, int[]> stateCountsPerGridSize;
    private transient MapState<Integer, Long> subtaskCountPerGridSize;
    private transient ValueState<Long> lastSendTime;
    private transient ValueState<Integer> lastNonZeroGridSize;
    private transient ValueState<Long> lastNonZeroIndexSize;
    private final long timeGap = 15 * 60 * 1000;
    private final int stateSize;

    public SubtaskAndStateCountFunction(int stateSize) {
        this.stateSize = stateSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        stateCountsPerGridSize = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("stateCountsPerGridSize", Integer.class, int[].class));
        subtaskCountPerGridSize = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("subtaskCountPerGridSize", Integer.class, Long.class));
        lastSendTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastSendTime", Long.class));
        lastNonZeroGridSize = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastNonZeroGridSize", Integer.class));
        lastNonZeroIndexSize = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastNonZeroIndexSize", Long.class));
    }

    @Override
    public void processElement(Tuple6<Integer,Integer,Long,Long,Integer,Long> value, Context ctx, Collector<Tuple6<Integer, Long, Long,Integer,Long, int[]>> out) throws Exception {
        int subTaskId = value.f0;
        int stateIndex = value.f1;
        long timestamp = value.f2;
        long count = value.f3;
        int gridSize = value.f4;
        long indexSize = value.f5;

        if (lastSendTime.value() == null) {
            lastSendTime.update(1201930244000L);
        }
        if(lastNonZeroGridSize.value() == null)
        {
            lastNonZeroGridSize.update(0);
        }

        if (gridSize != 0) {
            lastNonZeroGridSize.update(gridSize);
            lastNonZeroIndexSize.update(indexSize);
        }

        Long currentSubtaskCount = subtaskCountPerGridSize.get(gridSize);
        if (currentSubtaskCount == null) {
            currentSubtaskCount = 0L;
        }
        subtaskCountPerGridSize.put(gridSize, currentSubtaskCount + count);

        int[] currentCounts = stateCountsPerGridSize.get(gridSize);
        if (currentCounts == null) {
            currentCounts = new int[stateSize * stateSize];
        }
        if (stateIndex != -1) {
            currentCounts[stateIndex]++;
        }
        stateCountsPerGridSize.put(gridSize, currentCounts);

        if (timestamp - lastSendTime.value() >= timeGap) {
            Integer lastGridSize = lastNonZeroGridSize.value();
            long gap = Math.floorDiv(timestamp - lastSendTime.value(),timeGap);
            if(lastGridSize != 0) {
                int[] states = stateCountsPerGridSize.get(lastGridSize);
                long subtaskCount = subtaskCountPerGridSize.get(lastGridSize);
                out.collect(new Tuple6<>(subTaskId, lastSendTime.value() + gap*timeGap, subtaskCount, lastGridSize, lastNonZeroIndexSize.value(), states));
            }
            else {
                out.collect(new Tuple6<>(subTaskId, lastSendTime.value() + gap*timeGap, 0L, 0, 0L, new int[stateSize * stateSize]));
            }
            stateCountsPerGridSize.put(lastGridSize, new int[stateSize * stateSize]);
            subtaskCountPerGridSize.put(lastGridSize, 0L);
            clearStates();
            lastNonZeroGridSize.update(0);
            lastNonZeroIndexSize.update(0L);
            lastSendTime.update(lastSendTime.value()+gap*timeGap);
        }
    }
    private void clearStates() throws Exception {
        stateCountsPerGridSize.clear();
        subtaskCountPerGridSize.clear();
    }
}