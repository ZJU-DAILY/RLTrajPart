package funs;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class OptimizedHeartbeatProcessFunction extends KeyedProcessFunction<Integer, Tuple6<Integer,Integer,Long,Long,Integer,Long>, Tuple6<Integer,Integer,Long,Long,Integer,Long>> {

    private transient ValueState<Long> nextScheduledHeartbeat;
    private final long heartbeatIntervalMs;
    public OptimizedHeartbeatProcessFunction(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        nextScheduledHeartbeat = getRuntimeContext().getState(new ValueStateDescriptor<>("next-scheduled-heartbeat", Long.class));
    }

    @Override
    public void processElement(Tuple6<Integer,Integer,Long,Long,Integer,Long> value, Context ctx, Collector<Tuple6<Integer,Integer,Long,Long,Integer,Long>> out) throws Exception {
        out.collect(value);

        Long currentScheduledTime = nextScheduledHeartbeat.value();
        long newScheduledTime = value.f2 + heartbeatIntervalMs;

        if (currentScheduledTime == null || newScheduledTime < currentScheduledTime) {
            nextScheduledHeartbeat.update(newScheduledTime);
            if (currentScheduledTime != null) {
                ctx.timerService().deleteEventTimeTimer(currentScheduledTime);
            }
//            System.out.println("regist");
            ctx.timerService().registerEventTimeTimer(newScheduledTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple6<Integer,Integer,Long,Long,Integer,Long>> out) throws Exception {
//        System.out.println("out");
        out.collect(new Tuple6<>(ctx.getCurrentKey(), -1, timestamp, 0L, 0, 0L));
        long nextHeartbeat = timestamp + heartbeatIntervalMs;
        ctx.timerService().registerEventTimeTimer(nextHeartbeat);
        nextScheduledHeartbeat.update(nextHeartbeat);
    }
}
