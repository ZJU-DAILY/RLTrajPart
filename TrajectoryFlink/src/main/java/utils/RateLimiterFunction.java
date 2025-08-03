package utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;

public class RateLimiterFunction<T> extends RichMapFunction<T, T> {
    private static final long serialVersionUID = 1L;
    private static final String LIMIT_RATE_KEY = "udf.limit.rate";
    private static final String LIMIT_RATE_DEFAULT = "500000";

    private transient GuavaFlinkConnectorRateLimiter rateLimiter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String rateStr = globalJobParameters.toMap().getOrDefault(LIMIT_RATE_KEY, LIMIT_RATE_DEFAULT);
        long rate = Long.parseLong(rateStr);

        rateLimiter = new GuavaFlinkConnectorRateLimiter();
        rateLimiter.setRate(rate);
        rateLimiter.open(getRuntimeContext());
        System.out.println("RateLimiter initialized with rate: " + rate + " permits per second");
    }

    @Override
    public T map(T value) throws Exception {
        if (rateLimiter != null) {
            rateLimiter.acquire(1);
        }
        return value;
    }
}
