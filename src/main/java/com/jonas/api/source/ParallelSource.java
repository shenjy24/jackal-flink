package com.jonas.api.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 自定义支持多并行度的Source
 */
public class ParallelSource implements ParallelSourceFunction<Long> {

    private long count = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count++);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
