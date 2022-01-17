package com.jonas.api.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义实现并行度1的Source
 */
public class NoParallelSource implements SourceFunction<Long> {
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
