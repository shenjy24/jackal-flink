package com.jonas.api.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义实现并行度1的Source
 */
public class NoParallelSource implements SourceFunction<Long> {
    private long count = 1L;
    private boolean isRunning = true;

    /**
     * 大部分情况下，都需要在这个方法中实现一个循环，这样就可以循环产生数据
     */
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
