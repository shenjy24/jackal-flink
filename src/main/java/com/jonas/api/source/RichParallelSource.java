package com.jonas.api.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义支持多并行度的Source
 *
 * 如果在source中需要获取其他链接资源，那么可以在open方法中打开资源链接，在close中关闭资源链接
 */
public class RichParallelSource extends RichParallelSourceFunction<Long> {

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

    /**
     * 该方法只会在最开始的时候被调用一次
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open...");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
