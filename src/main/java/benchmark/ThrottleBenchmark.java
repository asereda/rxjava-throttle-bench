package benchmark;


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.Throughput)
public class ThrottleBenchmark {

    @Param({
       "EXECUTOR",
       "RXJAVA_FLATMAP",
       "RXJAVA_OPERATOR"
    })
    private Factory factory;


    /**
     * Duration in millis
     */
    @Param("100")
    private int window;


    /**
     * number of keys
     */
    @Param("1048576")
    private int buckets;


    /**
     * Current throttler
     */
    private Throttler<Long> throttler;


    @State(Scope.Thread)
    public static class ThreadState {
        private long index;
    }


    @Setup
    public void setup() throws Exception {
        throttler = factory.create(Duration.ofMillis(window), key -> key % buckets);
        throttler.start();
    }


    @TearDown
    public void teardown() throws Exception {
        Blackhole.consumeCPU(throttler.count());
        throttler.stop();
    }

    @Benchmark
    @Threads(4)
    public void consume(ThreadState state) {
        throttler.accept(state.index++);
    }





}
