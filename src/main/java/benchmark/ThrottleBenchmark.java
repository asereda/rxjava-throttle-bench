package benchmark;


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.Throughput)
public class ThrottleBenchmark {

    @Param({
       "EXECUTOR",
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


    /**
     * Current index
     */
    private long index;


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
    public void consume() {
        throttler.accept(index++);
    }





}
