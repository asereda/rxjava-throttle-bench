package benchmark;


import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Uses scheduled executor + map to implement throttling logic
 */
class ExecutorThrottler<K, T> implements Throttler<T> {

    private final long nanos;
    private final Function<? super T,? extends K> keySelector;
    private final AtomicLong count;
    private final ScheduledExecutorService executor;
    private final ConcurrentMap<K, Holder<K, T>> map;
    private final Consumer<T> consumer;

    ExecutorThrottler(Duration duration, Function<? super T, ? extends K> keySelector) {
        Objects.requireNonNull(duration, "duration");
        if (duration.isNegative()) throw new IllegalArgumentException("Negative duration: " + duration);
        this.nanos = duration.toNanos();
        this.keySelector = Objects.requireNonNull(keySelector, "keySelector");
        this.count = new AtomicLong();
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.map = new ConcurrentHashMap<>();
        this.consumer = event -> count.incrementAndGet();
    }

    @Override
    public void stop() throws Exception {
        map.clear();
        executor.shutdown();
        if (!executor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
            executor.shutdownNow();
        }
    }

    @Override
    public long count() {
        return count.get();
    }

    @Override
    public void accept(T event) {
        final K key = keySelector.apply(event);
        final Holder<K, T> holder = map.computeIfAbsent(key, k -> new Holder<>(k, consumer));
        holder.setValue(event);

        if (!holder.publishing.get() && holder.publishing().compareAndSet(false, true)) {
            executor.schedule(holder::onPublish, nanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Keeps current publishing state for each key.
     *
     */
    private static class Holder<A, B> implements Map.Entry<A,B> {
        private final A key;
        private final Consumer<B> consumer;

        private volatile B value;

        private final AtomicBoolean publishing;

        private Holder(A key, Consumer<B> consumer) {
            this.key = key;
            this.consumer = consumer;
            this.publishing = new AtomicBoolean();
        }

        @Override
        public A getKey() {
            return key;
        }

        @Override
        public B getValue() {
            return value;
        }

        @Override
        public B setValue(B value) {
            this.value = value;
            return value;
        }

        AtomicBoolean publishing() {
            return this.publishing;
        }

        void onPublish() {
            publishing.set(false);
            consumer.accept(value);
        }
    }
}
