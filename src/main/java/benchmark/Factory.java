package benchmark;

import java.time.Duration;
import java.util.function.Function;

public enum Factory {

    EXECUTOR {
        @Override
        <T, K> Throttler<T> create(Duration duration, Function<T, K> keySelector) {
            return new ExecutorThrottler<>(duration, keySelector);
        }
    },

    RXJAVA_FLATMAP {
        @Override
        <T, K> Throttler<T> create(Duration duration, Function<T, K> keySelector) {
            return RxJava.flatMap(duration, keySelector);
        }
    },

    RXJAVA_OPERATOR {
        @Override
        <T, K> Throttler<T> create(Duration duration, Function<T, K> keySelector) {
            return RxJava.operator(duration, keySelector);
        }
    };


    abstract <T, K> Throttler<T> create(Duration duration, Function<T, K> keySelector);

}
