package benchmark;


import java.util.function.Consumer;

public interface Throttler<T> extends Consumer<T> {

    default void start() throws Exception {

    }

    default void stop() throws Exception {

    }

    /**
     * Number of messages published after applying flow control
     */
    long count();

}
