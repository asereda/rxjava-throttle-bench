package benchmark;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class RxJava {

    static <K, T> Throttler<T> flatMap(Duration duration, Function<? super T, ? extends K> keySelector) {
        final Subject<T,T> subject = PublishSubject.<T>create().toSerialized();

        Function<Observable<T>, Observable<T>> func = observer -> observer.groupBy(keySelector::apply)
                .flatMap(obs -> obs.throttleLast(duration.toNanos(), TimeUnit.NANOSECONDS));

        return new RxJavaThrottler<K, T>(subject, subject.compose(func::apply));
    }

    /**
     *  Uses rxjava operator
     */
    static <K, T> Throttler<T> operator(Duration duration, Function<? super T, ? extends K> keySelector) {
        final Subject<T,T> subject = PublishSubject.<T>create().toSerialized();

        Observable<T> observable = subject.lift(new ThrottleOperator<K, T>(duration, keySelector, Schedulers.computation()));
        return new RxJavaThrottler<K, T>(subject, observable);
    }


    private static class RxJavaThrottler<K,T> implements Throttler<T> {

        private final AtomicLong counter;
        private final Subject<T, T> subject;
        private final Subscription subscription;

        public RxJavaThrottler(Subject<T, T> subject, Observable<T> observable) {
            this.subject = subject;
            final AtomicLong counter = new AtomicLong();
            this.counter = counter;
            this.subscription = observable.subscribe( t -> counter.incrementAndGet());
        }


        @Override
        public long count() {
            return counter.get();
        }

        @Override
        public void accept(T event) {
            subject.onNext(event);
        }

        @Override
        public void stop() throws Exception {
            subscription.unsubscribe();
        }
    }


    private static class ThrottleOperator<K, T> implements Observable.Operator<T, T> {

        private final long nanos;
        private final Function<? super T, ? extends K> keySelector;
        private final Scheduler scheduler;

        public ThrottleOperator(Duration duration, Function<? super T, ? extends K> keySelector, Scheduler scheduler) {
            this.nanos = Objects.requireNonNull(duration, "duration").toNanos();
            this.keySelector = Objects.requireNonNull(keySelector, "keySelector");
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        }


        @Override
        public Subscriber<? super T> call(Subscriber<? super T> child) {
            return new Subscriber<T>() {
                ConcurrentMap<K, Holder<K,T>> map = new ConcurrentHashMap<K, Holder<K, T>>();
                final Scheduler.Worker worker = scheduler.createWorker();

                @Override
                public void onCompleted() {
                    map = null;
                    worker.unsubscribe();
                    child.onCompleted();
                }

                @Override
                public void onError(Throwable throwable) {
                    map = null;
                    worker.unsubscribe();
                    child.onError(throwable);
                }

                @Override
                public void onNext(T event) {

                    final K key = keySelector.apply(event);
                    final Holder<K, T> holder = map.computeIfAbsent(key, k -> new Holder<K, T>(k, child));
                    holder.setValue(event);

                    if (!holder.publishing().get() && holder.publishing().compareAndSet(false, true)) {
                        worker.schedule(holder::onPublish, nanos, TimeUnit.NANOSECONDS);
                    }

                    request(1);

                }
            };
        }

        private static class Holder<A, B> implements Map.Entry<A, B> {

            private final A key;
            private final Subscriber<? super B> subscriber;

            /**
             * Last observed value
             */
            private volatile B value;

            private final AtomicBoolean publishing;

            Holder(A key, Subscriber<? super B> subscriber) {
                this.key = key;
                this.subscriber = subscriber;
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
                return publishing;
            }

            void onPublish() {
                publishing.set(false);
                subscriber.onNext(value);
            }
        }

    }

}
