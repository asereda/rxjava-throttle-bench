# rxjava-throttle-bench

Tries to compare throttling options with RXJava using
- groupBy + flatmap
- groupBy + PublishSubject
- Observable.Operator via lift() method

# to run

```
$ mvn package
$ java -jar target/benchmarks.jar
```

