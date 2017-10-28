package org.influxdb.dto;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/**
 * @author brett.wooldridge [at] gmail.com
 */
@State(Scope.Benchmark)
@Warmup(iterations=3)
@Measurement(iterations=8)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings("unused")
public class PointPoolingBenchmark {
  @Param({ "default", "storm" })
  public String pool;

  @Setup(Level.Trial)
  public void configurePool() {
    System.setProperty("org.influxdb.pointBuilderPool", pool);
  }

  @Benchmark
  public static Point cyclePoints() {
    return Point.measurement("test")
      .tag("tag1", "value1")
      .tag("tag2", "value2")
      .addField("field1", 1.0)
      .build();
  }
}
