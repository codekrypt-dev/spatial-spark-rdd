package perf;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.junit.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime) // We are going to find Average Time taken for @Benchmark
@OutputTimeUnit(TimeUnit.MILLISECONDS) // Unit of measurement is Milliseconds
public class BaseBenchmark extends SharedJavaSparkContext implements Serializable {
  private static final long serialVersionUID = -5681683598336701496L;

  // Will perform WARMUP_ITERATIONS with the load, before actually measuring.
  private static final Integer WARMUP_ITERATIONS = 5;

  // Will perform MEASUREMENT_ITERATIONS with the load.
  private static final Integer MEASUREMENT_ITERATIONS = 5;

  @Test
  public void executeJmhRunner() throws RunnerException {
    Options opts =
        new OptionsBuilder()
            // set the class name regex for benchmarks to search for, to the current class
            .include("\\." + this.getClass().getSimpleName() + "\\.")
            .measurementIterations(MEASUREMENT_ITERATIONS)
            .warmupIterations(WARMUP_ITERATIONS)
            .forks(0) // do not use forking
            .threads(1) // do not use multiple threads
            .shouldDoGC(true)
            .shouldFailOnError(true)
            .result("benchmark-result.csv")
            .resultFormat(ResultFormatType.CSV)
            .build();

    new Runner(opts).run();
  }
}
