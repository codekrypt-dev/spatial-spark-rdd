package perf;

import edu.utdallas.cg.spatial_rdd.Driver;
import edu.utdallas.cg.spatial_rdd.core.query.range.RangeQuery;
import edu.utdallas.cg.spatial_rdd.core.rdd.SpatialRDD;
import edu.utdallas.cg.spatial_rdd.enums.GridType;
import edu.utdallas.cg.spatial_rdd.enums.IndexType;
import edu.utdallas.cg.spatial_rdd.file.io.impl.GeoJsonRddReader;
import org.locationtech.jts.geom.Envelope;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import perf.utils.RandomUtils;

import java.util.ArrayList;
import java.util.List;

@State(Scope.Thread)
public class RddBenchmarks extends BaseBenchmark {

  // We will test with the below list sizes.
  @Param({"1", "10", "100", "1000"})
  public int queryListSize;

  public List<Envelope> queryList;
  public SpatialRDD spatialRDD;

  @Setup
  public void setUp() {

    // 1. Load
    String inputPath = Driver.class.getClassLoader().getResource("geojson_points.json").getPath();
    spatialRDD = GeoJsonRddReader.readToGeometryRDD(jsc(), inputPath);

    // 2. PreProcess
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KD_TREE);
    spatialRDD.buildIndex(IndexType.KDTREE);

    queryList = createQueryTestData();
  }

  @Benchmark
  public void query_on_hash_hash(Blackhole bh) {
    for (Envelope queryEnvelope : queryList) {
      long result = RangeQuery.spatialRangeQuery(spatialRDD, queryEnvelope, false).count();
      bh.consume(result);
    }
  }

  @Benchmark
  public void query_on_kdTree_kdTree(Blackhole bh) {
    for (Envelope queryEnvelope : queryList) {
      long result = RangeQuery.spatialRangeQuery(spatialRDD, queryEnvelope, true).count();
      bh.consume(result);
    }
  }

  private List<Envelope> createQueryTestData() {
    List<Envelope> result = new ArrayList<>();

    int x1 = RandomUtils.valueBetween(-300, 90);
    int x2 = RandomUtils.valueBetween(-300, 90);
    int y1 = RandomUtils.valueBetween(-300, 90);
    int y2 = RandomUtils.valueBetween(-300, 90);

    for (int i = 0; i < queryListSize; i++) result.add(new Envelope(x1, x2, y1, y2));

    return result;
  }
}
