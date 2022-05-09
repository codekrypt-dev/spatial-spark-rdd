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
  public SpatialRDD spatialRDD1;
  public SpatialRDD spatialRDD2;

  @Setup
  public void setUp() {

    // 1. Load
    String inputPath = Driver.class.getClassLoader().getResource("geojson_points.json").getPath();
    spatialRDD1 = GeoJsonRddReader.readToGeometryRDD(jsc(), inputPath);
    spatialRDD2 = GeoJsonRddReader.readToGeometryRDD(jsc(), inputPath);

    // 2.a PreProcess (KD Tre)
    spatialRDD1.analyze();
    spatialRDD1.spatialPartitioning(GridType.KD_TREE);
    spatialRDD1.buildIndex(IndexType.KDTREE);


    // 2.b PreProcess (KD Tre)
    spatialRDD2.analyze();
    spatialRDD2.spatialPartitioning(GridType.KDB_TREE);
    spatialRDD2.buildIndex(IndexType.RTREE);

    queryList = createQueryTestData();
  }

  @Benchmark
  public void query_on_hash_hash(Blackhole bh) {
    for (Envelope queryEnvelope : queryList) {
      long result = RangeQuery.spatialRangeQuery(spatialRDD1, queryEnvelope, false).count();
      bh.consume(result);
    }
  }

  @Benchmark
  public void query_on_kdTree_kdTree(Blackhole bh) {
    for (Envelope queryEnvelope : queryList) {
      long result = RangeQuery.spatialRangeQuery(spatialRDD1, queryEnvelope, true).count();
      bh.consume(result);
    }
  }

  @Benchmark
  public void query_on_kdbTree_rTree(Blackhole bh) {
    for (Envelope queryEnvelope : queryList) {
      long result = RangeQuery.spatialRangeQuery(spatialRDD2, queryEnvelope, true).count();
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
