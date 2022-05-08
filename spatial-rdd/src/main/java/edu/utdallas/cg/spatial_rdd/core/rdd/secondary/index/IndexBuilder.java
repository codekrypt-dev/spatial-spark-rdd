/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.utdallas.cg.spatial_rdd.core.rdd.secondary.index;

import edu.utdallas.cg.spatial_rdd.core.rdd.secondary.index.impl.kdtree.KdTreeIndex;
import edu.utdallas.cg.spatial_rdd.enums.IndexType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public final class IndexBuilder<T extends Geometry>
    implements FlatMapFunction<Iterator<T>, SpatialIndex> {
  IndexType indexType;

  public IndexBuilder(IndexType indexType) {
    this.indexType = indexType;
  }

  @Override
  public Iterator<SpatialIndex> call(Iterator<T> objectIterator) throws Exception {
    SpatialIndex spatialIndex = null;
    if (indexType == IndexType.RTREE) {
      spatialIndex = new STRtree();
    } else if (indexType == IndexType.QUADTREE) {
      spatialIndex = new Quadtree();
    } else if (indexType == IndexType.KDTREE) {
      spatialIndex = new KdTreeIndex();
    }

    while (objectIterator.hasNext()) {
      T spatialObject = objectIterator.next();
      spatialIndex.insert(spatialObject.getEnvelopeInternal(), spatialObject);
    }

    Set<SpatialIndex> result = new HashSet();
    result.add(spatialIndex);
    return result.iterator();
  }
}