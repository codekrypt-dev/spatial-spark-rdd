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

package edu.utdallas.cg.spatial_rdd.core.query.range.filter;

import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;

public class BaseFilter<U extends Geometry> implements Serializable {

  private final boolean considerBoundaryIntersection;
  protected boolean leftCoveredByRight;
  protected U queryGeometry;

  /**
   * Instantiates a new range filter using index.
   *
   * @param queryWindow the query window
   * @param considerBoundaryIntersection the consider boundary intersection
   */
  public BaseFilter(
      U queryWindow, boolean considerBoundaryIntersection, boolean leftCoveredByRight) {
    this.considerBoundaryIntersection = considerBoundaryIntersection;
    this.queryGeometry = queryWindow;
    this.leftCoveredByRight = leftCoveredByRight;
  }

  public boolean match(Geometry spatialObject, Geometry queryWindow) {
    if (considerBoundaryIntersection) {
      return queryWindow.intersects(spatialObject);
    } else {
      return queryWindow.covers(spatialObject);
    }
  }
}
