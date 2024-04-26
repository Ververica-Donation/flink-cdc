/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.tidb.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit test for {@link TiDBSourceMetrics}. */
public class TiDBSourceMetricsTest {
  private static final String FETCH_EVENTTIME_LAG = "currentFetchEventTimeLag";
  private static final String EMIT_EVENTTIME_LAG = "currentEmitEventTimeLag";
  private MetricListener metricListener;
  private TiDBSourceMetrics sourceMetrics;

  @Before
  public void setUp() {
    metricListener = new MetricListener();
    sourceMetrics = new TiDBSourceMetrics(metricListener.getMetricGroup());
    sourceMetrics.registerMetrics();
  }

  @Test
  public void testFetchEventTimeLagTracking() {
    sourceMetrics.recordFetchDelay(5L);
    assertGauge(metricListener, FETCH_EVENTTIME_LAG, 5L);
  }

  @Test
  public void testEmitEventTimeLagTracking() {
    sourceMetrics.recordEmitDelay(3L);
    assertGauge(metricListener, EMIT_EVENTTIME_LAG, 3L);
  }

  private void assertGauge(MetricListener metricListener, String identifier, long expected) {
    Optional<Gauge<Object>> gauge = metricListener.getGauge(identifier);
    assertTrue(gauge.isPresent());
    assertEquals(expected, (long) gauge.get().getValue());
  }
}