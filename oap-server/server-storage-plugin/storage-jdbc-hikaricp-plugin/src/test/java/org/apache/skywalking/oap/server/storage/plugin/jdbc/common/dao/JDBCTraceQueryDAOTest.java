/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.jdbc.common.dao;

import org.apache.skywalking.oap.server.core.analysis.IDManager;
import org.apache.skywalking.oap.server.core.analysis.manual.segment.SegmentRecord;
import org.apache.skywalking.oap.server.core.query.type.QueryOrder;
import org.apache.skywalking.oap.server.core.query.type.TraceBrief;
import org.apache.skywalking.oap.server.core.query.type.TraceState;
import org.apache.skywalking.oap.server.library.client.jdbc.hikaricp.JDBCClient;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.util.BooleanUtils;
import org.apache.skywalking.oap.server.storage.plugin.jdbc.common.JDBCTableInstaller;
import org.apache.skywalking.oap.server.storage.plugin.jdbc.common.TableHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JDBCTraceQueryDAOTest {

    private static final String TABLE = "segment_20260406";

    @Mock
    private JDBCClient jdbcClient;
    @Mock
    private ModuleManager moduleManager;
    @Mock
    private TableHelper tableHelper;

    private JDBCTraceQueryDAO dao;

    @BeforeEach
    void setUp() {
        dao = new JDBCTraceQueryDAO(moduleManager, jdbcClient, tableHelper);
    }

    @Test
    void queryByTraceId_shouldContainTableColumnAndTraceIdCondition() throws Exception {
        when(tableHelper.getTablesWithinTTL(SegmentRecord.INDEX_NAME))
            .thenReturn(Collections.singletonList(TABLE));

        final AtomicReference<String> capturedSql = new AtomicReference<>();
        doAnswer(invocation -> {
            capturedSql.set(invocation.getArgument(0));
            return Collections.emptyList();
        }).when(jdbcClient).executeQuery(anyString(), any(), any(Object[].class));

        dao.queryByTraceId("trace-abc", null);

        final String sql = capturedSql.get();
        assertThat(sql).contains(JDBCTableInstaller.TABLE_COLUMN + " = ?");
        assertThat(sql).contains(SegmentRecord.TRACE_ID + " = ?");
        // TABLE_COLUMN should appear exactly once
        assertThat(countOccurrences(sql, JDBCTableInstaller.TABLE_COLUMN + " = ?")).isEqualTo(1);
    }

    @Test
    void queryBySegmentIdList_shouldUseInClause() throws Exception {
        when(tableHelper.getTablesWithinTTL(SegmentRecord.INDEX_NAME))
            .thenReturn(Collections.singletonList(TABLE));

        final AtomicReference<String> capturedSql = new AtomicReference<>();
        doAnswer(invocation -> {
            capturedSql.set(invocation.getArgument(0));
            return Collections.emptyList();
        }).when(jdbcClient).executeQuery(anyString(), any(), any(Object[].class));

        dao.queryBySegmentIdList(Arrays.asList("seg-1", "seg-2", "seg-3"), null);

        final String sql = capturedSql.get();
        assertThat(sql).contains(JDBCTableInstaller.TABLE_COLUMN + " = ?");
        assertThat(sql).contains(SegmentRecord.SEGMENT_ID + " in (?,?,?)");
        assertThat(sql).doesNotContain(" or ");
    }

    @Test
    void queryByTraceIdWithInstanceId_shouldProduceValidSqlWithBothInClauses() throws Exception {
        when(tableHelper.getTablesWithinTTL(SegmentRecord.INDEX_NAME))
            .thenReturn(Collections.singletonList(TABLE));

        final AtomicReference<String> capturedSql = new AtomicReference<>();
        doAnswer(invocation -> {
            capturedSql.set(invocation.getArgument(0));
            return Collections.emptyList();
        }).when(jdbcClient).executeQuery(anyString(), any(), any(Object[].class));

        dao.queryByTraceIdWithInstanceId(
            Arrays.asList("trace-1", "trace-2"),
            Arrays.asList("instance-1", "instance-2"),
            null
        );

        final String sql = capturedSql.get();
        assertThat(sql).contains(JDBCTableInstaller.TABLE_COLUMN + " = ?");
        assertThat(sql).contains(SegmentRecord.TRACE_ID + " in (?,?)");
        assertThat(sql).contains(" and " + SegmentRecord.SERVICE_INSTANCE_ID + " in (?,?)");
        // verify the IN clauses are both properly enclosed with parentheses
        assertThat(sql).containsPattern("trace_id in \\(\\?,\\?\\) and service_instance_id in \\(\\?,\\?\\)");
    }

    @Test
    void queryByTraceIdWithInstanceId_withSingleItems_shouldProduceValidSql() throws Exception {
        when(tableHelper.getTablesWithinTTL(SegmentRecord.INDEX_NAME))
            .thenReturn(Collections.singletonList(TABLE));

        final AtomicReference<String> capturedSql = new AtomicReference<>();
        doAnswer(invocation -> {
            capturedSql.set(invocation.getArgument(0));
            return Collections.emptyList();
        }).when(jdbcClient).executeQuery(anyString(), any(), any(Object[].class));

        dao.queryByTraceIdWithInstanceId(
            Collections.singletonList("trace-1"),
            Collections.singletonList("instance-1"),
            null
        );

        final String sql = capturedSql.get();
        assertThat(sql).contains(SegmentRecord.TRACE_ID + " in (?)");
        assertThat(sql).contains(" and " + SegmentRecord.SERVICE_INSTANCE_ID + " in (?)");
    }

    @Test
    void queryBasicTraces_shouldUsePerTableLimitWithoutOffset() throws Exception {
        when(tableHelper.getTablesWithinTTL(SegmentRecord.INDEX_NAME))
            .thenReturn(Collections.singletonList(TABLE));

        final AtomicReference<String> capturedSql = new AtomicReference<>();
        doAnswer(invocation -> {
            capturedSql.set(invocation.getArgument(0));
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(false);
            JDBCClient.ResultHandler<?> handler = invocation.getArgument(1);
            return handler.handle(rs);
        }).when(jdbcClient).executeQuery(anyString(), any(), any(Object[].class));

        dao.queryBasicTraces(
            null, 0, 0, null, null, null, null,
            20, 20, TraceState.ALL, QueryOrder.BY_START_TIME, null
        );

        final String sql = capturedSql.get();
        // from=20, limit=20 → need first 40 rows of each day table; no SQL OFFSET
        assertThat(sql).contains(" LIMIT 40");
        assertThat(sql).doesNotContain("OFFSET");
    }

    @Test
    void queryBasicTraces_shouldGloballySortAndPageAcrossDayTables() throws Exception {
        when(tableHelper.getTablesWithinTTL(SegmentRecord.INDEX_NAME))
            .thenReturn(Arrays.asList("segment_20260406", "segment_20260407"));

        final String endpointId = IDManager.EndpointID.buildId(
            IDManager.ServiceID.buildId("svc", true), "/api"
        );

        // Day-older table is iterated first (ascending day buckets). Without a global merge,
        // page 1 would start with the older day's newest rows.
        doAnswer(invocation -> {
            final String sql = invocation.getArgument(0);
            final JDBCClient.ResultHandler<?> handler = invocation.getArgument(1);
            final ResultSet rs = mock(ResultSet.class);
            if (sql.contains("segment_20260406")) {
                when(rs.next()).thenReturn(true, true, false);
                when(rs.getString(SegmentRecord.SEGMENT_ID)).thenReturn("old-1", "old-2");
                when(rs.getString(SegmentRecord.START_TIME)).thenReturn("1000", "2000");
                when(rs.getString(SegmentRecord.ENDPOINT_ID)).thenReturn(endpointId, endpointId);
                when(rs.getInt(SegmentRecord.LATENCY)).thenReturn(10, 20);
                when(rs.getInt(SegmentRecord.IS_ERROR)).thenReturn(BooleanUtils.FALSE, BooleanUtils.FALSE);
                when(rs.getString(SegmentRecord.TRACE_ID)).thenReturn("t-old-1", "t-old-2");
            } else {
                when(rs.next()).thenReturn(true, true, false);
                when(rs.getString(SegmentRecord.SEGMENT_ID)).thenReturn("new-1", "new-2");
                when(rs.getString(SegmentRecord.START_TIME)).thenReturn("3000", "4000");
                when(rs.getString(SegmentRecord.ENDPOINT_ID)).thenReturn(endpointId, endpointId);
                when(rs.getInt(SegmentRecord.LATENCY)).thenReturn(30, 40);
                when(rs.getInt(SegmentRecord.IS_ERROR)).thenReturn(BooleanUtils.FALSE, BooleanUtils.FALSE);
                when(rs.getString(SegmentRecord.TRACE_ID)).thenReturn("t-new-1", "t-new-2");
            }
            return handler.handle(rs);
        }).when(jdbcClient).executeQuery(anyString(), any(), any(Object[].class));

        final TraceBrief brief = dao.queryBasicTraces(
            null, 0, 0, null, null, null, null,
            2, 0, TraceState.ALL, QueryOrder.BY_START_TIME, null
        );

        assertThat(brief.getTraces()).hasSize(2);
        assertThat(brief.getTraces().get(0).getSegmentId()).isEqualTo("new-2");
        assertThat(brief.getTraces().get(1).getSegmentId()).isEqualTo("new-1");
    }

    @Test
    void queryBasicTraces_pageTwo_shouldSkipGloballyNotPerTable() throws Exception {
        when(tableHelper.getTablesWithinTTL(SegmentRecord.INDEX_NAME))
            .thenReturn(Arrays.asList("segment_20260406", "segment_20260407"));

        final String endpointId = IDManager.EndpointID.buildId(
            IDManager.ServiceID.buildId("svc", true), "/api"
        );

        doAnswer(invocation -> {
            final String sql = invocation.getArgument(0);
            final JDBCClient.ResultHandler<?> handler = invocation.getArgument(1);
            final ResultSet rs = mock(ResultSet.class);
            if (sql.contains("segment_20260406")) {
                when(rs.next()).thenReturn(true, true, false);
                when(rs.getString(SegmentRecord.SEGMENT_ID)).thenReturn("old-1", "old-2");
                when(rs.getString(SegmentRecord.START_TIME)).thenReturn("1000", "2000");
                when(rs.getString(SegmentRecord.ENDPOINT_ID)).thenReturn(endpointId, endpointId);
                when(rs.getInt(SegmentRecord.LATENCY)).thenReturn(10, 20);
                when(rs.getInt(SegmentRecord.IS_ERROR)).thenReturn(BooleanUtils.FALSE, BooleanUtils.FALSE);
                when(rs.getString(SegmentRecord.TRACE_ID)).thenReturn("t-old-1", "t-old-2");
            } else {
                when(rs.next()).thenReturn(true, true, false);
                when(rs.getString(SegmentRecord.SEGMENT_ID)).thenReturn("new-1", "new-2");
                when(rs.getString(SegmentRecord.START_TIME)).thenReturn("3000", "4000");
                when(rs.getString(SegmentRecord.ENDPOINT_ID)).thenReturn(endpointId, endpointId);
                when(rs.getInt(SegmentRecord.LATENCY)).thenReturn(30, 40);
                when(rs.getInt(SegmentRecord.IS_ERROR)).thenReturn(BooleanUtils.FALSE, BooleanUtils.FALSE);
                when(rs.getString(SegmentRecord.TRACE_ID)).thenReturn("t-new-1", "t-new-2");
            }
            return handler.handle(rs);
        }).when(jdbcClient).executeQuery(anyString(), any(), any(Object[].class));

        // Global order desc by start: new-2, new-1, old-2, old-1. Page 2 (from=2, limit=2) → old-2, old-1
        final TraceBrief brief = dao.queryBasicTraces(
            null, 0, 0, null, null, null, null,
            2, 2, TraceState.ALL, QueryOrder.BY_START_TIME, null
        );

        assertThat(brief.getTraces()).hasSize(2);
        assertThat(brief.getTraces().get(0).getSegmentId()).isEqualTo("old-2");
        assertThat(brief.getTraces().get(1).getSegmentId()).isEqualTo("old-1");
    }

    private long countOccurrences(final String text, final String pattern) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(pattern, index)) != -1) {
            count++;
            index += pattern.length();
        }
        return count;
    }
}
