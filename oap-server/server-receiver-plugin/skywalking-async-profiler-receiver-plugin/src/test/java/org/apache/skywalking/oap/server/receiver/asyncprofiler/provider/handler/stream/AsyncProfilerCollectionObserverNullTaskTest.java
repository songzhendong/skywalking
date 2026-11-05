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

package org.apache.skywalking.oap.server.receiver.asyncprofiler.provider.handler.stream;

import io.grpc.stub.StreamObserver;
import org.apache.skywalking.apm.network.language.asyncprofiler.v10.AsyncProfilerCollectionResponse;
import org.apache.skywalking.apm.network.language.asyncprofiler.v10.AsyncProfilerData;
import org.apache.skywalking.apm.network.language.asyncprofiler.v10.AsyncProfilerMetaData;
import org.apache.skywalking.apm.network.language.asyncprofiler.v10.AsyncProfilingStatus;
import org.apache.skywalking.oap.server.core.source.SourceReceiver;
import org.apache.skywalking.oap.server.core.storage.profiling.asyncprofiler.IAsyncProfilerTaskQueryDAO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AsyncProfilerCollectionObserverNullTaskTest {

    @Mock
    private IAsyncProfilerTaskQueryDAO taskDAO;
    @Mock
    private SourceReceiver sourceReceiver;
    @Mock
    private StreamObserver<AsyncProfilerCollectionResponse> responseObserver;

    @Test
    public void fileObserverRejectsUnknownTaskWithoutNpeOrUploadAck() throws IOException {
        when(taskDAO.getById("missing-task")).thenReturn(null);
        final AsyncProfilerFileCollectionObserver observer =
            new AsyncProfilerFileCollectionObserver(taskDAO, responseObserver, sourceReceiver, 1024);

        assertDoesNotThrow(() -> observer.onNext(metaData("missing-task")));

        final ArgumentCaptor<AsyncProfilerCollectionResponse> captor =
            ArgumentCaptor.forClass(AsyncProfilerCollectionResponse.class);
        verify(responseObserver).onNext(captor.capture());
        assertEquals(AsyncProfilingStatus.EXECUTION_TASK_ERROR, captor.getValue().getType());
        // Empty ACK would tell the agent to upload JFR content.
        verify(responseObserver, never()).onNext(AsyncProfilerCollectionResponse.newBuilder().build());
    }

    @Test
    public void byteBufObserverRejectsUnknownTaskWithoutAllocatingBuffer() throws IOException {
        when(taskDAO.getById("missing-task")).thenReturn(null);
        final AsyncProfilerByteBufCollectionObserver observer =
            new AsyncProfilerByteBufCollectionObserver(taskDAO, responseObserver, sourceReceiver, 1024);

        assertDoesNotThrow(() -> observer.onNext(metaData("missing-task")));

        final ArgumentCaptor<AsyncProfilerCollectionResponse> captor =
            ArgumentCaptor.forClass(AsyncProfilerCollectionResponse.class);
        verify(responseObserver).onNext(captor.capture());
        assertEquals(AsyncProfilingStatus.EXECUTION_TASK_ERROR, captor.getValue().getType());
        verify(sourceReceiver, never()).receive(any());
    }

    private static AsyncProfilerData metaData(String taskId) {
        return AsyncProfilerData.newBuilder()
            .setMetaData(AsyncProfilerMetaData.newBuilder()
                .setService("svc")
                .setServiceInstance("inst")
                .setTaskId(taskId)
                .setType(AsyncProfilingStatus.PROFILING_SUCCESS)
                .setContentSize(64)
                .build())
            .build();
    }
}
