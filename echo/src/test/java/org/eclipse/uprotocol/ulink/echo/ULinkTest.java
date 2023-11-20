/*
 * Copyright (c) 2023 General Motors GTO LLC
 *
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
 * 
 * SPDX-FileType: SOURCE
 * SPDX-FileCopyrightText: 2023 General Motors GTO LLC
 * SPDX-License-Identifier: Apache-2.0
 */
package org.eclipse.uprotocol.ulink.echo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Executor;

import org.eclipse.uprotocol.uri.builder.UResourceBuilder;
import org.eclipse.uprotocol.rpc.RpcMapper;
import org.eclipse.uprotocol.rpc.RpcResult;
import org.eclipse.uprotocol.transport.builder.UAttributesBuilder;
import org.eclipse.uprotocol.transport.datamodel.UListener;
import org.eclipse.uprotocol.transport.datamodel.UStatus;
import org.eclipse.uprotocol.v1.UAttributes;
import org.eclipse.uprotocol.v1.UEntity;
import org.eclipse.uprotocol.v1.UPayload;
import org.eclipse.uprotocol.v1.UPayloadFormat;
import org.eclipse.uprotocol.v1.UPriority;
import org.eclipse.uprotocol.v1.UResource;
import org.eclipse.uprotocol.v1.UUri;
import org.junit.Test;

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;

import java.util.concurrent.CompletableFuture;

/**
 * Unit test for simple App.
 */
public class ULinkTest {
    
    private Any mData = Any.pack(Int32Value.of(3));

    final UPayload mPayload = UPayload.newBuilder()
            .setValue(mData.toByteString())
            .setFormat(UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF)
            .build();


    /**
     * Example sending a message and having it echo back
     */
    @Test
    public void testPublishAndReceive() {
        

        final UUri mTopic = UUri.newBuilder()
        .setEntity(UEntity.newBuilder().setName("body.access").setVersionMajor(1))
        .setResource(UResource.newBuilder().setName("door").setInstance("front_left").setMessage("Door")).build();
        

        final class MyListener implements UListener {
            @Override
            public UStatus onReceive(UUri uri, UPayload payload, UAttributes attributes) {
                assertEquals(uri, mTopic);
                assertEquals(mPayload, payload);
                return UStatus.ok();
            }
        };
        
        ULink ulink = new ULink(new Executor() {
            @Override
            public void execute(Runnable command) {
                command.run();
            }
        });

        ulink.registerListener(mTopic, new MyListener());

        final UAttributes attributes = UAttributesBuilder.publish(UPriority.UPRIORITY_CS4).build();

        UStatus status = ulink.send(mTopic, mPayload, attributes);
        assertTrue(status.isSuccess());
    }


    /**
     * Test invoking a method and then receiving the response which was the request
     * thrown back at the client
     */
    @Test
    public void testInvokeMethod() {
        
        final UUri source = UUri.newBuilder()
        .setEntity(UEntity.newBuilder().setName("hr").setVersionMajor(1))
        .setResource(UResourceBuilder.forRpcRequest("Raise")).build();

        final UUri sink = UUri.newBuilder()
        .setEntity(UEntity.newBuilder().setName("hartley").setVersionMajor(1))
        .setResource(UResourceBuilder.forRpcResponse()).build();

        final UAttributes attributes = UAttributesBuilder.request(UPriority.UPRIORITY_CS4, sink, 1000).build();

        ULink ulink = new ULink(new Executor() {
            @Override
            public void execute(Runnable command) {
                command.run();
            }
        });

        final CompletableFuture<RpcResult<Int32Value>> rpcResponse =
                RpcMapper.mapResponseToResult(
                ulink.invokeMethod(source, mPayload, attributes), Int32Value.class);

        assertFalse(rpcResponse.isCompletedExceptionally());
        final CompletableFuture<Void> test = rpcResponse.thenAccept(RpcResult -> {
            assertTrue(RpcResult.isSuccess());

            assertEquals(Int32Value.of(3), RpcResult.successValue());
        });

        assertFalse(test.isCompletedExceptionally());
    }
}
