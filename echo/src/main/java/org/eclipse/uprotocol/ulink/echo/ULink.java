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

import org.eclipse.uprotocol.transport.UTransport;
import org.eclipse.uprotocol.transport.builder.UAttributesBuilder;
import org.eclipse.uprotocol.transport.datamodel.UListener;
import org.eclipse.uprotocol.transport.datamodel.UStatus;
import org.eclipse.uprotocol.uri.validator.UriValidator;
import org.eclipse.uprotocol.v1.UAttributes;
import org.eclipse.uprotocol.v1.UPayload;
import org.eclipse.uprotocol.v1.UPriority;
import org.eclipse.uprotocol.v1.UUID;
import org.eclipse.uprotocol.v1.UEntity;
import org.eclipse.uprotocol.v1.UUri;
import org.eclipse.uprotocol.v1.UMessageType;
import org.eclipse.uprotocol.validation.ValidationResult;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;

import java.util.Map;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.eclipse.uprotocol.rpc.RpcClient;

/* Simple example code that shows an echo transport */
public class ULink implements UTransport, RpcClient {

    private final Map<UUri, UListener> mListeners = new HashMap<>();
    private final Map<UUID, CompletableFuture<UPayload>> mRequests = new ConcurrentHashMap<>();
    private Executor mExecutor;
    
    // The following fakes ingress events and is called whenever send() is called since this is
    // an echo uTransport and doesn't connect uEs together.
    private final UListener mGenericListener = new UListener() {
        @Override
        public UStatus onReceive(UUri uri, UPayload payload, UAttributes attributes) {
            
            switch(attributes.getType()) {
                // Responses requires us to complete the future for the request to notify the client
                case UMESSAGE_TYPE_RESPONSE:
                    final CompletableFuture<UPayload> responseFuture = mRequests.remove(attributes.getReqid());
                    if (responseFuture == null) {
                        UStatus.failed("Request{id='" + attributes.getReqid().toString() + "'} already completed");
                    }
        
                    break;

                default:
                    for (Map.Entry<UUri, UListener> entry : mListeners.entrySet()) {
                        if (entry.getKey().equals(uri)) {
                            mExecutor.execute(() -> {
                                entry.getValue().onReceive(uri, payload, attributes);
                            });
                        }
                    }
                    break;
            }
            return UStatus.ok();
        }
    };

    /*
     * uLink constructor that takes in an executor to run the listener callbacks on
     * 
     * @param executor - the executor to run the listener callbacks on
     */
    public ULink(Executor executor) {
        Objects.requireNonNull(executor, "Executor cannot be null");
        mExecutor = executor;
    }


    /*
     * This simple invoke method will send the request and add a completable future
     * to a list that will be completed when the response is received in the generic 
     * event handler.
     * 
     * @param uri - the uri of the request
     * @param payload - the payload of the request
     * @param attributes - the attributes of the request
     * @return CompletableFuture<UPayload> - the completed future with the response
     */
    @Override
    public CompletableFuture<UPayload> invokeMethod(UUri uri, UPayload payload, UAttributes attributes) {
        Objects.requireNonNull(uri, "Uri cannot be null");
        Objects.requireNonNull(payload, "Payload cannot be null");
        Objects.requireNonNull(attributes, "Attributes cannot be null");

        return mRequests.compute(attributes.getId(), (requestId, currentRequest) -> {
            // This is test code so we know it is always successful :-)
            send(uri, payload, attributes);
            
            return new CompletableFuture<UPayload>()
                    .orTimeout(attributes.getTtl(), TimeUnit.MILLISECONDS)
                    .whenComplete((responseData, exception) -> {
                        mRequests.remove(requestId);
                });
       
        });
    }


    /**
     * Authenticator that always says you are authenticated, normally you need to validate the calling 
     * uE's identity matches that of entity
     * 
     * @param entity - the entity to authenticate
     */
    @Override
    public UStatus authenticate(UEntity entity) {
        return UStatus.ok();
    }


    /**
     * Register a listener that will be called when am event is reived. This method is
     * used by services to register listeners for soon as you call send()
     * 
     * @param uri - the uri to register the listener for
     * @param listener - the listener to register
     * @return UStatus - the status of the registration
     */
    @Override
    public UStatus registerListener(UUri uri, UListener listener) {
        ValidationResult result = UriValidator.validate(uri);
        if (result.isFailure()) {
            return UStatus.failed(result.getMessage());
        }

        if (mListeners.containsKey(uri)) {
            return UStatus.failed("Listener already registered for " + uri);
        }

        mListeners.put(uri, listener);

        return UStatus.ok();
    }


    /**
     * Send a message to the uri, this will call the listener that was registered for the uri
     * to perform the loopback
     */
    @Override
    public UStatus send(UUri uri, UPayload payload, UAttributes attributes) {
        ValidationResult result = UriValidator.validate(uri);
        if (result.isFailure()) {
            return UStatus.failed(result.getMessage());
        }

        // Trigger the callback to pretend that the event has looped back
        mExecutor.execute(() -> {
            mGenericListener.onReceive(uri, payload, attributes);
        });

        return UStatus.ok();
    }
    

    /**
     * Unregister the listener for the uri
     * 
     * @param uri - the uri to unregister the listener for
     * @param listener - the listener to unregister
     * @return UStatus - the status of the unregistration
     */
    @Override
    public UStatus unregisterListener(UUri uri, UListener listener) {
        ValidationResult result = UriValidator.validate(uri);
        if (result.isFailure()) {
            return UStatus.failed(result.getMessage());
        }

        if (!mListeners.containsKey(uri)) {
            return UStatus.failed("No listener found for " + uri);
        }

        mListeners.remove(uri, listener);
        return UStatus.ok();
    }
    
}
