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
 */

package com.alopeyk.nifi.processors.kafka.pubsub;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.nifi.logging.ComponentLog;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class PublisherPool implements Closeable {
    private final ComponentLog logger;
    private final BlockingQueue<com.alopeyk.nifi.processors.kafka.pubsub.PublisherLease> publisherQueue;
    private final Map<String, Object> kafkaProperties;
    private final int maxMessageSize;
    private final long maxAckWaitMillis;
    private final boolean useTransactions;
    private final Pattern attributeNameRegex;
    private final Charset headerCharacterSet;
    private Supplier<String> transactionalIdSupplier;

    private volatile boolean closed = false;

    PublisherPool(final Map<String, Object> kafkaProperties, final ComponentLog logger, final int maxMessageSize, final long maxAckWaitMillis,
                  final boolean useTransactions, final Supplier<String> transactionalIdSupplier, final Pattern attributeNameRegex, final Charset headerCharacterSet) {
        this.logger = logger;
        this.publisherQueue = new LinkedBlockingQueue<>();
        this.kafkaProperties = kafkaProperties;
        this.maxMessageSize = maxMessageSize;
        this.maxAckWaitMillis = maxAckWaitMillis;
        this.useTransactions = useTransactions;
        this.attributeNameRegex = attributeNameRegex;
        this.headerCharacterSet = headerCharacterSet;
        this.transactionalIdSupplier = transactionalIdSupplier;
    }

    public com.alopeyk.nifi.processors.kafka.pubsub.PublisherLease obtainPublisher() {
        if (isClosed()) {
            throw new IllegalStateException("Connection Pool is closed");
        }

        com.alopeyk.nifi.processors.kafka.pubsub.PublisherLease lease = publisherQueue.poll();
        if (lease != null) {
            return lease;
        }

        lease = createLease();
        return lease;
    }

    private com.alopeyk.nifi.processors.kafka.pubsub.PublisherLease createLease() {
        final Map<String, Object> properties = new HashMap<>(kafkaProperties);
        if (useTransactions) {
            properties.put("transactional.id", transactionalIdSupplier.get());
        }

        final Producer<byte[], byte[]> producer = new KafkaProducer<>(properties);
        final com.alopeyk.nifi.processors.kafka.pubsub.PublisherLease lease = new com.alopeyk.nifi.processors.kafka.pubsub.PublisherLease(producer, maxMessageSize, maxAckWaitMillis, logger, useTransactions, attributeNameRegex, headerCharacterSet) {
            @Override
            public void close() {
                if (isPoisoned() || isClosed()) {
                    super.close();
                } else {
                    publisherQueue.offer(this);
                }
            }
        };

        return lease;
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized void close() {
        closed = true;

        com.alopeyk.nifi.processors.kafka.pubsub.PublisherLease lease;
        while ((lease = publisherQueue.poll()) != null) {
            lease.close();
        }
    }

    /**
     * Returns the number of leases that are currently available
     *
     * @return the number of leases currently available
     */
    protected int available() {
        return publisherQueue.size();
    }
}
