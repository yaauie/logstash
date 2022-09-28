/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.logstash.instrument.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static org.logstash.instrument.metrics.FlowMetricRetentionPolicy.BuiltInRetentionPolicy;

/**
 * The {@link ExtendedFlowMetric} is an implementation of {@link FlowMetric} that produces
 * a variable number of rates defined by {@link FlowMetricRetentionPolicy}-s, retaining no
 * more {@link FlowCapture}s than necessary to satisfy the requested resolution or the
 * requested retention. This implementation is lock-free and concurrency-safe.
 */
public class ExtendedFlowMetric extends BaseFlowMetric {
    static final Logger LOGGER = LogManager.getLogger(ExtendedFlowMetric.class);
    static final String LIFETIME_KEY = "lifetime";

    private final List<RetentionWindow> retentionWindows;

    public ExtendedFlowMetric(final String name,
                              final Metric<? extends Number> numeratorMetric,
                              final Metric<? extends Number> denominatorMetric) {
        this(System::nanoTime, name, numeratorMetric, denominatorMetric);
    }

    ExtendedFlowMetric(final LongSupplier nanoTimeSupplier,
                       final String name,
                       final Metric<? extends Number> numeratorMetric,
                       final Metric<? extends Number> denominatorMetric,
                       final Collection<? extends FlowMetricRetentionPolicy> retentionPolicies) {
        super(nanoTimeSupplier, name, numeratorMetric, denominatorMetric);

        retentionWindows = retentionPolicies.stream()
                                            .map(this::retentionWindowFromRetentionPolicy)
                                            .collect(Collectors.toUnmodifiableList());
    }

    public ExtendedFlowMetric(final LongSupplier nanoTimeSupplier,
                              final String name,
                              final Metric<? extends Number> numeratorMetric,
                              final Metric<? extends Number> denominatorMetric) {
        this(nanoTimeSupplier, name, numeratorMetric, denominatorMetric, EnumSet.allOf(BuiltInRetentionPolicy.class));
    }

    @Override
    public void capture() {
        final FlowCapture candidateCapture = doCapture();
        this.retentionWindows.forEach(window -> window.append(candidateCapture));
    }

    @Override
    public Map<String, Double> getValue() {
        final FlowCapture currentCapture = doCapture();

        final Map<String, Double> rates = new LinkedHashMap<>();

        this.retentionWindows.forEach(window -> window.baseline(currentCapture.nanoTime())
                             .map((b) -> calculateRate(currentCapture, b))
                             .orElseGet(OptionalDouble::empty)
                             .ifPresent((rate) -> rates.put(window.policy.policyName(), rate)));

        calculateRate(currentCapture, lifetimeBaseline).ifPresent((rate) -> rates.put(LIFETIME_KEY, rate));

        return Collections.unmodifiableMap(rates);
    }

    /**
     * Internal tooling to select the younger of two captures
     */
    private static FlowCapture selectNewerCapture(final FlowCapture existing, final FlowCapture proposed) {
        if (existing == null) { return proposed; }
        if (proposed == null) { return existing; }

        return (existing.nanoTime() > proposed.nanoTime()) ? existing : proposed;
    }

    /**
     * Internal introspection for tests to measure how many captures we are retaining.
     * @return the number of captures in all tracked windows
     */
    int estimateCapturesRetained() {
        return this.retentionWindows.stream()
                                    .map(RetentionWindow::estimateSize)
                                    .mapToInt(Integer::intValue)
                                    .sum();
    }

    /**
     * Internal introspection for tests to measure excess retention.
     * @param retentionWindowFunction given a policy, return a retention window, in nanos
     * @return the sum of over-retention durations.
     */
    Duration estimateExcessRetained(final ToLongFunction<FlowMetricRetentionPolicy> retentionWindowFunction) {
        final long currentNanoTime = nanoTimeSupplier.getAsLong();
        final long cumulativeExcessRetained = this.retentionWindows.stream()
                .map(s -> s.excessRetained(currentNanoTime, retentionWindowFunction))
                .mapToLong(Long::longValue)
                .sum();
        return Duration.ofNanos(cumulativeExcessRetained);
    }

    private RetentionWindow retentionWindowFromRetentionPolicy(final FlowMetricRetentionPolicy retentionPolicy) {
        final RetentionWindow retentionWindow = new RetentionWindow(retentionPolicy, this.lifetimeBaseline);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} created {}", this.name, retentionWindow);
        }
        return retentionWindow;
    }

    /**
     * A {@link RetentionWindow} efficiently holds sufficient {@link FlowCapture}s to
     * meet its {@link FlowMetricRetentionPolicy}, providing access to the youngest capture
     * that is older than the policy's allowed retention (if any).
     * The implementation is similar to a singly-linked list whose youngest captures are at
     * the tail and oldest captures are at the head, with an additional pre-tail stag.
     * Compaction is always done at read-time and occasionally at write-time.
     * Both reads and writes are concurrency-safe.
     */
    private static class RetentionWindow {
        private final AtomicReference<FlowCapture> stagedCapture = new AtomicReference<>();
        private final AtomicReference<Node> tail;
        private final AtomicReference<Node> head;
        private final FlowMetricRetentionPolicy policy;

        RetentionWindow(final FlowMetricRetentionPolicy policy, final FlowCapture zeroCapture) {
            this.policy = policy;
            final Node zeroNode = new Node(zeroCapture);
            this.head = new AtomicReference<>(zeroNode);
            this.tail = new AtomicReference<>(zeroNode);
        }

        /**
         * Append the newest {@link FlowCapture} into this {@link RetentionWindow},
         * while respecting our {@link FlowMetricRetentionPolicy}.
         * We tolerate minor jitter in the provided {@link FlowCapture#nanoTime()}, but
         * expect callers of this method to minimize lag between instantiating the capture
         * and appending it.
         *
         * @param newestCapture the newest capture to stage
         */
        private void append(final FlowCapture newestCapture) {
            final Node casTail = this.tail.get(); // for CAS
            final long newestCaptureNanoTime = newestCapture.nanoTime();

            // stage our newest capture unless it is older than the currently-staged capture
            final FlowCapture previouslyStaged = stagedCapture.getAndAccumulate(newestCapture, ExtendedFlowMetric::selectNewerCapture);

            // promote our previously-staged capture IFF our newest capture is too far
            // ahead of the current tail to support policy's resolution.
            if (previouslyStaged != null && Math.subtractExact(newestCaptureNanoTime, casTail.captureNanoTime()) > policy.resolutionNanos()) {
                // attempt to set an _unlinked_ Node to our tail
                final Node proposedNode = new Node(previouslyStaged);
                if (this.tail.compareAndSet(casTail, proposedNode)) {
                    // if we succeeded at setting an unlinked node, link to it from our old tail
                    casTail.setNext(proposedNode);

                    // perform a force-compaction of our head if necessary,
                    // detected using plain memory access
                    final Node currentHead = head.getPlain();
                    final long headAgeNanos = Math.subtractExact(newestCaptureNanoTime, currentHead.captureNanoTime());
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("{} post-append result (captures: `{}` span: `{}` }", this, estimateSize(currentHead), Duration.ofNanos(headAgeNanos));
                    }
                    if (headAgeNanos > policy.forceCompactionNanos()) {
                        final Node compactHead = compactHead(Math.subtractExact(newestCaptureNanoTime, policy.retentionNanos()));
                        if (LOGGER.isDebugEnabled()) {
                            final long compactHeadAgeNanos = Math.subtractExact(newestCaptureNanoTime, compactHead.captureNanoTime());
                            LOGGER.debug("{} forced-compaction result (captures: `{}` span: `{}`)", this, estimateSize(compactHead), Duration.ofNanos(compactHeadAgeNanos));
                        }
                    }
                }
            }
        }

        @Override
        public String toString() {
            return "RetentionWindow{" +
                    "policy=" + policy.policyName() +
                    " id=" + System.identityHashCode(this) +
                    '}';
        }

        /**
         * @param nanoTime the nanoTime of the capture for which we are retrieving a baseline.
         * @return an {@link Optional} that contains the youngest {@link FlowCapture} that is older
         *         than this window's {@link FlowMetricRetentionPolicy} allowed retention if one
         *         exists, and is otherwise empty.
         */
        public Optional<FlowCapture> baseline(final long nanoTime) {
            final long barrier = Math.subtractExact(nanoTime, policy.retentionNanos());
            final Node head = compactHead(barrier);
            if (head.captureNanoTime() <= barrier) {
                return Optional.of(head.capture);
            } else {
                return Optional.empty();
            }
        }

        /**
         * @return a computationally-expensive estimate of the number of captures in this window,
         *         using plain memory access. This should NOT be run in unguarded production code.
         */
        private static int estimateSize(final Node headNode) {
            int i = 1; // assume we have one additional staged
            // NOTE: we chase the provided headNode's tail with plain-gets,
            //       which tolerates missed appends from other threads.
            for (Node current = headNode; current != null; current = current.getNextPlain()) { i++; }
            return i;
        }

        /**
         * @see RetentionWindow#estimateSize(Node)
         */
        private int estimateSize() {
            return estimateSize(this.head.getPlain());
        }

        /**
         * @param barrier a nanotime that will NOT be crossed during compaction
         * @return the head node after compaction up to the provided barrier.
         */
        private Node compactHead(final long barrier) {
            return this.head.updateAndGet((existingHead) -> {
                final Node proposedHead = existingHead.seekWithoutCrossing(barrier);
                return Objects.requireNonNullElse(proposedHead, existingHead);
            });
        }

        /**
         * Internal testing support
         */
        private long excessRetained(final long currentNanoTime, final ToLongFunction<FlowMetricRetentionPolicy> retentionWindowFunction) {
            final long barrier = Math.subtractExact(currentNanoTime, retentionWindowFunction.applyAsLong(this.policy));
            return Math.max(0L, Math.subtractExact(barrier, this.head.getPlain().captureNanoTime()));
        }

        /**
         * A {@link Node} holds a single {@link FlowCapture} and
         * may link ahead to the next {@link Node}.
         * It is an implementation detail of {@link RetentionWindow}.
         */
        private static class Node {
            private static final VarHandle NEXT;
            static {
                try {
                    MethodHandles.Lookup l = MethodHandles.lookup();
                    NEXT = l.findVarHandle(Node.class, "next", Node.class);
                } catch (ReflectiveOperationException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }

            private final FlowCapture capture;
            private volatile Node next;

            Node(final FlowCapture capture) {
                this.capture = capture;
            }

            Node seekWithoutCrossing(final long barrier) {
                Node newestOlderThanThreshold = null;
                Node candidate = this;

                while(candidate != null && candidate.captureNanoTime() < barrier) {
                    newestOlderThanThreshold = candidate;
                    candidate = candidate.getNext();
                }
                return newestOlderThanThreshold;
            }

            long captureNanoTime() {
                return this.capture.nanoTime();
            }

            void setNext(final Node nextNode) {
                next = nextNode;
            }

            Node getNext() {
                return next;
            }

            Node getNextPlain() {
                return (Node)NEXT.get(this);
            }
        }
    }
}
