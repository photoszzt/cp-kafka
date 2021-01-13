/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.utils;

import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public interface EventQueue extends AutoCloseable {
    interface Event {
        void run() throws Exception;
        default void handleException(Throwable e) {}
    }

    abstract class FailureLoggingEvent implements Event {
        private final Logger log;

        public FailureLoggingEvent(Logger log) {
            this.log = log;
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Unexpected error handling {}", this.getClass().getSimpleName(), e);
        }
    }

    class DeadlineFunction implements Function<Long, Long> {
        private final long deadlineNs;

        public DeadlineFunction(long deadlineNs) {
            this.deadlineNs = deadlineNs;
        }

        @Override
        public Long apply(Long t) {
            return deadlineNs;
        }
    }

    class VoidEvent implements Event {
        public final static VoidEvent INSTANCE = new VoidEvent();

        @Override
        public void run() throws Exception {
        }
    }

    /**
     * Add an element to the front of the queue.
     *
     * @param event             The mandatory event to prepend.
     */
    default void prepend(Event event) {
        enqueue(EventInsertionType.PREPEND, null, null, event);
    }

    /**
     * Add an element to the end of the queue.
     *
     * @param event             The event to append.
     */
    default void append(Event event) {
        enqueue(EventInsertionType.APPEND, null, null, event);
    }

    /**
     * Enqueue an event to be run in FIFO order.
     *
     * @param deadlineNs        The time in monotonic nanoseconds after which the future
     *                          is completed with a
     *                          @{org.apache.kafka.common.errors.TimeoutException},
     *                          and the event is cancelled.
     * @param event             The event to append.
     */
    default void appendWithDeadline(long deadlineNs, Event event) {
        enqueue(EventInsertionType.APPEND, null, __ -> deadlineNs, event);
    }

    /**
     * Schedule an event to be run at a specific time.
     *
     * @param tag                   If this is non-null, the unique tag to use for this
     *                              event.  If an event with this tag already exists, it
     *                              will be cancelled.
     * @param deadlineNsCalculator  A function which takes as an argument the existing
     *                              deadline for the event with this tag (or null if the
     *                              event has no tag, or if there is none such), and
     *                              produces the deadline to use for this event.
     * @param event                 The event to schedule.
     */
    default void scheduleDeferred(String tag,
                                  Function<Long, Long> deadlineNsCalculator,
                                  Event event) {
        enqueue(EventInsertionType.DEFERRED, tag, deadlineNsCalculator, event);
    }

    enum EventInsertionType {
        PREPEND,
        APPEND,
        DEFERRED;
    }

    /**
     * Enqueue an event to be run in FIFO order.
     *
     * @param insertionType         How to insert the event.
     *                              PREPEND means insert the event as the first thing
     *                              to run.  APPEND means insert the event as the last
     *                              thing to run.  DEFERRED means insert the event to
     *                              run after a delay.
     * @param tag                   If this is non-null, the unique tag to use for
     *                              this event.  If an event with this tag already
     *                              exists, it will be cancelled.
     * @param deadlineNsCalculator  If this is non-null, it is a function which takes
     *                              as an argument the existing deadline for the
     *                              event with this tag (or null if the event has no
     *                              tag, or if there is none such), and produces the
     *                              deadline to use for this event (or null to use
     *                              none.)
     * @param event                 The event to enqueue.
     */
    void enqueue(EventInsertionType insertionType,
                 String tag,
                 Function<Long, Long> deadlineNsCalculator,
                 Event event);

    /**
     * Asynchronously shut down the event queue with no unnecessary delay.
     * @see #beginShutdown(String, Event, TimeUnit, long)
     *
     * @param source                The source of the shutdown.
     */
    default void beginShutdown(String source) {
        beginShutdown(source, new VoidEvent());
    }

    /**
     * Asynchronously shut down the event queue with no unnecessary delay.
     *
     * @param source        The source of the shutdown.
     * @param cleanupEvent  The mandatory event to invoke after all other events have
     *                      been processed.
     * @see #beginShutdown(String, Event, TimeUnit, long)
     */
    default void beginShutdown(String source, Event cleanupEvent) {
        beginShutdown(source, cleanupEvent, TimeUnit.SECONDS, 0);
    }

    /**
     * Asynchronously shut down the event queue.
     *
     * No new events will be accepted, and the timeout will be initiated
     * for all existing events.
     *
     * @param source        The source of the shutdown.
     * @param cleanupEvent  The mandatory event to invoke after all other events have
     *                      been processed.
     * @param timeUnit      The time unit to use for the timeout.
     * @param timeSpan      The amount of time to use for the timeout.
     *                      Once the timeout elapses, any remaining queued
     *                      events will get a
     *                      @{org.apache.kafka.common.errors.TimeoutException}.
     */
    void beginShutdown(String source, Event cleanupEvent, TimeUnit timeUnit, long timeSpan);

    /**
     * This method is used during unit tests where MockTime is in use.
     * It is used to alert the queue that the mock time has changed.
     */
    default void wakeup() { }

    /**
     * Synchronously close the event queue and wait for any threads to be joined.
     */
    void close() throws InterruptedException;
}
