/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.CheckedSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A listener for action responses or failures.
 */
public interface ActionListener<Response> {
    /**
     * Handle action response. This response may constitute a failure or a
     * success but it is up to the listener to make that decision.
     */
    void onResponse(Response response);

    /**
     * A failure caused by an exception at some phase of the task.
     */
    void onFailure(Exception e);

    default <T> ActionListener<T> map(CheckedFunction<T, Response, Exception> fn) {
        return ActionListener.map(this, fn);
    }

    default <T> ActionListener<T> delegateFailure(BiConsumer<ActionListener<Response>, T> bc) {
        return delegateFailure(this, bc);
    }

    default ActionListener<Response> delegateResponse(BiConsumer<ActionListener<Response>, Exception> bc) {
        return delegateResponse(this, bc);
    }

    /**
     * Wraps a given listener and returns a new listener which executes the provided {@code runAfter}
     * callback when the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     */
    default ActionListener<Response> runAfter(Runnable runAfter) {
        final ActionListener<Response> self = this;
        return new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                try {
                    self.onResponse(response);
                } finally {
                    runAfter.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    self.onFailure(e);
                } finally {
                    runAfter.run();
                }
            }
        };
    }

    /**
     * Wraps a given listener and returns a new listener which executes the provided {@code runBefore}
     * callback before the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     * If the callback throws an exception then it will be passed to the listener's {@code #onFailure} and its {@code #onResponse} will
     * not be executed.
     */
    default ActionListener<Response> runBefore(CheckedRunnable<?> runBefore) {
        return new RunBeforeListener<>(runBefore, this);
    }

    /**
     * Creates a listener that listens for a response (or failure) and executes the
     * corresponding consumer when the response (or failure) is received.
     *
     * @param onResponse the checked consumer of the response, when the listener receives one
     * @param onFailure the consumer of the failure, when the listener receives one
     * @param <Response> the type of the response
     * @return a listener that listens for responses and invokes the consumer when received
     */
    static <Response> ActionListener<Response> wrap(CheckedConsumer<Response, ? extends Exception> onResponse,
            Consumer<Exception> onFailure) {
        return new WrappedActionListener<>(onResponse, onFailure);
    }

    /**
     * Creates a listener that delegates all responses it receives to another listener.
     *
     * @param delegate ActionListener to wrap and delegate any exception to
     * @param bc BiConsumer invoked with delegate listener and exception
     * @param <T> Type of the listener
     * @return Delegating listener
     */
    private static <T> ActionListener<T> delegateResponse(ActionListener<T> delegate, BiConsumer<ActionListener<T>, Exception> bc) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T r) {
                delegate.onResponse(r);
            }

            @Override
            public void onFailure(Exception e) {
                bc.accept(delegate, e);
            }
        };
    }

    /**
     * Creates a listener that delegates all exceptions it receives to another listener.
     *
     * @param delegate ActionListener to wrap and delegate any exception to
     * @param bc BiConsumer invoked with delegate listener and response
     * @param <T> Type of the delegating listener's response
     * @param <R> Type of the wrapped listeners
     * @return Delegating listener
     */
    private static <T, R> ActionListener<T> delegateFailure(ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
        return new ActionListener<T>() {

            @Override
            public void onResponse(T r) {
                bc.accept(delegate, r);
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }

    /**
     * Creates a listener that listens for a response (or failure) and executes the
     * corresponding runnable when the response (or failure) is received.
     *
     * @param runnable the runnable that will be called in event of success or failure
     * @param <Response> the type of the response
     * @return a listener that listens for responses and invokes the runnable when received
     */
    static <Response> ActionListener<Response> wrap(Runnable runnable) {
        return wrap(r -> runnable.run(), e -> runnable.run());
    }

    /**
     * Creates a listener that wraps another listener, mapping response values via the given mapping function and passing along
     * exceptions to the delegate.
     *
     * Notice that it is considered a bug if the listener's onResponse or onFailure fails. onResponse failures will not call onFailure.
     *
     * If the function fails, the listener's onFailure handler will be called. The principle is that the mapped listener will handle
     * exceptions from the mapping function {@code fn} but it is the responsibility of {@code delegate} to handle its own exceptions
     * inside `onResponse` and `onFailure`.
     *
     * @param delegate Listener to delegate to
     * @param fn Function to apply to listener response
     * @param <Response> Response type of the new listener
     * @param <T> Response type of the wrapped listener
     * @return a listener that maps the received response and then passes it to its delegate listener
     */
    static <T, Response> ActionListener<Response> map(ActionListener<T> delegate, CheckedFunction<Response, T, Exception> fn) {
        if (delegate instanceof MappedActionListener) {
            return new MappedActionListener<>(fn.andThen(((MappedActionListener<T, T>) delegate).fn),
                    ((MappedActionListener<?, T>) delegate).delegate);
        }
        return new MappedActionListener<>(fn, delegate);
    }

    /**
     * Converts a listener to a {@link BiConsumer} for compatibility with the {@link java.util.concurrent.CompletableFuture}
     * api.
     *
     * @param listener that will be wrapped
     * @param <Response> the type of the response
     * @return a bi consumer that will complete the wrapped listener
     */
    static <Response> BiConsumer<Response, Exception> toBiConsumer(ActionListener<Response> listener) {
        return (response, throwable) -> {
            if (throwable == null) {
                listener.onResponse(response);
            } else {
                listener.onFailure(throwable);
            }
        };
    }

    /**
     * Notifies every given listener with the response passed to {@link #onResponse(Object)}. If a listener itself throws an exception
     * the exception is forwarded to {@link #onFailure(Exception)}. If in turn {@link #onFailure(Exception)} fails all remaining
     * listeners will be processed and the caught exception will be re-thrown.
     */
    static <Response> void onResponse(Iterable<ActionListener<Response>> listeners, Response response) {
        List<Exception> exceptionList = new ArrayList<>();
        for (ActionListener<Response> listener : listeners) {
            try {
                listener.onResponse(response);
            } catch (Exception ex) {
                try {
                    listener.onFailure(ex);
                } catch (Exception ex1) {
                    exceptionList.add(ex1);
                }
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptionList);
    }

    /**
     * Notifies every given listener with the failure passed to {@link #onFailure(Exception)}. If a listener itself throws an exception
     * all remaining listeners will be processed and the caught exception will be re-thrown.
     */
    static <Response> void onFailure(Iterable<ActionListener<Response>> listeners, Exception failure) {
        List<Exception> exceptionList = new ArrayList<>();
        for (ActionListener<Response> listener : listeners) {
            try {
                listener.onFailure(failure);
            } catch (Exception ex) {
                exceptionList.add(ex);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptionList);
    }

    /**
     * Wraps a given listener and returns a new listener which makes sure {@link #onResponse(Object)}
     * and {@link #onFailure(Exception)} of the provided listener will be called at most once.
     */
    static <Response> ActionListener<Response> notifyOnce(ActionListener<Response> delegate) {
        return new NotifyOnceListener<Response>() {
            @Override
            protected void innerOnResponse(Response response) {
                delegate.onResponse(response);
            }

            @Override
            protected void innerOnFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }

    /**
     * Completes the given listener with the result from the provided supplier accordingly.
     * This method is mainly used to complete a listener with a block of synchronous code.
     *
     * If the supplier fails, the listener's onFailure handler will be called.
     * It is the responsibility of {@code delegate} to handle its own exceptions inside `onResponse` and `onFailure`.
     */
    static <Response> void completeWith(ActionListener<Response> listener, CheckedSupplier<Response, ? extends Exception> supplier) {
        Response response;
        try {
            response = supplier.get();
        } catch (Exception e) {
            try {
                listener.onFailure(e);
            } catch (RuntimeException ex) {
                assert false : ex;
                throw ex;
            }
            return;
        }
        try {
            listener.onResponse(response);
        } catch (RuntimeException ex) {
            assert false : ex;
            throw ex;
        }
    }

    final class WrappedActionListener<Response> implements ActionListener<Response> {
        private final CheckedConsumer<Response, ? extends Exception> onResponse;
        private final Consumer<Exception> onFailure;

        public WrappedActionListener(CheckedConsumer<Response, ? extends Exception> onResponse, Consumer<Exception> onFailure) {
            this.onResponse = onResponse;
            this.onFailure = onFailure;
        }

        @Override
        public void onResponse(Response response) {
            try {
                onResponse.accept(response);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            onFailure.accept(e);
        }
    }

    final class MappedActionListener<Response, O> implements ActionListener<Response> {

        private final CheckedFunction<Response, O, Exception> fn;

        private final ActionListener<O> delegate;

        public MappedActionListener(CheckedFunction<Response, O, Exception> fn, ActionListener<O> delegate) {
            this.fn = fn;
            this.delegate = delegate;
        }

        @Override
        public void onResponse(Response response) {
            O mapped;
            try {
                mapped = fn.apply(response);
            } catch (Exception e) {
                onFailure(e);
                return;
            }
            try {
                delegate.onResponse(mapped);
            } catch (RuntimeException e) {
                assert false : new AssertionError("map: listener.onResponse failed", e);
                throw e;
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                delegate.onFailure(e);
            } catch (RuntimeException ex) {
                if (ex != e) {
                    ex.addSuppressed(e);
                }
                assert false : new AssertionError("map: listener.onFailure failed", ex);
                throw ex;
            }
        }
    }

    class RunBeforeListener<T> implements ActionListener<T> {
        private final CheckedRunnable<?> runBefore;
        private final ActionListener<T> self;

        public RunBeforeListener(CheckedRunnable<?> runBefore, ActionListener<T> self) {
            this.runBefore = runBefore;
            this.self = self;
        }

        @Override
        public void onResponse(T response) {
            try {
                runBefore.run();
            } catch (Exception ex) {
                this.onFailure(ex);
                return;
            }
            self.onResponse(response);
        }

        @Override
        public void onFailure(Exception e) {
            try {
                runBefore.run();
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            self.onFailure(e);
        }
    }
}
