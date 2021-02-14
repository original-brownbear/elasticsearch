/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    /**
     * Creates a listener that wraps this listener, mapping response values via the given mapping function and passing along
     * exceptions to this instance.
     *
     * Notice that it is considered a bug if the listener's onResponse or onFailure fails. onResponse failures will not call onFailure.
     *
     * If the function fails, the listener's onFailure handler will be called. The principle is that the mapped listener will handle
     * exceptions from the mapping function {@code fn} but it is the responsibility of {@code delegate} to handle its own exceptions
     * inside `onResponse` and `onFailure`.
     *
     * @param fn Function to apply to listener response
     * @param <T> Response type of the wrapped listener
     * @return a listener that maps the received response and then passes it to this instance
     */
    default <T, K extends T> ActionListener<K> map(CheckedFunction<K, Response, Exception> fn) {
        return new MappedActionListener<>(fn, this);
    }

    final class MappedActionListener<Response, MappedResponse> extends DelegatingActionListener<MappedResponse, Response> {

        private final CheckedFunction<Response, MappedResponse, Exception> fn;

        private MappedActionListener(CheckedFunction<Response, MappedResponse, Exception> fn, ActionListener<MappedResponse> delegate) {
            super(delegate);
            this.fn = fn;
        }

        @Override
        public void onResponse(Response response) {
            MappedResponse mapped;
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

        @Override
        public <T, K extends T> ActionListener<K> map(CheckedFunction<K, Response, Exception> fn) {
            return new MappedActionListener<>(t -> this.fn.apply(fn.apply(t)), delegate);
        }
    }

    /**
     * Wraps this listener and returns a new listener which executes the provided {@code runAfter}
     * callback when this is notified via either {@link #onResponse} or {@link #onFailure}.
     */
    default <T extends Response> ActionListener<T> runAfter(Runnable runAfter) {
        return new RunAfterActionListener<>(this, runAfter);
    }

    abstract class DelegatingActionListener<R, T> implements ActionListener<T> {

        protected final ActionListener<R> delegate;

        protected DelegatingActionListener(ActionListener<R> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onFailure(Exception e) {
            delegate.onFailure(e);
        }
    }

    final class RunAfterActionListener<K, T extends K> extends DelegatingActionListener<K, T> {

        private final Runnable runAfter;

        public RunAfterActionListener(ActionListener<K> delegate, Runnable runAfter) {
            super(delegate);
            this.runAfter = runAfter;
        }

        @Override
        public void onResponse(T response) {
            try {
                delegate.onResponse(response);
            } finally {
                runAfter.run();
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                super.onFailure(e);
            } finally {
                runAfter.run();
            }
        }
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
        return new ActionListener<Response>() {
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

            @Override
            public final <T> ActionListener<T> wrap(CheckedConsumer<T, ? extends Exception> onResponse) {
                return ActionListener.wrap(onResponse, onFailure);
            }
        };
    }

    default <T> ActionListener<T> wrap(CheckedConsumer<T, ? extends Exception> onResponse) {
        return new WrappingActionListener<>(this, onResponse);
    }

    final class WrappingActionListener<K, T> extends DelegatingActionListener <K, T> {

        private final CheckedConsumer<T, ? extends Exception> onResponse;

        WrappingActionListener(ActionListener<K> delegate, CheckedConsumer<T, ? extends Exception> onResponse) {
            super(delegate);
            this.onResponse = onResponse;
        }

        @Override
        public void onResponse(T t) {
            try {
                onResponse.accept(t);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public final <S> ActionListener<S> wrap(CheckedConsumer<S, ? extends Exception> onResponse) {
            return new WrappingActionListener<>(delegate, onResponse);
        }
    }

    /**
     * Creates a listener that delegates all responses it receives to another listener.
     *
     * @param delegate ActionListener to wrap and delegate any exception to
     * @param bc BiConsumer invoked with delegate listener and exception
     * @param <T> Type of the listener
     * @return Delegating listener
     */
    static <T> ActionListener<T> delegateResponse(ActionListener<T> delegate, BiConsumer<ActionListener<? super T>, Exception> bc) {
        return new DelegatingActionListener<>(delegate) {
            @Override
            public void onResponse(T t) {
                delegate.onResponse(t);
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
    static <T, R> ActionListener<T> delegateFailure(ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
        return new DelegatingActionListener<>(delegate) {
            @Override
            public void onResponse(T r) {
                bc.accept(delegate, r);
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
     * Wraps a given listener and returns a new listener which executes the provided {@code runBefore}
     * callback before the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     * If the callback throws an exception then it will be passed to the listener's {@code #onFailure} and its {@code #onResponse} will
     * not be executed.
     */
    default <T extends Response> ActionListener<T> runBefore(CheckedRunnable<?> runBefore) {
        return new RunBeforeActionListener<>(this, runBefore);
    }

    final class RunBeforeActionListener<K, T extends K> extends DelegatingActionListener<K, T> {

        private final CheckedRunnable<?> runBefore;

        public RunBeforeActionListener(ActionListener<K> delegate, CheckedRunnable<?> runBefore) {
            super(delegate);
            this.runBefore = runBefore;
        }

        @Override
        public void onResponse(T response) {
            try {
                runBefore.run();
            } catch (Exception ex) {
                delegate.onFailure(ex);
                return;
            }
            delegate.onResponse(response);
        }

        @Override
        public void onFailure(Exception e) {
            try {
                runBefore.run();
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            super.onFailure(e);
        }
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
}
