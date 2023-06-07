/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/** Utility methods to work with {@link Releasable}s. */
public enum Releasables {
    ;

    /** Release the provided {@link Releasable}s. */
    public static void close(Iterable<? extends Releasable> releasables) {
        // this does the right thing with respect to add suppressed and not wrapping errors etc.
        Exception firstException = null;
        for (final Releasable object : releasables) {
            try {
                close(object);
            } catch (final RuntimeException e) {
                firstException = IOUtils.addOrSuppress(firstException, e);
            }
        }

        if (firstException != null) {
            throw (RuntimeException) firstException;
        }
    }

    /** Release the provided {@link Releasable}. */
    public static void close(@Nullable Releasable releasable) {
        if (releasable != null) {
            releasable.close();
        }
    }

    /** Release the provided {@link Releasable}s. */
    public static void close(Releasable... releasables) {
        Exception firstException = null;
        for (final Releasable object : releasables) {
            try {
                close(object);
            } catch (final RuntimeException e) {
                firstException = IOUtils.addOrSuppress(firstException, e);
            }
        }

        if (firstException != null) {
            throw (RuntimeException) firstException;
        }
    }

    /** Release the provided {@link Releasable}s expecting no exception to by thrown by any of them. */
    public static void closeExpectNoException(Releasable... releasables) {
        try {
            close(releasables);
        } catch (RuntimeException e) {
            assert false : e;
            throw e;
        }
    }

    /** Release the provided {@link Releasable} expecting no exception to by thrown. */
    public static void closeExpectNoException(Releasable releasable) {
        try {
            close(releasable);
        } catch (RuntimeException e) {
            assert false : e;
            throw e;
        }
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions. */
    public static void closeWhileHandlingException(Releasable... releasables) {
        try {
            // this does the right thing with respect to add suppressed and not wrapping errors etc.
            close(releasables);
        } catch (RuntimeException ignored) {}
    }

    /** Wrap several releasables into a single one. This is typically useful for use with try-with-resources: for example let's assume
     *  that you store in a list several resources that you would like to see released after execution of the try block:
     *
     *  <pre>
     *  List&lt;Releasable&gt; resources = ...;
     *  try (Releasable releasable = Releasables.wrap(resources)) {
     *      // do something
     *  }
     *  // the resources will be released when reaching here
     *  </pre>
     */
    public static Releasable wrap(final Iterable<Releasable> releasables) {
        return new Releasable() {
            @Override
            public void close() {
                Releasables.close(releasables);
            }

            @Override
            public String toString() {
                return "wrapped[" + releasables + "]";
            }
        };
    }

    /** @see #wrap(Iterable) */
    public static Releasable wrap(final Releasable... releasables) {
        return new Releasable() {
            @Override
            public void close() {
                Releasables.close(releasables);
            }

            @Override
            public String toString() {
                return "wrapped" + Arrays.toString(releasables);
            }
        };
    }

    /**
     * Wraps a {@link Releasable} such that its {@link Releasable#close()} method can be called multiple times without double-releasing.
     */
    public static Releasable releaseOnce(final Releasable releasable) {
        return new ReleaseOnce(releasable);
    }

    public static Releasable assertOnce(final Releasable delegate) {
        if (Assertions.ENABLED) {
            return new Releasable() {
                // if complete, records the stack trace which first completed it
                private final AtomicReference<Exception> firstCompletion = new AtomicReference<>();

                private void assertFirstRun() {
                    var previousRun = firstCompletion.compareAndExchange(null, new Exception(delegate.toString()));
                    assert previousRun == null : previousRun; // reports the stack traces of both completions
                }

                @Override
                public void close() {
                    assertFirstRun();
                    delegate.close();
                }

                @Override
                public String toString() {
                    return delegate.toString();
                }

                @Override
                public int hashCode() {
                    // It's legitimate to wrap the delegate twice, with two different assertOnce calls, which would yield different objects
                    // if and only if assertions are enabled. So we'd better not ever use these things as map keys etc.
                    throw new AssertionError("almost certainly a mistake to need the hashCode() of a one-shot Releasable");
                }

                @Override
                public boolean equals(Object obj) {
                    // It's legitimate to wrap the delegate twice, with two different assertOnce calls, which would yield different objects
                    // if and only if assertions are enabled. So we'd better not ever use these things as map keys etc.
                    throw new AssertionError("almost certainly a mistake to compare a one-shot Releasable for equality");
                }
            };
        } else {
            return delegate;
        }
    }

    private static class ReleaseOnce extends AtomicReference<Releasable> implements Releasable {
        ReleaseOnce(Releasable releasable) {
            super(releasable);
        }

        @Override
        public void close() {
            final var acquired = getAndSet(null);
            if (acquired != null) {
                acquired.close();
            }
        }

        @Override
        public String toString() {
            return "releaseOnce[" + get() + "]";
        }
    }
}
