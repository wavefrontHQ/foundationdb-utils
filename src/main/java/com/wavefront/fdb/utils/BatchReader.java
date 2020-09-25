package com.wavefront.fdb.utils;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * FDB batch reader that allows reads (single key fetches or scans) to be performed in an
 * asynchronous fashion in batches to FDB. While there are no transactional guarantees, range scans
 * would still be consistent as a single unit of operation. The default TTL for a transaction is 1s. It has to be
 * shorter than 5s or else reads may fail with version too old errors.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class BatchReader {

  /**
   * Default TTL in milliseconds for transactions.
   */
  public static final int DEFAULT_TRANSACTION_TTL_MILLISECONDS = 1000;
  /**
   * Default number of attempts for retryable (error code >=2000 and <3000) errors.
   */
  public static final int DEFAULT_MAX_ATTEMPTS = 3;

  /**
   * Current transaction that can be used to service gets. May be null.
   */
  @Nullable
  private volatile Transaction transaction;
  /**
   * Time {@link #transaction} was created (we dispose transactions after 1 second).
   */
  private long lastTransactionCreationTime;
  /**
   * Reference counting for a transaction to determine whether to dispose a transaction or not.
   */
  private final ConcurrentMap<ReadTransaction, AtomicLong> activeTransactions = new ConcurrentHashMap<>();
  /**
   * Monitor for protecting access to {@link #transaction}
   */
  private final StampedLock lock = new StampedLock();
  /**
   * The FDB database we are working with.
   */
  private final Database database;
  /**
   * Default max attempts.
   */
  private final int defaultMaxAttempts;
  /**
   * TTL for {@link #transaction} before {@link #releaseTransaction(Transaction)}.
   */
  private final int ttlMilliseconds;
  /**
   * Interface to call in order to generate metrics for batch reader operations.
   */
  private final Metrics metrics;

  /**
   * Construct a new batch reader.
   *
   * @param database                   Database to batch range scans and gets for.
   * @param defaultMaxAttempts         Max retry attempts for gets and scans.
   * @param transactionTtlMilliseconds Transaction TTL in milliseconds.
   * @param metrics                    Metrics interface to report metrics.
   */
  public BatchReader(Database database, int defaultMaxAttempts, int transactionTtlMilliseconds, Metrics metrics) {
    if (database == null) throw new NullPointerException();
    if (defaultMaxAttempts < 1) throw new IllegalArgumentException("defaultMaxAttempts must be >= 1");
    if (transactionTtlMilliseconds < 0) throw new IllegalArgumentException("transactionTtlMilliseconds must be >= 0");
    if (metrics == null) {
      metrics = new Metrics() {
      };
    }
    this.database = database;
    this.defaultMaxAttempts = defaultMaxAttempts;
    this.ttlMilliseconds = transactionTtlMilliseconds;
    this.metrics = metrics;
  }

  /**
   * Convenience constructor to just instantiate a batchreader for the database.
   *
   * @param database Database we are working with.
   */
  public BatchReader(Database database) {
    this(database, DEFAULT_MAX_ATTEMPTS, DEFAULT_TRANSACTION_TTL_MILLISECONDS, new Metrics() {
    });
  }

  /**
   * Can we used to debug transaction leakages (or when tons of transactions are being disposed and hence many of them
   * are alive at the same time.
   *
   * @return Number of active transactions (with an outstanding scan or get against it).
   */
  public int getActiveTransactions() {
    return activeTransactions.size();
  }

  /**
   * Certain errors from FDB are deemed retryable and when not-specified explicitly, this batch reader will retry gets
   * and scans up to the value returned by this call.
   *
   * @return Maximum number of times we would retry a get/scan (after recycling the transaction).
   */
  public int getDefaultMaxAttempts() {
    return defaultMaxAttempts;
  }

  /**
   * Force a new transaction to be used from this point onwards. Useful for when read-then-get semantics are needed.
   * Calling this function unnecessarily can negate the performance improvements of doing batching.
   *
   * @return Whether a transaction was active and was deposed.
   */
  public boolean forceNewTransaction() {
    if (transaction == null) {
      return false;
    }
    long writeLock = lock.writeLock();
    try {
      if (transaction != null) {
        setTransactionToNull();
        return true;
      } else {
        return false;
      }
    } finally {
      lock.unlockWrite(writeLock);
    }
  }

  /**
   * @return The read version that forms the lower bound for all subsequent reads of this batch reader. Since we recycle
   * transactions rapidly, this call does <b>not</b> return the read version used for subsequent calls to
   * {@link #getAsync(byte[])} or {@link #getRangeAsync(Function)}. Reads would be guaranteed to be using
   * a read version at or beyond the result of this call. WARNING: calling this <b>after</b> reads (which might
   * sometimes end up with the same transaction) should not yield any meaning.
   */
  public CompletableFuture<Long> getReadVersion() {
    Transaction txn = getTransaction();
    return txn.getReadVersion().thenApply(version -> {
      releaseTransaction(txn);
      return version;
    });
  }

  /**
   * Dispose the specified transaction but only if it's the currently active one..
   *
   * @param toDispose Transaction to dispose.
   * @return Whether the transaction is the currently active one and was disposed.
   */
  private boolean disposeTransactionIfCurrent(@Nonnull Transaction toDispose) {
    if (transaction != toDispose) {
      return false;
    }
    long writeLock = lock.writeLock();
    try {
      if (transaction == toDispose) {
        setTransactionToNull();
        return true;
      } else {
        return false;
      }
    } finally {
      lock.unlockWrite(writeLock);
    }
  }

  /**
   * Can only be called when {@link #lock} is obtained.
   */
  private void setTransactionToNull() {
    Transaction toDispose = transaction;
    transaction = null;
    lastTransactionCreationTime = 0;
    final AtomicLong refCounter = activeTransactions.get(toDispose);
    if (refCounter != null && refCounter.get() == 0) {
      metrics.transactionDisposed();
      try {
        toDispose.close();
      } catch (Throwable ignored) {
      }
      activeTransactions.remove(toDispose);
    }
  }

  /**
   * Generate a transaction or return the currently active one. Each call will increment the reference for the
   * transaction by one.
   *
   * @return Transaction to use for reads.
   */
  private Transaction getTransaction() {
    long stamp = lock.readLock();
    try {
      if (transaction == null || lastTransactionCreationTime < System.currentTimeMillis() - ttlMilliseconds) {
        long ws = lock.tryConvertToWriteLock(stamp);
        if (ws != 0) {
          stamp = ws;
        } else {
          lock.unlockRead(stamp);
          stamp = lock.writeLock();
        }
        Transaction curr = transaction;
        if (curr == null || disposeTransactionIfCurrent(curr)) {
          metrics.transactionCreated();
          lastTransactionCreationTime = System.currentTimeMillis();
          // we keep the transaction and not the read transaction so that we can sloe it properly.
          transaction = database.createTransaction();
          // we would never attempt writes with this transaction.
          transaction.options().setReadYourWritesDisable();
          transaction.options().setSnapshotRywDisable();
        }
      }
      // mark the new transaction as used.
      activeTransactions.computeIfAbsent(transaction, tx -> new AtomicLong()).incrementAndGet();
      return transaction;
    } finally {
      lock.unlock(stamp);
    }
  }

  private void releaseTransaction(Transaction txn) {
    final AtomicLong refCounter = activeTransactions.get(txn);
    if (refCounter != null && refCounter.decrementAndGet() == 0) {
      // all references are cleared but we might still be the active transaction check.
      long stamp = lock.readLock();
      try {
        if (transaction != txn) {
          // we are done.
          metrics.transactionDisposed();
          try {
            txn.close();
          } catch (Throwable ignored) {
          }
          activeTransactions.remove(txn);
        }
      } finally {
        lock.unlockRead(stamp);
      }
    }
  }

  /**
   * Read a key.
   *
   * @param key Key to read.
   * @return {@link CompletableFuture} for the value of the key.
   */
  public CompletableFuture<byte[]> getAsync(final byte[] key) {
    return getAsync(key, defaultMaxAttempts);
  }

  /**
   * Read a key with a limit of maximum number of retryable attempts.
   *
   * @param key         Key to read.
   * @param maxAttempts Max number of retries if fdb returns a retryable error.
   * @return {@link CompletableFuture} for the value of the key.
   */
  private CompletableFuture<byte[]> getAsync(final byte[] key, final int maxAttempts) {
    return getAsync(key, maxAttempts, true);
  }

  /**
   * Read a key with a limit of maximum number of retryable attempts.
   *
   * @param key          Key to read.
   * @param maxAttempts  Max number of retries if fdb returns a retryable error.
   * @param retryOnNulls Retry the read with a new transaction if the read returns null (up to max attempts). This is
   *                     useful if we are expecting a key to exist and instead of recycling the transaction to ensure
   *                     that we can do write-then-read, only dispose the transaction if the key is expected to exist
   *                     but we are getting nulls. Obviously this wouldn't cover cases where the key is expected to be
   *                     updated and we want to get the latest version.
   * @return {@link CompletableFuture} for the value of the key.
   */
  public CompletableFuture<byte[]> getAsync(final byte[] key, final int maxAttempts, final boolean retryOnNulls) {
    metrics.get();
    final AtomicInteger attempts = new AtomicInteger(1);
    final CompletableFuture<byte[]> toReturn = new CompletableFuture<>();
    final AtomicReference<Transaction> currentTxn = new AtomicReference<>(getTransaction());
    final AtomicReference<CompletableFuture<byte[]>> fetchFuture = new AtomicReference<>(
        currentTxn.get().snapshot().get(key));
    final AtomicBoolean retryOnNull = new AtomicBoolean(false);
    final BiFunction<? super byte[], Throwable, ? extends byte[]> onReadyFn =
        new BiFunction<byte[], Throwable, byte[]>() {
          @Override
          public byte[] apply(byte[] result, Throwable error) {
            Transaction toDispose = currentTxn.get();
            try {
              if (toReturn.isDone()) return null;
              if (error != null) {
                if (isRetryableError(error)) {
                  int attempted = attempts.getAndIncrement();
                  if (attempted >= maxAttempts) {
                    metrics.getTimeouts();
                    toReturn.completeExceptionally(
                        new RuntimeException("too many errors (" + maxAttempts + "), " +
                            "throwing last seen error", error));
                    return null;
                  }
                  // start a new txn and try again(since it's a RetryableError)
                  disposeTransactionIfCurrent(toDispose);
                  currentTxn.set(getTransaction());
                  final CompletableFuture<byte[]> newFuture = currentTxn.get().snapshot().get(key);
                  fetchFuture.set(newFuture);
                  newFuture.handle(this);
                } else {
                  metrics.getErrors();
                  toReturn.completeExceptionally(new RuntimeException("nonRetryableError in " +
                      "getAsync() " + error));
                }
              } else {
                if (retryOnNulls && !retryOnNull.get() && result == null) {
                  // try again with a new transaction if the result is null.
                  retryOnNull.set(true);
                  disposeTransactionIfCurrent(toDispose);
                  currentTxn.set(getTransaction());
                  final CompletableFuture<byte[]> newFuture = currentTxn.get().snapshot().get(key);
                  fetchFuture.set(newFuture);
                  newFuture.handle(this);
                } else {
                  toReturn.complete(result);
                }
              }
            } catch (Throwable t) {
              try {
                toReturn.completeExceptionally(
                    new RuntimeException("Uncaught exception in onReadyRunnable", t));
              } catch (IllegalStateException ignored) {
              }
            } finally {
              releaseTransaction(toDispose);
            }
            return null;
          }
        };
    fetchFuture.get().handle(onReadyFn);
    return toReturn;
  }

  /**
   * Read a range of keys by providing a function that given a {@link ReadTransaction} return an {@link AsyncIterable}
   * (for example, {@link ReadTransaction#getRange(Range)}).
   *
   * @param getRangeFunction The function to produce an {@link AsyncIterable}.
   * @return {@link CompletableFuture} for the value of the key.
   */
  public CompletableFuture<List<KeyValue>> getRangeAsync(
      final Function<ReadTransaction, AsyncIterable<KeyValue>> getRangeFunction) {
    return getRangeAsync(getRangeFunction, defaultMaxAttempts);
  }

  /**
   * Read a range of keys by providing a function that given a {@link ReadTransaction} return an {@link AsyncIterable}
   * (for example, {@link ReadTransaction#getRange(Range)}).
   *
   * @param getRangeFunction The function to produce an {@link AsyncIterable}.
   * @param maxAttempts      The maximum number of attempts to make if fdb throws a retryable error.
   * @return {@link CompletableFuture} for the value of the key.
   */
  private CompletableFuture<List<KeyValue>> getRangeAsync(
      final Function<ReadTransaction, AsyncIterable<KeyValue>> getRangeFunction,
      final int maxAttempts) {
    metrics.rangeGets();
    final AtomicInteger attempts = new AtomicInteger(1);
    final CompletableFuture<List<KeyValue>> toReturn = new CompletableFuture<>();
    final AtomicReference<Transaction> currentTxn = new AtomicReference<>(getTransaction());
    AsyncIterable<KeyValue> asyncIterable = getRangeFunction.apply(currentTxn.get().snapshot());
    final AtomicReference<CompletableFuture<List<KeyValue>>> fetchFuture = new AtomicReference<>(
        asyncIterable.asList());
    final BiFunction<? super List<KeyValue>, Throwable, ? extends List<KeyValue>> onReadyFn =
        new BiFunction<List<KeyValue>, Throwable, List<KeyValue>>() {
          @Override
          public List<KeyValue> apply(List<KeyValue> result, Throwable error) {
            Transaction toDispose = currentTxn.get();
            try {
              if (toReturn.isDone()) return null;
              if (error != null) {
                if (isRetryableError(error)) {
                  int attempted = attempts.getAndIncrement();
                  if (attempted >= maxAttempts) {
                    metrics.rangeGetTimeouts();
                    toReturn.completeExceptionally(
                        new RuntimeException("too many errors (" + maxAttempts + "), " +
                            "throwing last seen error", error));
                    return null;
                  }
                  // start a new txn and try again(since it's a RetryableError)
                  disposeTransactionIfCurrent(toDispose);
                  currentTxn.set(getTransaction());
                  final AsyncIterable<KeyValue> asyncIterator = getRangeFunction.apply(
                      currentTxn.get().snapshot());
                  final CompletableFuture<List<KeyValue>> newFuture = asyncIterator.asList();
                  fetchFuture.set(newFuture);
                  newFuture.handle(this);
                } else {
                  metrics.rangeGetErrors();
                  toReturn.completeExceptionally(new RuntimeException("nonRetryableError in " +
                      "getRangeAsync() " + error));
                }
              } else {
                toReturn.complete(result);
              }
            } catch (Throwable t) {
              try {
                toReturn.completeExceptionally(
                    new RuntimeException("Uncaught exception in onReadyFn", t));
              } catch (IllegalStateException ignored) {
              }
            } finally {
              releaseTransaction(toDispose);
            }
            return null;
          }
        };
    fetchFuture.get().handle(onReadyFn);
    return toReturn;
  }

  /**
   * As long as FDB returns a error code outside of 2000 and 3000 (inclusive), we retry the get/scan.
   *
   * @param t The throwable from FDB client.
   * @return Whether it is an FDB error and it's retryable.
   */
  private boolean isRetryableError(Throwable t) {
    if (t instanceof FDBException) {
      final int errorCode = ((FDBException) t).getCode();
      return errorCode < 2000 || errorCode >= 3000;
    } else if (t.getCause() instanceof FDBException) {
      final int errorCode = ((FDBException) (t.getCause())).getCode();
      return errorCode < 2000 || errorCode >= 3000;
    }
    return false;
  }

  /**
   * Interface to handle generating metrics for the batch reader.
   */
  public interface Metrics {

    default void transactionCreated() {
    }

    default void transactionDisposed() {
    }

    default void get() {
    }

    default void getTimeouts() {
    }

    default void getErrors() {
    }

    default void rangeGets() {
    }

    default void rangeGetTimeouts() {
    }

    default void rangeGetErrors() {
    }
  }
}
