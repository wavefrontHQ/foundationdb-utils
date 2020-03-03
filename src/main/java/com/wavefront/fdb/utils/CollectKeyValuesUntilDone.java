package com.wavefront.fdb.utils;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.StreamingMode;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.apple.foundationdb.KeySelector.firstGreaterOrEqual;
import static com.apple.foundationdb.KeySelector.lastLessThan;
import static com.apple.foundationdb.tuple.ByteArrayUtil.strinc;

/**
 * Helper class to collect a large (i.e. a key range scan that cannot be fetched in a single
 * transaction due to FDB transaction deadline limitations). Underneath the hood, uses {@link
 * BatchReader} which means the scan is only piece-wise consistent transactional (or perhaps just
 * consider this as non-snapshot scans altogether). For scanning things that are written in batches, it
 * doesn't truly matter to us whether all points, for instance, are fetched from the same
 * snapshot.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public abstract class CollectKeyValuesUntilDone {

  /**
   * Scan a large range of keys, instantiate a data structure to collect key values (supplier),
   * accumulate results (accumulator) and then finalize it into a data structure to return.
   *
   * @param prefix      Scan prefix. Keys returned must start with this byte prefix.
   * @param batchSize   Batch size (e.g. 1000). If this value is too large, the transaction may
   *                    time-out.
   * @param metrics     Scan metrics.
   * @param batchReader {@link BatchReader} to execute scans.
   * @param supplier    Supplier of the data structure for the accumulator to work on.
   * @param accumulator Accumulator that takes in the supplier's data structure and a list of key
   *                    values.
   * @param executor    Executor for callbacks.
   * @param <T>         The returned object.
   * @return CompletableFuture of T that is completed (exceptionally or successfully) based on the
   * result of the finalizer. Any errors during iteration of runtime errors in the supplied lambdas
   * will also complete this future exceptionally.
   */
  public static <T> CompletableFuture<T> collect(
      byte[] prefix, int batchSize,
      @Nullable Metrics metrics, BatchReader batchReader,
      Supplier<T> supplier, BiFunction<T, List<KeyValue>, T> accumulator, Executor executor,
      boolean batchPriority) {
    return collect(prefix, batchSize, metrics, batchReader, supplier, accumulator,
        Function.identity(), executor, batchPriority);
  }

  /**
   * Scan a large range of keys, instantiate a data structure to collect key values (supplier),
   * accumulate results (accumulator) and then finalize it into a data structure to return.
   *
   * @param prefix      Scan prefix. Keys returned must start with this byte prefix.
   * @param batchSize   Batch size (e.g. 1000). If this value is too large, the transaction may
   *                    time-out.
   * @param metrics     Scan metrics.
   * @param batchReader {@link BatchReader} to execute scans.
   * @param supplier    Supplier of the data structure for the accumulator to work on.
   * @param accumulator Accumulator that takes in the supplier's data structure and a list of key
   *                    values.
   * @param finalizer   Finalizer that prepares the result for setting the returned {@link
   *                    CompletableFuture}
   * @param executor    Executor for callbacks.
   * @param <T>         The returned object.
   * @param <U>         The intermediate object used for accumulation of results.
   * @return CompletableFuture of T that is completed (exceptionally or successfully) based on the
   * result of the finalizer. Any errors during iteration of runtime errors in the supplied lambdas
   * will also complete this future exceptionally.
   */
  public static <T, U> CompletableFuture<T> collect(
      byte[] prefix, int batchSize,
      @Nullable Metrics metrics, BatchReader batchReader,
      Supplier<U> supplier, BiFunction<U, List<KeyValue>, U> accumulator,
      Function<U, T> finalizer, Executor executor, boolean batchPriority) {
    return collect(firstGreaterOrEqual(prefix), lastLessThan(strinc(prefix)).add(1),
        batchSize, metrics, batchReader, supplier, accumulator, finalizer, executor, batchPriority);
  }

  /**
   * Scan a large range of keys, instantiate a data structure to collect key values (supplier),
   * accumulate results (accumulator) and then finalize it into a data structure to return.
   *
   * @param startRange  Start key range.
   * @param endRange    End key range.
   * @param batchSize   Batch size (e.g. 1000). If this value is too large, the transaction may
   *                    time-out.
   * @param metrics     Scan metrics.
   * @param batchReader {@link BatchReader} to execute scans.
   * @param supplier    Supplier of the data structure for the accumulator to work on.
   * @param accumulator Accumulator that takes in the supplier's data structure and a list of key
   *                    values.
   * @param executor    Executor for callbacks.
   * @param <T>         The returned object.
   * @return CompletableFuture of T that is completed (exceptionally or successfully) based on the
   * result of the finalizer. Any errors during iteration of runtime errors in the supplied lambdas
   * will also complete this future exceptionally.
   */
  public static <T, U> CompletableFuture<T> collect(
      KeySelector startRange, KeySelector endRange, int batchSize,
      @Nullable Metrics metrics, BatchReader batchReader,
      Supplier<U> supplier, BiFunction<U, List<KeyValue>, U> accumulator,
      Function<U, T> finalizer, Executor executor,
      @SuppressWarnings("unused") boolean batchPriority) {
    return collect(startRange, endRange, batchSize, metrics, batchReader, supplier, accumulator, finalizer, executor,
        batchPriority, false);
  }

  /**
   * Scan a large range of keys, instantiate a data structure to collect key values (supplier),
   * accumulate results (accumulator) and then finalize it into a data structure to return.
   *
   * @param startRange  Start key range.
   * @param endRange    End key range.
   * @param batchSize   Batch size (e.g. 1000). If this value is too large, the transaction may
   *                    time-out.
   * @param metrics     Scan metrics.
   * @param batchReader {@link BatchReader} to execute scans.
   * @param supplier    Supplier of the data structure for the accumulator to work on.
   * @param accumulator Accumulator that takes in the supplier's data structure and a list of key
   *                    values.
   * @param executor    Executor for callbacks.
   * @param <T>         The returned object.
   * @return CompletableFuture of T that is completed (exceptionally or successfully) based on the
   * result of the finalizer. Any errors during iteration of runtime errors in the supplied lambdas
   * will also complete this future exceptionally.
   */
  public static <T> CompletableFuture<T> collect(
      KeySelector startRange, KeySelector endRange, int batchSize,
      @Nullable Metrics metrics, BatchReader batchReader,
      Supplier<T> supplier, BiFunction<T, List<KeyValue>, T> accumulator, Executor executor,
      boolean batchPriority) {
    return collect(startRange, endRange, batchSize, metrics, batchReader, supplier, accumulator,
        i -> i, executor, batchPriority);
  }

  /**
   * Scan a large range of keys, instantiate a data structure to collect key values (supplier),
   * accumulate results (accumulator) and then finalize it into a data structure to return.
   *
   * @param <T>           The returned object.
   * @param <U>           The intermediate object used for accumulation of results.
   * @param startRange    Start key range.
   * @param endRange      End key range.
   * @param batchSize     Batch size (e.g. 1000). If this value is too large, the transaction may
   *                      time-out.
   * @param metrics       Scan metrics.
   * @param batchReader   {@link BatchReader} to execute scans.
   * @param supplier      Supplier of the data structure for the accumulator to work on.
   * @param accumulator   Accumulator that takes in the supplier's data structure and a list of key
   *                      values.
   * @param finalizer     Finalizer that prepares the result for setting the returned {@link
   *                      CompletableFuture}
   * @param executor      Executor for callbacks.
   * @param batchPriority Batch priority (for FDB). Unused right now.
   * @return CompletableFuture of T that is completed (exceptionally or successfully) based on the
   * result of the finalizer. Any errors during iteration of runtime errors in the supplied lambdas
   * will also complete this future exceptionally.
   */
  public static <T, U> CompletableFuture<T> collect(
      KeySelector startRange, KeySelector endRange, int batchSize,
      @Nullable Metrics metrics, BatchReader batchReader,
      Supplier<U> supplier, BiFunction<U, List<KeyValue>, U> accumulator,
      Function<U, T> finalizer, Executor executor,
      @SuppressWarnings("unused") boolean batchPriority, boolean readSystemKeys) {
    AtomicReference<U> intermediate = new AtomicReference<>(supplier.get());
    AtomicLong start = new AtomicLong(System.currentTimeMillis());
    if (metrics != null) {
      metrics.scanIssued();
    }
    return batchReader.getRangeAsync(tx -> {
      if (readSystemKeys) {
        tx.options().setReadSystemKeys();
      }
      return tx.getRange(startRange, endRange, batchSize, false, StreamingMode.WANT_ALL);
    }).thenApply(keyValues -> {
      if (metrics != null) {
        long elapsed = System.currentTimeMillis() - start.get();
        metrics.keyValuesScanned(keyValues.size(), elapsed);
      }
      return keyValues;
    }).thenComposeAsync(
        new Function<List<KeyValue>, CompletionStage<T>>() {
          @Override
          public CompletionStage<T> apply(List<KeyValue> keyValues) {
            CompletableFuture<List<KeyValue>> nextBatch = null;
            boolean done = keyValues.size() < batchSize;
            if (!done) {
              if (metrics != null) {
                start.set(System.currentTimeMillis());
                metrics.scanIssued();
              }
              KeyValue lastKeyValue = keyValues.get(keyValues.size() - 1);
              nextBatch = batchReader.getRangeAsync(
                  tx -> {
                    if (readSystemKeys) {
                      tx.options().setReadSystemKeys();
                    }
                    return tx.getRange(KeySelector.firstGreaterThan(lastKeyValue.getKey()),
                        endRange, batchSize, false, StreamingMode.WANT_ALL);
                  }).
                  thenApply(kvs -> {
                    if (metrics != null) {
                      long elapsed = System.currentTimeMillis() - start.get();
                      metrics.keyValuesScanned(keyValues.size(), elapsed);
                    }
                    return kvs;
                  });
            }
            if (!keyValues.isEmpty()) {
              intermediate.set(accumulator.apply(intermediate.get(), keyValues));
            }
            if (done) {
              return CompletableFuture.completedFuture(finalizer.apply(intermediate.get()));
            }
            return nextBatch.thenComposeAsync(this, executor);
          }
        }, executor);
  }

  /**
   * Interface to handle generating metrics for the operations.
   */
  public interface Metrics {
    default void scanIssued() {
    }

    default void keyValuesScanned(int size, long millisecondsElapsed) {
    }
  }
}
