package org.influxdb.dto;

import stormpot.Allocator;
import stormpot.BlazePool;
import stormpot.Config;
import stormpot.LifecycledPool;
import stormpot.Poolable;
import stormpot.Slot;
import stormpot.Timeout;

import java.util.concurrent.TimeUnit;

/**
 * StormPot based implementation of the {@link Point.PointBuilderPool} interface.
 *
 * @author brett.wooldridge [at] gmail.com
 */
@SuppressWarnings("unused")
public class StormPointBuilderPool implements Point.PointBuilderPool {
  private static final int DEFAULT_BUILDER_POOL_SIZE = 1_000_000;
  private static final Timeout TIMEOUT = new Timeout(1, TimeUnit.HOURS);

  private final LifecycledPool<StormPointBuilder> blazePool;
  private final LifecycledPool<StormPoint> pointPool;

  /**
   * Construct a default StormPointBuilderPool which has a pool size of
   * 1024 PointBuilders.
   */
  @SuppressWarnings("WeakerAccess")
  public StormPointBuilderPool() {
    final Config<StormPointBuilder> builderConfig = configureBuilderPool()
        .setSize(DEFAULT_BUILDER_POOL_SIZE)
        .setThreadFactory(runnable -> {
            final Thread t = new Thread(runnable);
            t.setDaemon(true);
            return t;
          }
        );
    blazePool = new BlazePool<>(builderConfig);
    ((PointBuilderAllocator) builderConfig.getAllocator()).setStormPointBuilderPool(this);

    final Config<StormPoint> pointConfig = configurePointPool()
            .setSize(DEFAULT_BUILDER_POOL_SIZE)
            .setThreadFactory(runnable -> {
                      final Thread t = new Thread(runnable);
                      t.setDaemon(true);
                      return t;
                    }
            );
    pointPool = new BlazePool<>(pointConfig);
  }

  /**
   * Construct a default StormPointBuilderPool using the specified builder pool configuration
   * obtained from {@link #configureBuilderPool()} and specified point pool configuration
   * obtained from {@link #configurePointPool()}.
   *
   * @param builderConfig a StormPot {@link Config} instance used to configure the {@link Point.Builder}
   *                      internal pool.
   * @param pointConfig a StormPot {@link Config} instance used to configure the {@link Point}
   *                    internal pool.
   */
  @SuppressWarnings("WeakerAccess")
  public StormPointBuilderPool(final Config<StormPointBuilder> builderConfig, Config<StormPoint> pointConfig) {
    blazePool = new BlazePool<>(builderConfig);
    pointPool = new BlazePool<>(pointConfig);

    ((PointBuilderAllocator) builderConfig.getAllocator()).setStormPointBuilderPool(this);
  }

  /**
   * Obtain a StormPot {@link Config} instance that can be used to configure the pooling
   * {@link Point.Builder} behavior of the internal {@link BlazePool} instance.  This configuration
   * instance is expected to be passed into a manual construction of the pool through
   * the {@link #StormPointBuilderPool(Config, Config)} constructor.
   *
   * @return an instance of a StormPot {@link Config}
   */
  @SuppressWarnings("WeakerAccess")
  public static Config<StormPointBuilder> configureBuilderPool() {
    return new Config<StormPointBuilder>()
        .setAllocator(new PointBuilderAllocator());
  }

  @SuppressWarnings("WeakerAccess")
  public static Config<StormPoint> configurePointPool() {
    return new Config<StormPoint>()
            .setAllocator(new PointAllocator());
  }

  /**
   * Borrow a {@link PoolablePointBuilder} from the pool.  The borrowed {@link PoolablePointBuilder}
   * will be automatically returned to the pool when {@link Point.Builder#build()} is called.  Once
   * borrowed, failure to to call {@link Point.Builder#build()} will result in a memory leak.
   *
   * @return a {@link PoolablePointBuilder} from the pool
   */
  @Override
  public Point.Builder borrowPointBuilder() {
    try {
      return blazePool.claim(TIMEOUT);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
    blazePool.shutdown();
  }


  private static final class PointBuilderAllocator implements Allocator<StormPointBuilder> {
    private StormPointBuilderPool pointBuilderPool;

    @Override
    public StormPointBuilder allocate(final Slot slot) throws Exception {
      return new StormPointBuilder(slot, pointBuilderPool);
    }

    @Override
    public void deallocate(final StormPointBuilder poolablePointBuilder) throws Exception {
      // Note that a Poolable must never touch its slot object after it has been deallocated.
    }

    void setStormPointBuilderPool(final StormPointBuilderPool pointBuilderPool) {
      this.pointBuilderPool = pointBuilderPool;
    }
  }


  private static final class StormPointBuilder extends Point.Builder implements Poolable {
    private final StormPointBuilderPool pointBuilderPool;
    private final Slot slot;

    StormPointBuilder(final Slot slot, final StormPointBuilderPool pointBuilderPool) {
      this.slot = slot;
      this.pointBuilderPool = pointBuilderPool;
    }

    @Override
    protected Point createPoint() {
      try {
        return pointBuilderPool.pointPool.claim(TIMEOUT);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void release() {
      this.reset();
      slot.release(this);
    }
  }


  private static final class PointAllocator implements Allocator<StormPoint> {
    @Override
    public StormPoint allocate(final Slot slot) throws Exception {
      return new StormPoint(slot);
    }

    @Override
    public void deallocate(final StormPoint stormPoint) throws Exception {
      // Note that a Poolable must never touch its slot object after it has been deallocated.
    }
  }

  private static final class StormPoint extends Point implements Poolable {
    private final Slot slot;

    StormPoint(final Slot slot) {
      this.slot = slot;
    }

    @Override
    public void release() {
      // point.reset();
      slot.release(this);
    }
  }
}
