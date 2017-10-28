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

  /**
   * Construct a default StormPointBuilderPool which has a pool size of
   * 1024 PointBuilders.
   */
  @SuppressWarnings("WeakerAccess")
  public StormPointBuilderPool() {
    final Config<StormPointBuilder> config = configure()
        .setSize(DEFAULT_BUILDER_POOL_SIZE)
        .setThreadFactory(runnable -> {
            final Thread t = new Thread(runnable);
            t.setDaemon(true);
            return t;
          }
        );
    blazePool = new BlazePool<>(config);
  }

  /**
   * Construct a default StormPointBuilderPool using the specified pool configuration
   * obtained from {@link #configure()}.
   *
   * @param config a StormPot {@link Config} instance used to configure the internal
   *               pool.
   */
  @SuppressWarnings("WeakerAccess")
  public StormPointBuilderPool(final Config<StormPointBuilder> config) {
    blazePool = new BlazePool<>(config);
  }

  /**
   * Obtain a StormPot {@link Config} instance that can be used to configure the
   * pooling behavior of the internal {@link BlazePool} instance.  This configuration
   * instance is expected to be passed into a manual construction of the pool through
   * the {@link #StormPointBuilderPool(Config)} constructor.
   *
   * @return an instance of a StormPot {@link Config}
   */
  public static Config<StormPointBuilder> configure() {
    return new Config<StormPointBuilder>()
        .setAllocator(new PointBuilderAllocator());
  }

  /**
   * Borrow a {@link PoolablePointBuilder} from the pool.  The borrowed {@link PoolablePointBuilder}
   * will be automatically returned to the pool when {@link Point.Builder#build()} is called.  Once
   * borrowed, failure to to call {@link Point.Builder#build()} will result in a memory leak.
   *
   * @return a {@link PoolablePointBuilder} from the pool
   */
  @Override
  public PoolablePointBuilder borrowPointBuilder() {
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
    @Override
    public StormPointBuilder allocate(final Slot slot) throws Exception {
      return new StormPointBuilder(slot);
    }

    @Override
    public void deallocate(final StormPointBuilder poolablePointBuilder) throws Exception {
      // Note that a Poolable must never touch its slot object after it has been deallocated.
    }
  }


  private static final class StormPointBuilder extends Point.Builder implements PoolablePointBuilder, Poolable {
    private final Slot slot;
    private final Point.Builder builder = new Point.Builder();

    StormPointBuilder(final Slot slot) {
      this.slot = slot;
    }

//    @Override
//    public Point createPoint() {
//      return null;
//    }

    @Override
    public void release() {
      builder.reset();
      slot.release(this);
    }
  }
}
