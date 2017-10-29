package org.influxdb.dto;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.influxdb.impl.Preconditions;

/**
 * Representation of a InfluxDB database Point.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
@SuppressWarnings("WeakerAccess")
public class Point {

  interface PointBuilderPool {
    default Point.Builder borrowPointBuilder() {
      return new Builder();
    }

    default void shutdown() {
    }

    @SuppressWarnings("unused")
    interface PoolablePointBuilder {
      default void release() {
      }
    }
  }

  private String measurement;
  private Map<String, String> tags;
  private Long time;
  private TimeUnit precision = TimeUnit.NANOSECONDS;
  private Map<String, Object> fields;

  private static final Function<String, String> FIELD_ESCAPER = s ->
      s.replace("\\", "\\\\").replace("\"", "\\\"");
  private static final Function<String, String> KEY_ESCAPER = s ->
      s.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=");
  private static final int MAX_FRACTION_DIGITS = 340;
  private static final ThreadLocal<NumberFormat> NUMBER_FORMATTER =
          ThreadLocal.withInitial(() -> {
            NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
            numberFormat.setMaximumFractionDigits(MAX_FRACTION_DIGITS);
            numberFormat.setGroupingUsed(false);
            numberFormat.setMinimumFractionDigits(1);
            return numberFormat;
          });

  private static final ThreadLocal<Map<String, MeasurementStringBuilder>> CACHED_STRINGBUILDERS =
          ThreadLocal.withInitial(HashMap::new);

  private static final AtomicReference<PointBuilderPool> BUILDER_POOL = new AtomicReference<>();

  static {
    final String pool = System.getProperty("org.influxdb.pointBuilderPool", "default");
    switch (pool) {
      case "default":
        setPointBuilderPool(new DefaultPointBuilderPool());
        break;
      case "storm":
        setPointBuilderPool(new StormPointBuilderPool());
        break;
      default:
        setPointBuilderPool(createCustomPointBuilderPool(pool));
    }
  }

  Point() {
  }

  public static void setPointBuilderPool(final PointBuilderPool builderPool) {
    final PointBuilderPool previousPool = BUILDER_POOL.getAndSet(builderPool);
    if (previousPool != null) {
      previousPool.shutdown();
    }
  }

  /**
   * Create a new Point Build build to create a new Point in a fluent manner.
   *
   * @param measurement
   *            the name of the measurement.
   * @return the Builder to be able to add further Builder calls.
   */

  public static Builder measurement(final String measurement) {
    final Builder builder = BUILDER_POOL.get().borrowPointBuilder();
    builder.measurement = measurement;
    return builder;
  }

  /**
   * Builder for a new Point.
   *
   * @author stefan.majer [at] gmail.com
   *
   */
  @SuppressWarnings("unused")
  public static class Builder {
    private String measurement;
    private final Map<String, String> tags = new TreeMap<>();
    private final Map<String, Object> fields = new TreeMap<>();
    private Long time;
    private TimeUnit precision;

    Builder() {
    }

    /**
     * Add a tag to this point.
     *
     * @param tagName
     *            the tag name
     * @param value
     *            the tag value
     * @return the Builder instance.
     */
    public Builder tag(final String tagName, final String value) {
      Objects.requireNonNull(tagName, "tagName");
      Objects.requireNonNull(value, "value");
      if (!tagName.isEmpty() && !value.isEmpty()) {
        tags.put(tagName, value);
      }
      return this;
    }

    /**
     * Add a Map of tags to add to this point.
     *
     * @param tagsToAdd
     *            the Map of tags to add
     * @return the Builder instance.
     */
    public Builder tag(final Map<String, String> tagsToAdd) {
      for (Entry<String, String> tag : tagsToAdd.entrySet()) {
        tag(tag.getKey(), tag.getValue());
      }
      return this;
    }

    /**
     * Add a field to this point.
     *
     * @param field
     *            the field name
     * @param value
     *            the value of this field
     * @return the Builder instance.
     */
    @SuppressWarnings("checkstyle:finalparameters")
    @Deprecated
    public Builder field(final String field, Object value) {
      if (value instanceof Number) {
        if (value instanceof Byte) {
          value = ((Byte) value).doubleValue();
        } else if (value instanceof Short) {
          value = ((Short) value).doubleValue();
        } else if (value instanceof Integer) {
          value = ((Integer) value).doubleValue();
        } else if (value instanceof Long) {
          value = ((Long) value).doubleValue();
        } else if (value instanceof BigInteger) {
          value = ((BigInteger) value).doubleValue();
        }
      }
      fields.put(field, value);
      return this;
    }

    public Builder addField(final String field, final boolean value) {
      fields.put(field, value);
      return this;
    }

    public Builder addField(final String field, final long value) {
      fields.put(field, value);
      return this;
    }

    public Builder addField(final String field, final double value) {
      fields.put(field, value);
      return this;
    }

    public Builder addField(final String field, final Number value) {
      fields.put(field, value);
      return this;
    }

    public Builder addField(final String field, final String value) {
      Objects.requireNonNull(value, "value");

      fields.put(field, value);
      return this;
    }

    /**
     * Add a Map of fields to this point.
     *
     * @param fieldsToAdd
     *            the fields to add
     * @return the Builder instance.
     */
    public Builder fields(final Map<String, Object> fieldsToAdd) {
      this.fields.putAll(fieldsToAdd);
      return this;
    }

    /**
     * Add a time to this point.
     *
     * @param timeToSet the time for this point
     * @param precisionToSet the TimeUnit
     * @return the Builder instance.
     */
    public Builder time(final long timeToSet, final TimeUnit precisionToSet) {
      Objects.requireNonNull(precisionToSet, "precisionToSet");
      this.time = timeToSet;
      this.precision = precisionToSet;
      return this;
    }

    /**
     * Create a new Point.
     *
     * @return the newly created Point.
     */
    public Point build() {
      final Point point = createPoint();
      Preconditions.checkNonEmptyString(this.measurement, "measurement");
      Preconditions.checkPositiveNumber(this.fields.size(), "fields size");

      point.setTags(new TreeMap<>(this.tags));
      point.setFields(new HashMap<>(this.fields));

      point.setMeasurement(this.measurement);
      if (this.time != null) {
        point.setTime(this.time);
        point.setPrecision(this.precision);
      }

      return point;
    }

    protected Point createPoint() {
      return new Point();
    }

    void reset() {
      tags.clear();
      fields.clear();
    }
  }

  /**
   * @param measurement
   *            the measurement to set
   */
  void setMeasurement(final String measurement) {
    this.measurement = measurement;
  }

  /**
   * @param time
   *            the time to set
   */
  void setTime(final Long time) {
    this.time = time;
  }

  /**
   * @param tags
   *            the tags to set
   */
  void setTags(final Map<String, String> tags) {
    this.tags = tags;
  }

  /**
   * @return the tags
   */
  Map<String, String> getTags() {
    return this.tags;
  }

  /**
   * @param precision
   *            the precision to set
   */
  void setPrecision(final TimeUnit precision) {
    this.precision = precision;
  }

  /**
   * @param fields
   *            the fields to set
   */
  void setFields(final Map<String, Object> fields) {
    this.fields = fields;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Point point = (Point) o;
    return Objects.equals(measurement, point.measurement)
            && Objects.equals(tags, point.tags)
            && Objects.equals(time, point.time)
            && precision == point.precision
            && Objects.equals(fields, point.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(measurement, tags, time, precision, fields);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Point [name=");
    builder.append(this.measurement);
    if (this.time != null) {
      builder.append(", time=");
      builder.append(this.time);
    }
    builder.append(", tags=");
    builder.append(this.tags);
    if (this.precision != null) {
      builder.append(", precision=");
      builder.append(this.precision);
    }
    builder.append(", fields=");
    builder.append(this.fields);
    builder.append("]");
    return builder.toString();
  }

  /**
   * calculate the lineprotocol entry for a single Point.
   *
   * Documentation is WIP : https://github.com/influxdb/influxdb/pull/2997
   *
   * https://github.com/influxdb/influxdb/blob/master/tsdb/README.md
   *
   * @return the String without newLine.
   */
  public String lineProtocol() {
    final StringBuilder sb = CACHED_STRINGBUILDERS
            .get()
            .computeIfAbsent(this.measurement, MeasurementStringBuilder::new)
            .resetForUse();

    concatenatedTags(sb);
    concatenatedFields(sb);
    formatedTime(sb);

    return sb.toString();
  }

  private void concatenatedTags(final StringBuilder sb) {
    for (Entry<String, String> tag : this.tags.entrySet()) {
      sb.append(',')
        .append(KEY_ESCAPER.apply(tag.getKey()))
        .append('=')
        .append(KEY_ESCAPER.apply(tag.getValue()));
    }
    sb.append(' ');
  }

  private void concatenatedFields(final StringBuilder sb) {
    for (Entry<String, Object> field : this.fields.entrySet()) {
      Object value = field.getValue();
      if (value == null) {
        continue;
      }

      sb.append(KEY_ESCAPER.apply(field.getKey())).append('=');
      if (value instanceof Number) {
        if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
          sb.append(NUMBER_FORMATTER.get().format(value));
        } else {
          sb.append(value).append('i');
        }
      } else if (value instanceof String) {
        String stringValue = (String) value;
        sb.append('"').append(FIELD_ESCAPER.apply(stringValue)).append('"');
      } else {
        sb.append(value);
      }

      sb.append(',');
    }

    // efficiently chop off the trailing comma
    int lengthMinusOne = sb.length() - 1;
    if (sb.charAt(lengthMinusOne) == ',') {
      sb.setLength(lengthMinusOne);
    }
  }

  private void formatedTime(final StringBuilder sb) {
    if (this.time == null || this.precision == null) {
      return;
    }
    sb.append(' ').append(TimeUnit.NANOSECONDS.convert(this.time, this.precision));
  }

  private static class MeasurementStringBuilder {
    private final StringBuilder sb = new StringBuilder(128);
    private final int length;

    MeasurementStringBuilder(final String measurement) {
      this.sb.append(KEY_ESCAPER.apply(measurement));
      this.length = sb.length();
    }

    StringBuilder resetForUse() {
      sb.setLength(length);
      return sb;
    }
  }

  private static PointBuilderPool createCustomPointBuilderPool(final String pool) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = Point.class.getClassLoader();
    }

    try {
      return (PointBuilderPool) classLoader.loadClass(pool).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException("Unable to instantiate custom PointBuilderPool", e);
    }
  }

  private static class DefaultPointBuilderPool implements PointBuilderPool {
    // default interface methods
  }
}
