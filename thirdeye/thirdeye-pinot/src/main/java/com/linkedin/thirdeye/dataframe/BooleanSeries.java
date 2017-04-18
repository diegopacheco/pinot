package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * Series container for primitive tri-state boolean (true, false, null). Implementation uses
 * the primitive byte for internal representation.
 */
public final class BooleanSeries extends TypedSeries<BooleanSeries> {
  public static final byte NULL = Byte.MIN_VALUE;
  public static final byte TRUE = 1;
  public static final byte FALSE = 0;
  public static final byte DEFAULT = FALSE;

  public static final BooleanFunctionEx ALL_TRUE = new BooleanAllTrue();
  public static final BooleanFunctionEx HAS_TRUE = new BooleanHasTrue();
  public static final BooleanFunctionEx ALL_FALSE = new BooleanAllFalse();
  public static final BooleanFunctionEx HAS_FALSE = new BooleanHasFalse();
  public static final BooleanFunctionEx FIRST = new BooleanFirst();
  public static final BooleanFunctionEx LAST = new BooleanLast();

  public static final class BooleanAllTrue implements BooleanFunctionEx {
    @Override
    public byte apply(boolean... values) {
      if(values.length <= 0)
        return NULL;
      for(boolean b : values) {
        if(!b) return FALSE;
      }
      return TRUE;
    }
  }

  public static final class BooleanHasTrue implements BooleanFunctionEx {
    @Override
    public byte apply(boolean... values) {
      if(values.length <= 0)
        return NULL;
      for(boolean b : values) {
        if(b) return TRUE;
      }
      return FALSE;
    }
  }

  public static final class BooleanAllFalse implements BooleanFunctionEx {
    @Override
    public byte apply(boolean... values) {
      if(values.length <= 0)
        return NULL;
      return valueOf(!isTrue(HAS_TRUE.apply(values)));
    }
  }

  public static final class BooleanHasFalse implements BooleanFunctionEx {
    @Override
    public byte apply(boolean... values) {
      if(values.length <= 0)
        return NULL;
      return valueOf(!isTrue(ALL_TRUE.apply(values)));
    }
  }

  public static final class BooleanFirst implements BooleanFunctionEx {
    @Override
    public byte apply(boolean... values) {
      if(values.length <= 0)
        return NULL;
      return values[0] ? TRUE : FALSE;
    }
  }

  public static final class BooleanLast implements BooleanFunctionEx {
    @Override
    public byte apply(boolean... values) {
      if(values.length <= 0)
        return NULL;
      return values[values.length-1] ? TRUE : FALSE;
    }
  }

  public static class Builder extends Series.Builder {
    final List<byte[]> arrays = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder addValues(byte... values) {
      byte[] newValues = new byte[values.length];
      for(int i=0; i<values.length; i++) {
        newValues[i] = valueOf(values[i]);
      }
      this.arrays.add(newValues);
      return this;
    }

    public Builder addValues(byte value) {
      return this.addValues(new byte[] { value });
    }

    public Builder addValues(Collection<Byte> values) {
      byte[] newValues = new byte[values.size()];
      int i = 0;
      for(Byte v : values)
        newValues[i++] = valueOf(v);
      this.arrays.add(newValues);
      return this;
    }

    public Builder addValues(Byte... values) {
      return this.addValues(Arrays.asList(values));
    }

    public Builder addValues(Byte value) {
      return this.addValues(new byte[] { valueOf(value) });
    }

    public Builder addBooleanValues(boolean... values) {
      byte[] newValues = new byte[values.length];
      int i = 0;
      for(boolean v : values)
        newValues[i++] = valueOf(v);
      return this.addValues(newValues);
    }

    public Builder addBooleanValues(boolean value) {
      return this.addValues(new byte[] { valueOf(value) });
    }

    public Builder addBooleanValues(Collection<Boolean> values) {
      byte[] newValues = new byte[values.size()];
      int i = 0;
      for(Boolean v : values)
        newValues[i++] = valueOf(v);
      return this.addValues(newValues);
    }

    public Builder addBooleanValues(Boolean... values) {
      return this.addBooleanValues(Arrays.asList(values));
    }

    public Builder addBooleanValues(Boolean value) {
      return this.addValues(new byte[] { valueOf(value) });
    }

    @Override
    public Builder addSeries(Collection<Series> series) {
      for(Series s : series)
        this.addValues(s.getBooleans().values);
      return this;
    }

    public Builder fillValues(int count, byte value) {
      byte[] values = new byte[count];
      Arrays.fill(values, value);
      return this.addValues(values);
    }

    public Builder fillValues(int count, Byte value) {
      return this.fillValues(count, valueOf(value));
    }

    public Builder fillValues(int count, boolean value) {
      return this.fillValues(count, valueOf(value));
    }

    public Builder fillValues(int count, Boolean value) {
      return this.fillValues(count, valueOf(value));
    }

    @Override
    public BooleanSeries build() {
      int totalSize = 0;
      for(byte[] array : this.arrays)
        totalSize += array.length;

      int offset = 0;
      byte[] values = new byte[totalSize];
      for(byte[] array : this.arrays) {
        System.arraycopy(array, 0, values, offset, array.length);
        offset += array.length;
      }

      return BooleanSeries.buildFrom(values);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static BooleanSeries buildFrom(byte... values) {
    return new BooleanSeries(values);
  }

  public static BooleanSeries empty() {
    return new BooleanSeries();
  }

  public static BooleanSeries nulls(int size) {
    return builder().fillValues(size, NULL).build();
  }

  // CAUTION: The array is final, but values are inherently modifiable
  final byte[] values;

  private BooleanSeries(byte... values) {
    this.values = values;
  }

  @Override
  public BooleanSeries getBooleans() {
    return this;
  }

  @Override
  public double getDouble(int index) {
    return getDouble(this.values[index]);
  }

  public static double getDouble(byte value) {
    if(BooleanSeries.isNull(value))
      return DoubleSeries.NULL;
    return (double) value;
  }

  @Override
  public long getLong(int index) {
    return getLong(this.values[index]);
  }

  public static long getLong(byte value) {
    if(BooleanSeries.isNull(value))
      return LongSeries.NULL;
    return value;
  }

  @Override
  public byte getBoolean(int index) {
    return getBoolean(this.values[index]);
  }

  public static byte getBoolean(byte value) {
    return value;
  }

  @Override
  public String getString(int index) {
    return getString(this.values[index]);
  }

  public static String getString(byte value) {
    if(BooleanSeries.isNull(value))
      return StringSeries.NULL;
    return isTrue(value) ? "true" : "false";
  }

  @Override
  public boolean isNull(int index) {
    return isNull(this.values[index]);
  }

  @Override
  public int size() {
    return this.values.length;
  }

  @Override
  public SeriesType type() {
    return SeriesType.BOOLEAN;
  }

  public byte[] values() {
    return this.values;
  }

  public boolean[] valuesBoolean() {
    boolean[] values = new boolean[this.values.length];
    int i = 0;
    for(byte v : this.values) {
      if(!isNull(v))
        values[i++] = isTrue(v);
    }
    return Arrays.copyOf(values, i);
  }

  public byte value() {
    if(this.size() != 1)
      throw new IllegalStateException("Series must contain exactly one element");
    return this.values[0];
  }

  public boolean valueBoolean() {
    if(this.size() != 1)
      throw new IllegalStateException("Series must contain exactly one element");
    return isTrue(this.values[0]);
  }

  /**
   * Returns the value of the first element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return first element in the series
   */
  public byte first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  /**
   * Returns the value of the last element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return last element in the series
   */
  public byte last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public BooleanSeries slice(int from, int to) {
    return buildFrom(Arrays.copyOfRange(this.values, from, to));
  }

  public boolean allTrue() {
    return this.aggregate(ALL_TRUE).valueBoolean();
  }

  public boolean hasTrue() {
    return this.aggregate(HAS_TRUE).valueBoolean();
  }

  public boolean allFalse() {
    return this.aggregate(ALL_FALSE).valueBoolean();
  }

  public boolean hasFalse() {
    return this.aggregate(HAS_FALSE).valueBoolean();
  }

  public BooleanSeries not() {
    return this.map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return !values[0];
      }
    });
  }

  public BooleanSeries or(final boolean constant) {
    return this.map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return values[0] | constant;
      }
    });
  }

  public BooleanSeries or(byte constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.or(isTrue(constant));
  }

  public BooleanSeries or(Series other) {
    return map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return values[0] | values[1];
      }
    }, this, other);
  }

  public BooleanSeries and(final boolean constant) {
    return this.map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return values[0] & constant;
      }
    });
  }

  public BooleanSeries and(byte constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.and(isTrue(constant));
  }

  public BooleanSeries and(Series other) {
    return map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return values[0] & values[1];
      }
    }, this, other);
  }

  public BooleanSeries xor(final boolean constant) {
    return this.map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return values[0] ^ constant;
      }
    });
  }

  public BooleanSeries xor(byte constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.xor(isTrue(constant));
  }

  public BooleanSeries xor(Series other) {
    return map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return values[0] ^ values[1];
      }
    }, this, other);
  }

  public BooleanSeries implies(final boolean constant) {
    return this.map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return !values[0] | constant;
      }
    });
  }

  public BooleanSeries implies(byte constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.implies(isTrue(constant));
  }

  public  BooleanSeries implies(Series other) {
    return map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return !values[0] | values[1];
      }
    }, this, other);
  }

  @Override
  public BooleanSeries unique() {
    boolean hasNull = false;
    boolean hasTrue = false;
    boolean hasFalse = false;

    for(byte v : this.values) {
      hasNull |= isNull(v);
      hasFalse |= isFalse(v);
      hasTrue |= isTrue(v);
    }

    Builder b = builder();
    if(hasNull)
      b.addValues(NULL);
    if(hasFalse)
      b.addValues(FALSE);
    if(hasTrue)
      b.addValues(TRUE);

    return b.build();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("BooleanSeries{");
    for(byte b : this.values) {
      if(isNull(b)) {
        builder.append("null");
      } else {
        builder.append(isTrue(b) ? "true" : "false");
      }
      builder.append(" ");
    }
    builder.append("}");
    return builder.toString();
  }

  @Override
  public String toString(int index) {
    if(isNull(this.values[index]))
      return TOSTRING_NULL;
    if(isFalse(this.values[index]))
      return "false";
    return "true";
  }

  public boolean hasValue(boolean value) {
    return this.hasValue(valueOf(value));
  }

  public boolean hasValue(byte value) {
    for(byte v : this.values)
      if(v == value)
        return true;
    return false;
  }

  public BooleanSeries replace(boolean find, boolean by) {
    return this.replace(valueOf(find), valueOf(by));
  }

  public BooleanSeries replace(byte find, byte by) {
    byte[] values = new byte[this.values.length];
    for(int i=0; i<values.length; i++) {
      if(this.values[i] == find) {
        values[i] = by;
      } else {
        values[i] = this.values[i];
      }
    }
    return buildFrom(values);
  }

  @Override
  public BooleanSeries fillNull() {
    return this.fillNull(DEFAULT);
  }

  /**
   * Return a copy of the series with all {@code null} values replaced by
   * {@code value}.
   *
   * @param value replacement value for {@code null}
   * @return series copy without nulls
   */
  public BooleanSeries fillNull(byte value) {
    byte[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return buildFrom(values);
  }

  @Override
  public BooleanSeries shift(int offset) {
    byte[] values = new byte[this.values.length];
    if(offset >= 0) {
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), values.length, NULL);
    }
    return buildFrom(values);
  }

  @Override
  BooleanSeries project(int[] fromIndex) {
    byte[] values = new byte[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] == -1) {
        values[i] = NULL;
      } else {
        values[i] = this.values[fromIndex[i]];
      }
    }
    return buildFrom(values);
  }

  @Override
  public BooleanSeries sorted() {
    int countNull = 0;
    int countFalse = 0;
    // countTrue is rest

    for(int i=0; i<this.values.length; i++) {
      if (isNull(this.values[i])) countNull++;
      else if (isFalse(this.values[i])) countFalse++;
    }

    byte[] values = new byte[this.values.length];
    Arrays.fill(values, 0, countNull, NULL);
    Arrays.fill(values, countNull, countNull + countFalse, FALSE);
    Arrays.fill(values, countNull + countFalse, this.values.length, TRUE);

    return buildFrom(values);
  }

  @Override
  int[] sortedIndex() {
    int[] fromIndex = new int[this.values.length];
    int j=0;

    // first null
    for(int i=0; i<this.values.length; i++) {
      if(isNull(this.values[i]))
        fromIndex[j++] = i;
    }

    // then false
    for(int i=0; i<this.values.length; i++) {
      if(isFalse(this.values[i]))
        fromIndex[j++] = i;
    }

    // then true
    for(int i=0; i<this.values.length; i++) {
      if(isTrue(this.values[i]))
        fromIndex[j++] = i;
    }

    return fromIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BooleanSeries that = (BooleanSeries) o;

    return Arrays.equals(this.values, that.values);
  }

  @Override
  int compare(Series that, int indexThis, int indexThat) {
    return Byte.compare(this.values[indexThis], that.getBoolean(indexThat));
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(final BooleanFunction function, Series... series) {
    final boolean[] input = new boolean[series.length];
    return map(new BooleanFunctionEx() {
      @Override
      public byte apply(boolean... values) {
        for(int i=0; i<input.length; i++) {
          input[i] = values[i];
        }
        return function.apply(input) ? BooleanSeries.TRUE : BooleanSeries.FALSE;
      }
    }, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(BooleanFunctionEx function, Series... series) {
    if(series.length <= 0)
      return empty();

    DataFrame.assertSameLength(series);

    // Note: code-specialization to help hot-spot vm
    if(series.length == 1)
      return map(function, series[0]);
    if(series.length == 2)
      return map(function, series[0], series[1]);
    if(series.length == 3)
      return map(function, series[0], series[1], series[2]);

    boolean[] input = new boolean[series.length];
    byte[] output = new byte[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return buildFrom(output);
  }

  private static byte mapRow(BooleanFunctionEx function, Series[] series, boolean[] input, int row) {
    for(int j=0; j<series.length; j++) {
      byte value = series[j].getBoolean(row);
      if(isNull(value))
        return NULL;
      input[j] = booleanValueOf(value);
    }
    return function.apply(input);
  }

  private static BooleanSeries map(BooleanFunctionEx function, Series a) {
    byte[] output = new byte[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(booleanValueOf(a.getBoolean(i)));
      }
    }
    return buildFrom(output);
  }

  private static BooleanSeries map(BooleanFunctionEx function, Series a, Series b) {
    byte[] output = new byte[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(booleanValueOf(a.getBoolean(i)), booleanValueOf(b.getBoolean(i)));
      }
    }
    return buildFrom(output);
  }

  private static BooleanSeries map(BooleanFunctionEx function, Series a, Series b, Series c) {
    byte[] output = new byte[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i) || c.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(booleanValueOf(a.getBoolean(i)), booleanValueOf(b.getBoolean(i)), booleanValueOf(c.getBoolean(i)));
      }
    }
    return buildFrom(output);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(final BooleanConditional function, Series... series) {
    return map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return function.apply(values);
      }
    }, series);
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(BooleanFunction function, Series series) {
    if(series.hasNull())
      return buildFrom(NULL);
    return builder().addBooleanValues(function.apply(series.getBooleans().valuesBoolean())).build();
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(BooleanFunctionEx function, Series series) {
    if(series.hasNull())
      return buildFrom(NULL);
    return builder().addValues(function.apply(series.getBooleans().valuesBoolean())).build();
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(BooleanConditional function, Series series) {
    if(series.hasNull())
      return buildFrom(NULL);
    return builder().addBooleanValues(function.apply(series.getBooleans().valuesBoolean())).build();
  }

  public static boolean isNull(byte value) {
    return value == NULL;
  }

  public static boolean isFalse(byte value) {
    return value == FALSE;
  }

  public static boolean isTrue(byte value) {
    return value != NULL && value != FALSE;
  }

  public static byte valueOf(boolean value) {
    return value ? TRUE : FALSE;
  }

  public static byte valueOf(Boolean value) {
    return value == null ? NULL : valueOf(value.booleanValue());
  }

  public static byte valueOf(byte value) {
    return isNull(value) ? NULL : isFalse(value) ? FALSE : TRUE;
  }

  public static byte valueOf(Byte value) {
    return value == null ? NULL : valueOf((byte)value);
  }

  public static boolean booleanValueOf(byte value) {
    return isTrue(value);
  }

  private static byte[] assertNotEmpty(byte[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }
}