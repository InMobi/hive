/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.RunLengthIntegerWriterV2.EncodingType;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

class RecordReaderImpl implements RecordReader {

  private static final Log LOG = LogFactory.getLog(RecordReaderImpl.class);

  private final FSDataInputStream file;
  private final long firstRow;
  private final List<StripeInformation> stripes =
    new ArrayList<StripeInformation>();
  private OrcProto.StripeFooter stripeFooter;
  private final long totalRowCount;
  private final CompressionCodec codec;
  private final List<OrcProto.Type> types;
  private final int bufferSize;
  private final boolean[] included;
  private final long rowIndexStride;
  private long rowInStripe = 0;
  private int currentStripe = -1;
  private long rowBaseInStripe = 0;
  private long rowCountInStripe = 0;
  private final Map<StreamName, InStream> streams =
      new HashMap<StreamName, InStream>();
  private final TreeReader reader;
  private final OrcProto.RowIndex[] indexes;
  private final SearchArgument sarg;
  // the leaf predicates for the sarg
  private final List<PredicateLeaf> sargLeaves;
  // an array the same length as the sargLeaves that map them to column ids
  private final int[] filterColumns;
  // an array about which row groups aren't skipped
  private boolean[] includedRowGroups = null;

  RecordReaderImpl(Iterable<StripeInformation> stripes,
                   FileSystem fileSystem,
                   Path path,
                   long offset, long length,
                   List<OrcProto.Type> types,
                   CompressionCodec codec,
                   int bufferSize,
                   boolean[] included,
                   long strideRate,
                   SearchArgument sarg,
                   String[] columnNames
                  ) throws IOException {
    this.file = fileSystem.open(path);
    this.codec = codec;
    this.types = types;
    this.bufferSize = bufferSize;
    this.included = included;
    this.sarg = sarg;
    if (sarg != null) {
      sargLeaves = sarg.getLeaves();
      filterColumns = new int[sargLeaves.size()];
      for(int i=0; i < filterColumns.length; ++i) {
        String colName = sargLeaves.get(i).getColumnName();
        filterColumns[i] = findColumns(columnNames, colName);
      }
    } else {
      sargLeaves = null;
      filterColumns = null;
    }
    long rows = 0;
    long skippedRows = 0;
    for(StripeInformation stripe: stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) {
        skippedRows += stripe.getNumberOfRows();
      } else if (stripeStart < offset + length) {
        this.stripes.add(stripe);
        rows += stripe.getNumberOfRows();
      }
    }
    firstRow = skippedRows;
    totalRowCount = rows;
    reader = createTreeReader(path, 0, types, included);
    indexes = new OrcProto.RowIndex[types.size()];
    rowIndexStride = strideRate;
    advanceToNextRow(0L);
  }

  private static int findColumns(String[] columnNames,
                                 String columnName) {
    for(int i=0; i < columnNames.length; ++i) {
      if (columnName.equals(columnNames[i])) {
        return i;
      }
    }
    return -1;
  }

  private static final class PositionProviderImpl implements PositionProvider {
    private final OrcProto.RowIndexEntry entry;
    private int index = 0;

    PositionProviderImpl(OrcProto.RowIndexEntry entry) {
      this.entry = entry;
    }

    @Override
    public long getNext() {
      return entry.getPositions(index++);
    }
  }

  private abstract static class TreeReader {
    protected final Path path;
    protected final int columnId;
    private BitFieldReader present = null;
    protected boolean valuePresent = false;

    TreeReader(Path path, int columnId) {
      this.path = path;
      this.columnId = columnId;
    }

    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    IntegerReader createIntegerReader(OrcProto.ColumnEncoding.Kind kind,
        InStream in,
        boolean signed) throws IOException {
      switch (kind) {
      case DIRECT_V2:
      case DICTIONARY_V2:
        return new RunLengthIntegerReaderV2(in, signed);
      case DIRECT:
      case DICTIONARY:
        return new RunLengthIntegerReader(in, signed);
      default:
        throw new IllegalArgumentException("Unknown encoding " + kind);
      }
    }

    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encoding
                    ) throws IOException {
      checkEncoding(encoding.get(columnId));
      InStream in = streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.PRESENT));
      if (in == null) {
        present = null;
        valuePresent = true;
      } else {
        present = new BitFieldReader(in, 1);
      }
    }

    /**
     * Seek to the given position.
     * @param index the indexes loaded from the file
     * @throws IOException
     */
    void seek(PositionProvider[] index) throws IOException {
      if (present != null) {
        present.seek(index[columnId]);
      }
    }

    protected long countNonNulls(long rows) throws IOException {
      if (present != null) {
        long result = 0;
        for(long c=0; c < rows; ++c) {
          if (present.next() == 1) {
            result += 1;
          }
        }
        return result;
      } else {
        return rows;
      }
    }

    abstract void skipRows(long rows) throws IOException;

    Object next(Object previous) throws IOException {
      if (present != null) {
        valuePresent = present.next() == 1;
      }
      return previous;
    }
  }

  private static class BooleanTreeReader extends TreeReader{
    private BitFieldReader reader = null;

    BooleanTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                     ) throws IOException {
      super.startStripe(streams, encodings);
      reader = new BitFieldReader(streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)), 1);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      reader.seek(index[columnId]);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      BooleanWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new BooleanWritable();
        } else {
          result = (BooleanWritable) previous;
        }
        result.set(reader.next() == 1);
      }
      return result;
    }
  }

  private static class ByteTreeReader extends TreeReader{
    private RunLengthByteReader reader = null;

    ByteTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      reader = new RunLengthByteReader(streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)));
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      reader.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      ByteWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ByteWritable();
        } else {
          result = (ByteWritable) previous;
        }
        result.set(reader.next());
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  private static class ShortTreeReader extends TreeReader{
    private IntegerReader reader = null;

    ShortTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encodings.get(columnId).getKind(), streams.get(name), true);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      reader.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      ShortWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ShortWritable();
        } else {
          result = (ShortWritable) previous;
        }
        result.set((short) reader.next());
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  private static class IntTreeReader extends TreeReader{
    private IntegerReader reader = null;

    IntTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encodings.get(columnId).getKind(), streams.get(name), true);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      reader.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      IntWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new IntWritable();
        } else {
          result = (IntWritable) previous;
        }
        result.set((int) reader.next());
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  private static class LongTreeReader extends TreeReader{
    private IntegerReader reader = null;

    LongTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encodings.get(columnId).getKind(), streams.get(name), true);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      reader.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      LongWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new LongWritable();
        } else {
          result = (LongWritable) previous;
        }
        result.set(reader.next());
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  private static class FloatTreeReader extends TreeReader{
    private InStream stream;

    FloatTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      stream.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      FloatWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new FloatWritable();
        } else {
          result = (FloatWritable) previous;
        }
        result.set(SerializationUtils.readFloat(stream));
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for(int i=0; i < items; ++i) {
        SerializationUtils.readFloat(stream);
      }
    }
  }

  private static class DoubleTreeReader extends TreeReader{
    private InStream stream;

    DoubleTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name =
        new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      stream.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      DoubleWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new DoubleWritable();
        } else {
          result = (DoubleWritable) previous;
        }
        result.set(SerializationUtils.readDouble(stream));
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      stream.skip(items * 8);
    }
  }

  private static class BinaryTreeReader extends TreeReader{
    private InStream stream;
    private IntegerReader lengths = null;

    BinaryTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
      lengths = createIntegerReader(encodings.get(columnId).getKind(), streams.get(new
          StreamName(columnId, OrcProto.Stream.Kind.LENGTH)), false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      stream.seek(index[columnId]);
      lengths.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      BytesWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new BytesWritable();
        } else {
          result = (BytesWritable) previous;
        }
        int len = (int) lengths.next();
        result.setSize(len);
        int offset = 0;
        while (len > 0) {
          int written = stream.read(result.getBytes(), offset, len);
          if (written < 0) {
            throw new EOFException("Can't finish byte read from " + stream);
          }
          len -= written;
          offset += written;
        }
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long lengthToSkip = 0;
      for(int i=0; i < items; ++i) {
        lengthToSkip += lengths.next();
      }
      stream.skip(lengthToSkip);
    }
  }

  private static class TimestampTreeReader extends TreeReader{
    private IntegerReader data = null;
    private IntegerReader nanos = null;

    TimestampTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      data = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.DATA)), true);
      nanos = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.SECONDARY)), false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      data.seek(index[columnId]);
      nanos.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      Timestamp result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new Timestamp(0);
        } else {
          result = (Timestamp) previous;
        }
        long millis = (data.next() + WriterImpl.BASE_TIMESTAMP) *
            WriterImpl.MILLIS_PER_SECOND;
        int newNanos = parseNanos(nanos.next());
        // fix the rounding when we divided by 1000.
        if (millis >= 0) {
          millis += newNanos / 1000000;
        } else {
          millis -= newNanos / 1000000;
        }
        result.setTime(millis);
        result.setNanos(newNanos);
      }
      return result;
    }

    private static int parseNanos(long serialized) {
      int zeros = 7 & (int) serialized;
      int result = (int) serialized >>> 3;
      if (zeros != 0) {
        for(int i =0; i <= zeros; ++i) {
          result *= 10;
        }
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      data.skip(items);
      nanos.skip(items);
    }
  }

  private static class DateTreeReader extends TreeReader{
    private IntegerReader reader = null;

    DateTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encodings.get(columnId).getKind(), streams.get(name), true);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      reader.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      Date result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new Date(0);
        } else {
          result = (Date) previous;
        }
        result.setTime(DateWritable.daysToMillis((int) reader.next()));
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  private static class DecimalTreeReader extends TreeReader{
    private InStream valueStream;
    private IntegerReader scaleStream = null;

    DecimalTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
    ) throws IOException {
      super.startStripe(streams, encodings);
      valueStream = streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA));
      scaleStream = createIntegerReader(encodings.get(columnId).getKind(), streams.get(
          new StreamName(columnId, OrcProto.Stream.Kind.SECONDARY)), true);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      valueStream.seek(index[columnId]);
      scaleStream.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      if (valuePresent) {
        return new HiveDecimal(SerializationUtils.readBigInteger(valueStream),
            (int) scaleStream.next());
      }
      return null;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for(int i=0; i < items; i++) {
        SerializationUtils.readBigInteger(valueStream);
      }
      scaleStream.skip(items);
    }
  }

  /**
   * A tree reader that will read string columns. At the start of the
   * stripe, it creates an internal reader based on whether a direct or
   * dictionary encoding was used.
   */
  private static class StringTreeReader extends TreeReader {
    private TreeReader reader;

    StringTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      reader.checkEncoding(encoding);
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      // For each stripe, checks the encoding and initializes the appropriate
      // reader
      switch (encodings.get(columnId).getKind()) {
        case DIRECT:
        case DIRECT_V2:
          reader = new StringDirectTreeReader(path, columnId);
          break;
        case DICTIONARY:
        case DICTIONARY_V2:
          reader = new StringDictionaryTreeReader(path, columnId);
          break;
        default:
          throw new IllegalArgumentException("Unsupported encoding " +
              encodings.get(columnId).getKind());
      }
      reader.startStripe(streams, encodings);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      return reader.next(previous);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skipRows(items);
    }
  }

  /**
   * A reader for string columns that are direct encoded in the current
   * stripe.
   */
  private static class StringDirectTreeReader extends TreeReader {
    private InStream stream;
    private IntegerReader lengths;

    StringDirectTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
      lengths = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(new StreamName(columnId, OrcProto.Stream.Kind.LENGTH)),
          false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      stream.seek(index[columnId]);
      lengths.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      Text result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new Text();
        } else {
          result = (Text) previous;
        }
        int len = (int) lengths.next();
        int offset = 0;
        byte[] bytes = new byte[len];
        while (len > 0) {
          int written = stream.read(bytes, offset, len);
          if (written < 0) {
            throw new EOFException("Can't finish byte read from " + stream);
          }
          len -= written;
          offset += written;
        }
        result.set(bytes);
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long lengthToSkip = 0;
      for(int i=0; i < items; ++i) {
        lengthToSkip += lengths.next();
      }
      stream.skip(lengthToSkip);
    }
  }

  /**
   * A reader for string columns that are dictionary encoded in the current
   * stripe.
   */
  private static class StringDictionaryTreeReader extends TreeReader {
    private DynamicByteArray dictionaryBuffer;
    private int[] dictionaryOffsets;
    private IntegerReader reader;

    StringDictionaryTreeReader(Path path, int columnId) {
      super(path, columnId);
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DICTIONARY &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);

      // read the dictionary blob
      int dictionarySize = encodings.get(columnId).getDictionarySize();
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DICTIONARY_DATA);
      InStream in = streams.get(name);
      if (in.available() > 0) {
        dictionaryBuffer = new DynamicByteArray(64, in.available());
        dictionaryBuffer.readAll(in);
      } else {
        dictionaryBuffer = null;
      }
      in.close();

      // read the lengths
      name = new StreamName(columnId, OrcProto.Stream.Kind.LENGTH);
      in = streams.get(name);
      IntegerReader lenReader = createIntegerReader(encodings.get(columnId)
          .getKind(), in, false);
      int offset = 0;
      if (dictionaryOffsets == null ||
          dictionaryOffsets.length < dictionarySize + 1) {
        dictionaryOffsets = new int[dictionarySize + 1];
      }
      for(int i=0; i < dictionarySize; ++i) {
        dictionaryOffsets[i] = offset;
        offset += (int) lenReader.next();
      }
      dictionaryOffsets[dictionarySize] = offset;
      in.close();

      // set up the row reader
      name = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(name), false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      reader.seek(index[columnId]);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      Text result = null;
      if (valuePresent) {
        int entry = (int) reader.next();
        if (previous == null) {
          result = new Text();
        } else {
          result = (Text) previous;
        }
        int offset = dictionaryOffsets[entry];
        int length;
        // if it isn't the last entry, subtract the offsets otherwise use
        // the buffer length.
        if (entry < dictionaryOffsets.length - 1) {
          length = dictionaryOffsets[entry + 1] - offset;
        } else {
          length = dictionaryBuffer.size() - offset;
        }
        // If the column is just empty strings, the size will be zero,
        // so the buffer will be null, in that case just return result
        // as it will default to empty
        if (dictionaryBuffer != null) {
          dictionaryBuffer.setText(result, offset, length);
        } else {
          result.clear();
        }
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  private static class VarcharTreeReader extends StringTreeReader {
    int maxLength;

    VarcharTreeReader(Path path, int columnId, int maxLength) {
      super(path, columnId);
      this.maxLength = maxLength;
    }

    @Override
    Object next(Object previous) throws IOException {
      HiveVarcharWritable result = null;
      if (previous == null) {
        result = new HiveVarcharWritable();
      } else {
        result = (HiveVarcharWritable) previous;
      }
      // Use the string reader implementation to populate the internal Text value
      Object textVal = super.next(result.getTextValue());
      if (textVal == null) {
        return null;
      }
      // result should now hold the value that was read in.
      // enforce varchar length
      result.enforceMaxLength(maxLength);
      return result;
    }
  }

  private static class StructTreeReader extends TreeReader {
    private final TreeReader[] fields;
    private final String[] fieldNames;

    StructTreeReader(Path path, int columnId,
                     List<OrcProto.Type> types,
                     boolean[] included) throws IOException {
      super(path, columnId);
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getFieldNamesCount();
      this.fields = new TreeReader[fieldCount];
      this.fieldNames = new String[fieldCount];
      for(int i=0; i < fieldCount; ++i) {
        int subtype = type.getSubtypes(i);
        if (included == null || included[subtype]) {
          this.fields[i] = createTreeReader(path, subtype, types, included);
        }
        this.fieldNames[i] = type.getFieldNames(i);
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      for(TreeReader kid: fields) {
        if (kid != null) {
          kid.seek(index);
        }
      }
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      OrcStruct result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new OrcStruct(fields.length);
        } else {
          result = (OrcStruct) previous;

          // If the input format was initialized with a file with a
          // different number of fields, the number of fields needs to
          // be updated to the correct number
          if (result.getNumFields() != fields.length) {
            result.setNumFields(fields.length);
          }
        }
        for(int i=0; i < fields.length; ++i) {
          if (fields[i] != null) {
            result.setFieldValue(i, fields[i].next(result.getFieldValue(i)));
          }
        }
      }
      return result;
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      for(TreeReader field: fields) {
        if (field != null) {
          field.startStripe(streams, encodings);
        }
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for(TreeReader field: fields) {
        if (field != null) {
          field.skipRows(items);
        }
      }
    }
  }

  private static class UnionTreeReader extends TreeReader {
    private final TreeReader[] fields;
    private RunLengthByteReader tags;

    UnionTreeReader(Path path, int columnId,
                    List<OrcProto.Type> types,
                    boolean[] included) throws IOException {
      super(path, columnId);
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getSubtypesCount();
      this.fields = new TreeReader[fieldCount];
      for(int i=0; i < fieldCount; ++i) {
        int subtype = type.getSubtypes(i);
        if (included == null || included[subtype]) {
          this.fields[i] = createTreeReader(path, subtype, types, included);
        }
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      tags.seek(index[columnId]);
      for(TreeReader kid: fields) {
        kid.seek(index);
      }
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      OrcUnion result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new OrcUnion();
        } else {
          result = (OrcUnion) previous;
        }
        byte tag = tags.next();
        Object previousVal = result.getObject();
        result.set(tag, fields[tag].next(tag == result.getTag() ?
            previousVal : null));
      }
      return result;
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                     ) throws IOException {
      super.startStripe(streams, encodings);
      tags = new RunLengthByteReader(streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)));
      for(TreeReader field: fields) {
        if (field != null) {
          field.startStripe(streams, encodings);
        }
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long[] counts = new long[fields.length];
      for(int i=0; i < items; ++i) {
        counts[tags.next()] += 1;
      }
      for(int i=0; i < counts.length; ++i) {
        fields[i].skipRows(counts[i]);
      }
    }
  }

  private static class ListTreeReader extends TreeReader {
    private final TreeReader elementReader;
    private IntegerReader lengths = null;

    ListTreeReader(Path path, int columnId,
                   List<OrcProto.Type> types,
                   boolean[] included) throws IOException {
      super(path, columnId);
      OrcProto.Type type = types.get(columnId);
      elementReader = createTreeReader(path, type.getSubtypes(0), types,
          included);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      lengths.seek(index[columnId]);
      elementReader.seek(index);
    }

    @Override
    @SuppressWarnings("unchecked")
    Object next(Object previous) throws IOException {
      super.next(previous);
      List<Object> result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ArrayList<Object>();
        } else {
          result = (ArrayList<Object>) previous;
        }
        int prevLength = result.size();
        int length = (int) lengths.next();
        // extend the list to the new length
        for(int i=prevLength; i < length; ++i) {
          result.add(null);
        }
        // read the new elements into the array
        for(int i=0; i< length; i++) {
          result.set(i, elementReader.next(i < prevLength ?
              result.get(i) : null));
        }
        // remove any extra elements
        for(int i=prevLength - 1; i >= length; --i) {
          result.remove(i);
        }
      }
      return result;
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      lengths = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.LENGTH)), false);
      if (elementReader != null) {
        elementReader.startStripe(streams, encodings);
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long childSkip = 0;
      for(long i=0; i < items; ++i) {
        childSkip += lengths.next();
      }
      elementReader.skipRows(childSkip);
    }
  }

  private static class MapTreeReader extends TreeReader {
    private final TreeReader keyReader;
    private final TreeReader valueReader;
    private IntegerReader lengths = null;

    MapTreeReader(Path path,
                  int columnId,
                  List<OrcProto.Type> types,
                  boolean[] included) throws IOException {
      super(path, columnId);
      OrcProto.Type type = types.get(columnId);
      int keyColumn = type.getSubtypes(0);
      int valueColumn = type.getSubtypes(1);
      if (included == null || included[keyColumn]) {
        keyReader = createTreeReader(path, keyColumn, types, included);
      } else {
        keyReader = null;
      }
      if (included == null || included[valueColumn]) {
        valueReader = createTreeReader(path, valueColumn, types, included);
      } else {
        valueReader = null;
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      lengths.seek(index[columnId]);
      keyReader.seek(index);
      valueReader.seek(index);
    }

    @Override
    @SuppressWarnings("unchecked")
    Object next(Object previous) throws IOException {
      super.next(previous);
      Map<Object, Object> result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new HashMap<Object, Object>();
        } else {
          result = (HashMap<Object, Object>) previous;
        }
        // for now just clear and create new objects
        result.clear();
        int length = (int) lengths.next();
        // read the new elements into the array
        for(int i=0; i< length; i++) {
          result.put(keyReader.next(null), valueReader.next(null));
        }
      }
      return result;
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId + " of " + path);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      lengths = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.LENGTH)), false);
      if (keyReader != null) {
        keyReader.startStripe(streams, encodings);
      }
      if (valueReader != null) {
        valueReader.startStripe(streams, encodings);
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long childSkip = 0;
      for(long i=0; i < items; ++i) {
        childSkip += lengths.next();
      }
      keyReader.skipRows(childSkip);
      valueReader.skipRows(childSkip);
    }
  }

  private static TreeReader createTreeReader(Path path,
                                             int columnId,
                                             List<OrcProto.Type> types,
                                             boolean[] included
                                            ) throws IOException {
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case BOOLEAN:
        return new BooleanTreeReader(path, columnId);
      case BYTE:
        return new ByteTreeReader(path, columnId);
      case DOUBLE:
        return new DoubleTreeReader(path, columnId);
      case FLOAT:
        return new FloatTreeReader(path, columnId);
      case SHORT:
        return new ShortTreeReader(path, columnId);
      case INT:
        return new IntTreeReader(path, columnId);
      case LONG:
        return new LongTreeReader(path, columnId);
      case STRING:
        return new StringTreeReader(path, columnId);
      case VARCHAR:
        if (!type.hasMaximumLength()) {
          throw new IllegalArgumentException("ORC varchar type has no length specified");
        }
        return new VarcharTreeReader(path, columnId, type.getMaximumLength());
      case BINARY:
        return new BinaryTreeReader(path, columnId);
      case TIMESTAMP:
        return new TimestampTreeReader(path, columnId);
      case DATE:
        return new DateTreeReader(path, columnId);
      case DECIMAL:
        return new DecimalTreeReader(path, columnId);
      case STRUCT:
        return new StructTreeReader(path, columnId, types, included);
      case LIST:
        return new ListTreeReader(path, columnId, types, included);
      case MAP:
        return new MapTreeReader(path, columnId, types, included);
      case UNION:
        return new UnionTreeReader(path, columnId, types, included);
      default:
        throw new IllegalArgumentException("Unsupported type " +
          type.getKind());
    }
  }

  OrcProto.StripeFooter readStripeFooter(StripeInformation stripe
                                         ) throws IOException {
    long offset = stripe.getOffset() + stripe.getIndexLength() +
        stripe.getDataLength();
    int tailLength = (int) stripe.getFooterLength();

    // read the footer
    ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
    file.seek(offset);
    file.readFully(tailBuf.array(), tailBuf.arrayOffset(), tailLength);
    return OrcProto.StripeFooter.parseFrom(InStream.create("footer",
        new ByteBuffer[]{tailBuf}, new long[]{0}, tailLength, codec,
        bufferSize));
  }

  static enum Location {
    BEFORE, MIN, MIDDLE, MAX, AFTER
  }

  /**
   * Given a point and min and max, determine if the point is before, at the
   * min, in the middle, at the max, or after the range.
   * @param point the point to test
   * @param min the minimum point
   * @param max the maximum point
   * @param <T> the type of the comparision
   * @return the location of the point
   */
  static <T> Location compareToRange(Comparable<T> point, T min, T max) {
    int minCompare = point.compareTo(min);
    if (minCompare < 0) {
      return Location.BEFORE;
    } else if (minCompare == 0) {
      return Location.MIN;
    }
    int maxCompare = point.compareTo(max);
    if (maxCompare > 0) {
      return Location.AFTER;
    } else if (maxCompare == 0) {
      return Location.MAX;
    }
    return Location.MIDDLE;
  }

  /**
   * Get the minimum value out of an index entry.
   * @param index the index entry
   * @return the object for the minimum value or null if there isn't one
   */
  static Object getMin(OrcProto.ColumnStatistics index) {
    if (index.hasIntStatistics()) {
      OrcProto.IntegerStatistics stat = index.getIntStatistics();
      if (stat.hasMinimum()) {
        return stat.getMinimum();
      }
    }
    if (index.hasStringStatistics()) {
      OrcProto.StringStatistics stat = index.getStringStatistics();
      if (stat.hasMinimum()) {
        return stat.getMinimum();
      }
    }
    if (index.hasDoubleStatistics()) {
      OrcProto.DoubleStatistics stat = index.getDoubleStatistics();
      if (stat.hasMinimum()) {
        return stat.getMinimum();
      }
    }
    return null;
  }

  /**
   * Get the maximum value out of an index entry.
   * @param index the index entry
   * @return the object for the maximum value or null if there isn't one
   */
  static Object getMax(OrcProto.ColumnStatistics index) {
    if (index.hasIntStatistics()) {
      OrcProto.IntegerStatistics stat = index.getIntStatistics();
      if (stat.hasMaximum()) {
        return stat.getMaximum();
      }
    }
    if (index.hasStringStatistics()) {
      OrcProto.StringStatistics stat = index.getStringStatistics();
      if (stat.hasMaximum()) {
        return stat.getMaximum();
      }
    }
    if (index.hasDoubleStatistics()) {
      OrcProto.DoubleStatistics stat = index.getDoubleStatistics();
      if (stat.hasMaximum()) {
        return stat.getMaximum();
      }
    }
    return null;
  }

  /**
   * Evaluate a predicate with respect to the statistics from the column
   * that is referenced in the predicate.
   * @param index the statistics for the column mentioned in the predicate
   * @param predicate the leaf predicate we need to evaluation
   * @return the set of truth values that may be returned for the given
   *   predicate.
   */
  static TruthValue evaluatePredicate(OrcProto.ColumnStatistics index,
                               PredicateLeaf predicate) {
    Object minValue = getMin(index);
    // if we didn't have any values, everything must have been null
    if (minValue == null) {
      if (predicate.getOperator() == PredicateLeaf.Operator.IS_NULL) {
        return TruthValue.YES;
      } else {
        return TruthValue.NULL;
      }
    }
    Object maxValue = getMax(index);
    Location loc;
    switch (predicate.getOperator()) {
      case NULL_SAFE_EQUALS:
        loc = compareToRange((Comparable) predicate.getLiteral(),
            minValue, maxValue);
        if (loc == Location.BEFORE || loc == Location.AFTER) {
          return TruthValue.NO;
        } else {
          return TruthValue.YES_NO;
        }
      case EQUALS:
        loc = compareToRange((Comparable) predicate.getLiteral(),
            minValue, maxValue);
        if (minValue.equals(maxValue) && loc == Location.MIN) {
          return TruthValue.YES_NULL;
        } else if (loc == Location.BEFORE || loc == Location.AFTER) {
          return TruthValue.NO_NULL;
        } else {
          return TruthValue.YES_NO_NULL;
        }
      case LESS_THAN:
        loc = compareToRange((Comparable) predicate.getLiteral(),
            minValue, maxValue);
        if (loc == Location.AFTER) {
          return TruthValue.YES_NULL;
        } else if (loc == Location.BEFORE || loc == Location.MIN) {
          return TruthValue.NO_NULL;
        } else {
          return TruthValue.YES_NO_NULL;
        }
      case LESS_THAN_EQUALS:
        loc = compareToRange((Comparable) predicate.getLiteral(),
            minValue, maxValue);
        if (loc == Location.AFTER || loc == Location.MAX) {
          return TruthValue.YES_NULL;
        } else if (loc == Location.BEFORE) {
          return TruthValue.NO_NULL;
        } else {
          return TruthValue.YES_NO_NULL;
        }
      case IN:
        if (minValue.equals(maxValue)) {
          // for a single value, look through to see if that value is in the
          // set
          for(Object arg: predicate.getLiteralList()) {
            loc = compareToRange((Comparable) arg, minValue, maxValue);
            if (loc == Location.MIN) {
              return TruthValue.YES_NULL;
            }
          }
          return TruthValue.NO_NULL;
        } else {
          // are all of the values outside of the range?
          for(Object arg: predicate.getLiteralList()) {
            loc = compareToRange((Comparable) arg, minValue, maxValue);
            if (loc == Location.MIN || loc == Location.MIDDLE ||
                loc == Location.MAX) {
              return TruthValue.YES_NO_NULL;
            }
          }
          return TruthValue.NO_NULL;
        }
      case BETWEEN:
        List<Object> args = predicate.getLiteralList();
        loc = compareToRange((Comparable) args.get(0), minValue, maxValue);
        if (loc == Location.BEFORE || loc == Location.MIN) {
          Location loc2 = compareToRange((Comparable) args.get(1), minValue,
              maxValue);
          if (loc2 == Location.AFTER || loc2 == Location.MAX) {
            return TruthValue.YES_NULL;
          } else if (loc2 == Location.BEFORE) {
            return TruthValue.NO_NULL;
          } else {
            return TruthValue.YES_NO_NULL;
          }
        } else if (loc == Location.AFTER) {
          return TruthValue.NO_NULL;
        } else {
          return TruthValue.YES_NO_NULL;
        }
      case IS_NULL:
        return TruthValue.YES_NO;
      default:
        return TruthValue.YES_NO_NULL;
    }
  }

  /**
   * Pick the row groups that we need to load from the current stripe.
   * @return an array with a boolean for each row group or null if all of the
   *    row groups must be read.
   * @throws IOException
   */
  private boolean[] pickRowGroups() throws IOException {
    // if we don't have a sarg or indexes, we read everything
    if (sarg == null || rowIndexStride == 0) {
      return null;
    }
    readRowIndex();
    long rowsInStripe = stripes.get(currentStripe).getNumberOfRows();
    int groupsInStripe = (int) ((rowsInStripe + rowIndexStride - 1) /
        rowIndexStride);
    boolean[] result = new boolean[groupsInStripe];
    TruthValue[] leafValues = new TruthValue[sargLeaves.size()];
    for(int rowGroup=0; rowGroup < result.length; ++rowGroup) {
      for(int pred=0; pred < leafValues.length; ++pred) {
        if (filterColumns[pred] != -1) {
          OrcProto.ColumnStatistics stats =
              indexes[filterColumns[pred]].getEntry(rowGroup).getStatistics();
          leafValues[pred] = evaluatePredicate(stats, sargLeaves.get(pred));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Stats = " + stats);
            LOG.debug("Setting " + sargLeaves.get(pred) + " to " +
                leafValues[pred]);
          }
        } else {
          // the column is a virtual column
          leafValues[pred] = TruthValue.YES_NO_NULL;
        }
      }
      result[rowGroup] = sarg.evaluate(leafValues).isNotNeeded();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Row group " + (rowIndexStride * rowGroup) + " to " +
            (rowIndexStride * (rowGroup+1) - 1) + " is " +
            (result[rowGroup] ? "" : "not ") + "included.");
      }
    }

    // if we found something to skip, use the array. otherwise, return null.
    for(boolean b: result) {
      if (!b) {
        return result;
      }
    }
    return null;
  }

  /**
   * Read the current stripe into memory.
   * @throws IOException
   */
  private void readStripe() throws IOException {
    StripeInformation stripe = stripes.get(currentStripe);
    stripeFooter = readStripeFooter(stripe);
    streams.clear();
    // setup the position in the stripe
    rowCountInStripe = stripe.getNumberOfRows();
    rowInStripe = 0;
    rowBaseInStripe = 0;
    for(int i=0; i < currentStripe; ++i) {
      rowBaseInStripe += stripes.get(i).getNumberOfRows();
    }
    // reset all of the indexes
    for(int i=0; i < indexes.length; ++i) {
      indexes[i] = null;
    }
    includedRowGroups = pickRowGroups();

    // move forward to the first unskipped row
    if (includedRowGroups != null) {
      while (rowInStripe < rowCountInStripe &&
             !includedRowGroups[(int) (rowInStripe / rowIndexStride)]) {
        rowInStripe = Math.min(rowCountInStripe, rowInStripe + rowIndexStride);
      }
    }

    // if we haven't skipped the whole stripe, read the data
    if (rowInStripe < rowCountInStripe) {
      // if we aren't projecting columns or filtering rows, just read it all
      if (included == null && includedRowGroups == null) {
        readAllDataStreams(stripe);
      } else {
        readPartialDataStreams(stripe);
      }
      reader.startStripe(streams, stripeFooter.getColumnsList());
      // if we skipped the first row group, move the pointers forward
      if (rowInStripe != 0) {
        seekToRowEntry((int) (rowInStripe / rowIndexStride));
      }
    }
  }

  private void readAllDataStreams(StripeInformation stripe
                                  ) throws IOException {
    byte[] buffer =
      new byte[(int) (stripe.getDataLength())];
    file.seek(stripe.getOffset() + stripe.getIndexLength());
    file.readFully(buffer, 0, buffer.length);
    int sectionOffset = 0;
    for(OrcProto.Stream section: stripeFooter.getStreamsList()) {
      if (StreamName.getArea(section.getKind()) == StreamName.Area.DATA) {
        int sectionLength = (int) section.getLength();
        ByteBuffer sectionBuffer = ByteBuffer.wrap(buffer, sectionOffset,
            sectionLength);
        StreamName name = new StreamName(section.getColumn(),
            section.getKind());
        streams.put(name,
            InStream.create(name.toString(), new ByteBuffer[]{sectionBuffer},
                new long[]{0}, sectionLength, codec, bufferSize));
        sectionOffset += sectionLength;
      }
    }
  }

  /**
   * The secionts of stripe that we need to read.
   */
  static class DiskRange {
    /** the first address we need to read. */
    long offset;
    /** the first address afterwards. */
    long end;

    DiskRange(long offset, long end) {
      this.offset = offset;
      this.end = end;
      if (end < offset) {
        throw new IllegalArgumentException("invalid range " + this);
      }
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || other.getClass() != getClass()) {
        return false;
      }
      DiskRange otherR = (DiskRange) other;
      return otherR.offset == offset && otherR.end == end;
    }

    @Override
    public String toString() {
      return "range start: " + offset + " end: " + end;
    }
  }

  private static final int BYTE_STREAM_POSITIONS = 1;
  private static final int RUN_LENGTH_BYTE_POSITIONS =
      BYTE_STREAM_POSITIONS + 1;
  private static final int BITFIELD_POSITIONS = RUN_LENGTH_BYTE_POSITIONS + 1;
  private static final int RUN_LENGTH_INT_POSITIONS =
    BYTE_STREAM_POSITIONS + 1;

  /**
   * Get the offset in the index positions for the column that the given
   * stream starts.
   * @param encoding the encoding of the column
   * @param type the type of the column
   * @param stream the kind of the stream
   * @param isCompressed is the file compressed
   * @param hasNulls does the column have a PRESENT stream?
   * @return the number of positions that will be used for that stream
   */
  static int getIndexPosition(OrcProto.ColumnEncoding.Kind encoding,
                              OrcProto.Type.Kind type,
                              OrcProto.Stream.Kind stream,
                              boolean isCompressed,
                              boolean hasNulls) {
    if (stream == OrcProto.Stream.Kind.PRESENT) {
      return 0;
    }
    int compressionValue = isCompressed ? 1 : 0;
    int base = hasNulls ? (BITFIELD_POSITIONS + compressionValue) : 0;
    switch (type) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRUCT:
      case MAP:
      case LIST:
      case UNION:
        return base;
      case STRING:
        if (encoding == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
            encoding == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
          return base;
        } else {
          if (stream == OrcProto.Stream.Kind.DATA) {
            return base;
          } else {
            return base + BYTE_STREAM_POSITIONS + compressionValue;
          }
        }
      case BINARY:
        if (stream == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + BYTE_STREAM_POSITIONS + compressionValue;
      case DECIMAL:
        if (stream == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + BYTE_STREAM_POSITIONS + compressionValue;
      case TIMESTAMP:
        if (stream == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + RUN_LENGTH_INT_POSITIONS + compressionValue;
      default:
        throw new IllegalArgumentException("Unknown type " + type);
    }
  }

  // for uncompressed streams, what is the most overlap with the following set
  // of rows (long vint literal group).
  static final int WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;

  /**
   * Is this stream part of a dictionary?
   * @return is this part of a dictionary?
   */
  static boolean isDictionary(OrcProto.Stream.Kind kind,
                              OrcProto.ColumnEncoding encoding) {
    OrcProto.ColumnEncoding.Kind encodingKind = encoding.getKind();
    return kind == OrcProto.Stream.Kind.DICTIONARY_DATA ||
      (kind == OrcProto.Stream.Kind.LENGTH &&
       (encodingKind == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
        encodingKind == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2));
  }

  /**
   * Plan the ranges of the file that we need to read given the list of
   * columns and row groups.
   * @param streamList the list of streams avaiable
   * @param indexes the indexes that have been loaded
   * @param includedColumns which columns are needed
   * @param includedRowGroups which row groups are needed
   * @param isCompressed does the file have generic compression
   * @param encodings the encodings for each column
   * @param types the types of the columns
   * @param compressionSize the compression block size
   * @return the list of disk ranges that will be loaded
   */
  static List<DiskRange> planReadPartialDataStreams
      (List<OrcProto.Stream> streamList,
       OrcProto.RowIndex[] indexes,
       boolean[] includedColumns,
       boolean[] includedRowGroups,
       boolean isCompressed,
       List<OrcProto.ColumnEncoding> encodings,
       List<OrcProto.Type> types,
       int compressionSize) {
    List<DiskRange> result = new ArrayList<DiskRange>();
    long offset = 0;
    // figure out which columns have a present stream
    boolean[] hasNull = new boolean[types.size()];
    for(OrcProto.Stream stream: streamList) {
      if (stream.getKind() == OrcProto.Stream.Kind.PRESENT) {
        hasNull[stream.getColumn()] = true;
      }
    }
    for(OrcProto.Stream stream: streamList) {
      long length = stream.getLength();
      int column = stream.getColumn();
      OrcProto.Stream.Kind streamKind = stream.getKind();
      if (StreamName.getArea(streamKind) == StreamName.Area.DATA &&
          includedColumns[column]) {
        // if we aren't filtering or it is a dictionary, load it.
        if (includedRowGroups == null ||
            isDictionary(streamKind, encodings.get(column))) {
          result.add(new DiskRange(offset, offset + length));
        } else {
          for(int group=0; group < includedRowGroups.length; ++group) {
            if (includedRowGroups[group]) {
              int posn = getIndexPosition(encodings.get(column).getKind(),
                  types.get(column).getKind(), stream.getKind(), isCompressed,
                  hasNull[column]);
              long start = indexes[column].getEntry(group).getPositions(posn);
              // figure out the worst case last location
              long end = (group == includedRowGroups.length - 1) ?
                  length : Math.min(length,
                                    indexes[column].getEntry(group + 1)
                                        .getPositions(posn)
                                        + (isCompressed ?
                                            (OutStream.HEADER_SIZE
                                              + compressionSize) :
                                            WORST_UNCOMPRESSED_SLOP));
              result.add(new DiskRange(offset + start, offset + end));
            }
          }
        }
      }
      offset += length;
    }
    return result;
  }

  /**
   * Update the disk ranges to collapse adjacent or overlapping ranges. It
   * assumes that the ranges are sorted.
   * @param ranges the list of disk ranges to merge
   */
  static void mergeDiskRanges(List<DiskRange> ranges) {
    DiskRange prev = null;
    for(int i=0; i < ranges.size(); ++i) {
      DiskRange current = ranges.get(i);
      if (prev != null && overlap(prev.offset, prev.end,
          current.offset, current.end)) {
        prev.offset = Math.min(prev.offset, current.offset);
        prev.end = Math.max(prev.end, current.end);
        ranges.remove(i);
        i -= 1;
      } else {
        prev = current;
      }
    }
  }

  /**
   * Read the list of ranges from the file.
   * @param file the file to read
   * @param base the base of the stripe
   * @param ranges the disk ranges within the stripe to read
   * @return the bytes read for each disk range, which is the same length as
   *    ranges
   * @throws IOException
   */
  static byte[][] readDiskRanges(FSDataInputStream file,
                                 long base,
                                 List<DiskRange> ranges) throws IOException {
    byte[][] result = new byte[ranges.size()][];
    int i = 0;
    for(DiskRange range: ranges) {
      int len = (int) (range.end - range.offset);
      result[i] = new byte[len];
      file.seek(base + range.offset);
      file.readFully(result[i]);
      i += 1;
    }
    return result;
  }

  /**
   * Does region A overlap region B? The end points are inclusive on both sides.
   * @param leftA A's left point
   * @param rightA A's right point
   * @param leftB B's left point
   * @param rightB B's right point
   * @return Does region A overlap region B?
   */
  static boolean overlap(long leftA, long rightA, long leftB, long rightB) {
    if (leftA <= leftB) {
      return rightA >= leftB;
    }
    return rightB >= leftA;
  }

  /**
   * Build a string representation of a list of disk ranges.
   * @param ranges ranges to stringify
   * @return the resulting string
   */
  static String stringifyDiskRanges(List<DiskRange> ranges) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("[");
    for(int i=0; i < ranges.size(); ++i) {
      if (i != 0) {
        buffer.append(", ");
      }
      buffer.append(ranges.get(i).toString());
    }
    buffer.append("]");
    return buffer.toString();
  }

  static void createStreams(List<OrcProto.Stream> streamDescriptions,
                            List<DiskRange> ranges,
                            byte[][] bytes,
                            boolean[] includeColumn,
                            CompressionCodec codec,
                            int bufferSize,
                            Map<StreamName, InStream> streams
                           ) throws IOException {
    long offset = 0;
    for(OrcProto.Stream streamDesc: streamDescriptions) {
      int column = streamDesc.getColumn();
      if (includeColumn[column] &&
          StreamName.getArea(streamDesc.getKind()) == StreamName.Area.DATA) {
        long length = streamDesc.getLength();
        int first = -1;
        int last = -2;
        for(int i=0; i < bytes.length; ++i) {
          DiskRange range = ranges.get(i);
          if (overlap(offset, offset+length, range.offset, range.end)) {
            if (first == -1) {
              first = i;
            }
            last = i;
          }
        }
        ByteBuffer[] buffers = new ByteBuffer[last - first + 1];
        long[] offsets = new long[last - first + 1];
        for(int i=0; i < buffers.length; ++i) {
          DiskRange range = ranges.get(i + first);
          long start = Math.max(range.offset, offset);
          long end = Math.min(range.end, offset+length);
          buffers[i] = ByteBuffer.wrap(bytes[first + i],
              Math.max(0, (int) (offset - range.offset)), (int) (end - start));
          offsets[i] = Math.max(0, range.offset - offset);
        }
        StreamName name = new StreamName(column, streamDesc.getKind());
        streams.put(name, InStream.create(name.toString(), buffers, offsets,
            length, codec, bufferSize));
      }
      offset += streamDesc.getLength();
    }
  }

  private void readPartialDataStreams(StripeInformation stripe
                                      ) throws IOException {
    List<OrcProto.Stream> streamList = stripeFooter.getStreamsList();
    List<DiskRange> chunks =
        planReadPartialDataStreams(streamList,
            indexes, included, includedRowGroups, codec != null,
            stripeFooter.getColumnsList(), types, bufferSize);
    if (LOG.isDebugEnabled()) {
      LOG.debug("chunks = " + stringifyDiskRanges(chunks));
    }
    mergeDiskRanges(chunks);
    if (LOG.isDebugEnabled()) {
      LOG.debug("merge = " + stringifyDiskRanges(chunks));
    }
    byte[][] bytes = readDiskRanges(file, stripe.getOffset(), chunks);
    createStreams(streamList, chunks, bytes, included, codec, bufferSize,
        streams);
  }

  @Override
  public boolean hasNext() throws IOException {
    return rowInStripe < rowCountInStripe;
  }

  /**
   * Read the next stripe until we find a row that we don't skip.
   * @throws IOException
   */
  private void advanceStripe() throws IOException {
    rowInStripe = rowCountInStripe;
    while (rowInStripe >= rowCountInStripe &&
        currentStripe < stripes.size() - 1) {
      currentStripe += 1;
      readStripe();
    }
  }

  /**
   * Skip over rows that we aren't selecting, so that the next row is
   * one that we will read.
   * @param nextRow the row we want to go to
   * @throws IOException
   */
  private void advanceToNextRow(long nextRow) throws IOException {
    long nextRowInStripe = nextRow - rowBaseInStripe;
    // check for row skipping
    if (rowIndexStride != 0 &&
        includedRowGroups != null &&
        nextRowInStripe < rowCountInStripe) {
      int rowGroup = (int) (nextRowInStripe / rowIndexStride);
      if (!includedRowGroups[rowGroup]) {
        while (rowGroup < includedRowGroups.length &&
               !includedRowGroups[rowGroup]) {
          rowGroup += 1;
        }
        // if we are off the end of the stripe, just move stripes
        if (rowGroup >= includedRowGroups.length) {
          advanceStripe();
          return;
        }
        nextRowInStripe = Math.min(rowCountInStripe, rowGroup * rowIndexStride);
      }
    }
    if (nextRowInStripe < rowCountInStripe) {
      if (nextRowInStripe != rowInStripe) {
        if (rowIndexStride != 0) {
          int rowGroup = (int) (nextRowInStripe / rowIndexStride);
          seekToRowEntry(rowGroup);
          reader.skipRows(nextRowInStripe - rowGroup * rowIndexStride);
        } else {
          reader.skipRows(nextRowInStripe - rowInStripe);
        }
        rowInStripe = nextRowInStripe;
      }
    } else {
      advanceStripe();
    }
  }

  @Override
  public Object next(Object previous) throws IOException {
    Object result = reader.next(previous);
    // find the next row
    rowInStripe += 1;
    advanceToNextRow(rowInStripe + rowBaseInStripe);
    return result;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  @Override
  public long getRowNumber() {
    return rowInStripe + rowBaseInStripe + firstRow;
  }

  /**
   * Return the fraction of rows that have been read from the selected.
   * section of the file
   * @return fraction between 0.0 and 1.0 of rows consumed
   */
  @Override
  public float getProgress() {
    return ((float) rowBaseInStripe + rowInStripe) / totalRowCount;
  }

  private int findStripe(long rowNumber) {
    for(int i=0; i < stripes.size(); i++) {
      StripeInformation stripe = stripes.get(i);
      if (stripe.getNumberOfRows() > rowNumber) {
        return i;
      }
      rowNumber -= stripe.getNumberOfRows();
    }
    throw new IllegalArgumentException("Seek after the end of reader range");
  }

  private void readRowIndex() throws IOException {
    long offset = stripes.get(currentStripe).getOffset();
    for(OrcProto.Stream stream: stripeFooter.getStreamsList()) {
      if (stream.getKind() == OrcProto.Stream.Kind.ROW_INDEX) {
        int col = stream.getColumn();
        if ((included == null || included[col]) && indexes[col] == null) {
          byte[] buffer = new byte[(int) stream.getLength()];
          file.seek(offset);
          file.readFully(buffer);
          indexes[col] = OrcProto.RowIndex.parseFrom(InStream.create("index",
              new ByteBuffer[] {ByteBuffer.wrap(buffer)}, new long[]{0},
              stream.getLength(), codec, bufferSize));
        }
      }
      offset += stream.getLength();
    }
  }

  private void seekToRowEntry(int rowEntry) throws IOException {
    PositionProvider[] index = new PositionProvider[indexes.length];
    for(int i=0; i < indexes.length; ++i) {
      if (indexes[i] != null) {
        index[i]=
            new PositionProviderImpl(indexes[i].getEntry(rowEntry));
      }
    }
    reader.seek(index);
  }

  @Override
  public void seekToRow(long rowNumber) throws IOException {
    if (rowNumber < 0) {
      throw new IllegalArgumentException("Seek to a negative row number " +
                                         rowNumber);
    } else if (rowNumber < firstRow) {
      throw new IllegalArgumentException("Seek before reader range " +
                                         rowNumber);
    }
    // convert to our internal form (rows from the beginning of slice)
    rowNumber -= firstRow;

    // move to the right stripe
    int rightStripe = findStripe(rowNumber);
    if (rightStripe != currentStripe) {
      currentStripe = rightStripe;
      readStripe();
    }
    readRowIndex();

    // if we aren't to the right row yet, advanance in the stripe.
    advanceToNextRow(rowNumber);
  }
}
