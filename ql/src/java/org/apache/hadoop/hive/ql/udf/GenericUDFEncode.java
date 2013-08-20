package org.apache.hadoop.hive.ql.udf;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

@Description(name = "encode",
value = "_FUNC_(str, str) - Encode the first argument using the second argument character set",
extended = "Possible options for the character set are 'US_ASCII', 'ISO-8859-1',\n" +
    "'UTF-8', 'UTF-16BE', 'UTF-16LE', and 'UTF-16'. If either argument\n" +
    "is null, the result will also be null")
public class GenericUDFEncode extends GenericUDF {
  private transient CharsetEncoder encoder = null;
  private transient StringObjectInspector stringOI = null;
  private transient StringObjectInspector charsetOI = null;
  private transient BytesWritable result = new BytesWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("Encode() requires exactly two arguments");
    }

    if (arguments[0].getCategory() != Category.PRIMITIVE ||
        ((PrimitiveObjectInspector)arguments[0]).getPrimitiveCategory() != PrimitiveCategory.STRING){
      throw new UDFArgumentTypeException(0, "The first argument to Encode() must be a string");
    }

    stringOI = (StringObjectInspector) arguments[0];

    if (arguments[1].getCategory() != Category.PRIMITIVE ||
        ((PrimitiveObjectInspector)arguments[1]).getPrimitiveCategory() != PrimitiveCategory.STRING){
      throw new UDFArgumentTypeException(1, "The second argument to Encode() must be a string");
    }

    charsetOI = (StringObjectInspector) arguments[1];

    // If the character set for encoding is constant, we can optimize that
    StringObjectInspector charSetOI = (StringObjectInspector) arguments[1];
    if (charSetOI instanceof ConstantObjectInspector){
      String charSetName = ((Text) ((ConstantObjectInspector) charSetOI).getWritableConstantValue()).toString();
      encoder = Charset.forName(charSetName).newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
    }

    result = new BytesWritable();

    return (ObjectInspector) PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String value = stringOI.getPrimitiveJavaObject(arguments[0].get());
    if (value == null) {
      return null;
    }

    ByteBuffer encoded;
    if (encoder != null){
      try {
        encoded = encoder.encode(CharBuffer.wrap(value));
      } catch (CharacterCodingException e) {
        throw new HiveException(e);
      }
    } else {
      encoded = Charset.forName(charsetOI.getPrimitiveJavaObject(arguments[1].get())).encode(value);
    }
    result.setSize(encoded.limit());
    encoded.get(result.getBytes(), 0, encoded.limit());
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    StringBuilder sb = new StringBuilder();
    sb.append("encode(");
    sb.append(children[0]).append(",");
    sb.append(children[1]).append(")");
    return sb.toString();
  }
}
