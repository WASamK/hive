package org.apache.hadoop.hive.ql.udf.generic;

        import javaewah.EWAHCompressedBitmap;
        import org.apache.hadoop.hive.ql.exec.Description;
        import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
        import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
        import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
        import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectInput;
        import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectOutput;
        import org.apache.hadoop.hive.ql.metadata.HiveException;
        import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
        import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
        import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
        import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
        import org.apache.hadoop.io.LongWritable;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import java.io.IOException;
        import java.util.ArrayList;
        import java.util.List;

/**
 * Created by samadhik on 1/12/17.
 */
@Description(name = "ewah_bit_slices",
        value = "_FUNC_(b1) - Return a lis of bitslices")
public class GenericUDFEWAHBitSlices  extends GenericUDF{
    static final Logger LOG = LoggerFactory.getLogger(GenericUDFEWAHBitmapPositions.class.getName());

    private transient ObjectInspector bitmapOI;
    private transient ListObjectInspector bitmapList;
    protected final ArrayList<Object> ret = new ArrayList<Object>();
    private static int nu=0;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "The function EWAH_BITMAP_POSITIONS(b) takes exactly 1 argument");
        }

        if (arguments[0].getCategory().equals(ObjectInspector.Category.LIST)) {
            bitmapOI = (ListObjectInspector) arguments[0];

        } else {
            throw new UDFArgumentTypeException(0, "\""
                    + ObjectInspector.Category.LIST.toString().toLowerCase()
                    + "\" is expected at function EWAH_BITMAP_POSITIONS, but \""
                    + arguments[0].getTypeName() + "\" is found");
        }

        return ObjectInspectorFactory
                .getStandardListObjectInspector(ObjectInspectorFactory
                        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                                .writableLongObjectInspector));
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 1);
        Object b = arguments[0].get();

        ListObjectInspector lloi = (ListObjectInspector) bitmapOI;
        int length = lloi.getListLength(b);

        for (int i = 0; i < length; i++) {
            LongWritable element=(LongWritable)lloi.getListElement(b, i);
            LOG.info(element.toString());

        }


        return null;

    }

    protected List<LongWritable> bitmapToWordArray(EWAHCompressedBitmap bitmap) {
        BitmapObjectOutput bitmapObjOut = new BitmapObjectOutput();
        try {
            bitmap.writeExternal(bitmapObjOut);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bitmapObjOut.list();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("EWAH_BITMAP_COMPOUNDAND", children);
    }
}
