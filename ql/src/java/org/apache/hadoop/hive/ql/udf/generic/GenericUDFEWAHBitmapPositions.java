package org.apache.hadoop.hive.ql.udf.generic;

import javaewah.EWAHCompressedBitmap;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectInput;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by samadhik on 1/5/17.
 */
@Description(name = "ewah_bitmap_positions",
        value = "_FUNC_(b1) - Return a collection of positions used to create the bitmap")
public class GenericUDFEWAHBitmapPositions extends GenericUDF{
    static final Logger LOG = LoggerFactory.getLogger(GenericUDFEWAHBitmapPositions.class.getName());

    private transient ObjectInspector bitmapOI;
    private transient ObjectInspector listOI;
    ArrayList<LongWritable> buffer = new ArrayList<LongWritable>();

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
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                        .writableLongObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 1);
        Object b = arguments[0].get();

        ListObjectInspector lloi = (ListObjectInspector) bitmapOI;
        int length = lloi.getListLength(b);
        ArrayList<LongWritable> bitmapArray = new ArrayList<LongWritable>();
        for (int i = 0; i < length; i++) {
            long l = PrimitiveObjectInspectorUtils.getLong(
                    lloi.getListElement(b, i),
                    (PrimitiveObjectInspector) lloi.getListElementObjectInspector());
            bitmapArray.add(new LongWritable(l));
        }

        BitmapObjectInput bitmapObjIn = new BitmapObjectInput(bitmapArray);
        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        //EWAHCompressedBitmap a = new EWAHCompressedBitmap();
        try {
            bitmap.readExternal(bitmapObjIn);  //WASam edit was to comment this
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List a1=bitmap.getPositions();
        LOG.info("SAM index positions       --------------------------------------------");
        LOG.info("\t " + a1);

        // String s="\t " + a1;
        for(Object ob : a1){
            if(ob instanceof Integer)
                buffer.add(new LongWritable(Long.parseLong(ob.toString())));

        }

        return  buffer;
    }


    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("EWAH_BITMAP_EMPTY", children);
    }
}
