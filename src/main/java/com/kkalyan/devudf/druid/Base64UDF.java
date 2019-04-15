
package com.kkalyan.devudf.druid;


import java.io.IOException;
import java.util.Base64;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * UDF that converts bytearray to base64 string
 * used for storing Sketches for loading into Druid
 */
public class Base64UDF extends EvalFunc<String>{

    public String exec(Tuple tuple)
            throws IOException
    {
        if(tuple == null || tuple.size() == 0 || tuple.get(0)==null)
            return null;
        try
        {
            DataByteArray databytearray = (DataByteArray)tuple.get(0);
            return Base64.getEncoder().encodeToString(databytearray.get());
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
            return null;
        }
    }
}