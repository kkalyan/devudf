package com.kkalyan.devudf.dc;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 *
 * UDF that takes a bag and returns another bag by looking up every key into a
 * lookup file
 */
public class DistributedLookupBag extends EvalFunc<DataBag> {

    private DistributedLookupUtil dc;

    public DistributedLookupBag(String path, String separator, String lookupIndex, String outputIndex) {
        this.dc = new DistributedLookupUtil(path, separator, lookupIndex, outputIndex);
    }

    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        DataBag outputBag = BagFactory.getInstance().newDefaultBag();
        try {
            DataBag inputBag = (DataBag) tuple.get(0);
            for (Tuple t : inputBag) {
                String key = (String) t.get(0);
                String value = dc.get(key);
                Tuple ot = TupleFactory.getInstance().newTuple(1);
                ot.set(0, value);
                outputBag.add(ot);
            }
            return outputBag;
        } catch (Exception ex) {
            return outputBag;
        }
    }

}
