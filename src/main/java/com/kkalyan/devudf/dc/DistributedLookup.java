package com.kkalyan.devudf.dc;

import java.io.IOException;
import java.util.List;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * UDF takes TSV files and lookups the given key based on index values
 */
public class DistributedLookup extends EvalFunc<String> {

    DistributedLookupUtil dc;

    public DistributedLookup(String path, String separator, String lookupIndex, String outputIndex) {
        this.dc = new DistributedLookupUtil(path, separator, lookupIndex, outputIndex);
    }

    public List<String> getCacheFiles() {
        return dc.getCacheFiles();
    }
  
    @Override
    public String exec(Tuple inputTuple) throws IOException {
        String key = (String) inputTuple.get(0);
        return dc.get(key);
    }

}
