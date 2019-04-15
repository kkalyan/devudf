package com.kkalyan.devudf.ip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Generate a bag of IPs from start to end
 */

public class ExpandIpRanges extends EvalFunc<DataBag> {

    @Override
    public DataBag exec(Tuple tuple) throws IOException {

        String startIp = (String) tuple.get(0);
        String endIp = (String) tuple.get(1);
        DataBag output = BagFactory.getInstance().newDefaultBag();
        List<String> ips = getIps(startIp, endIp);
        for (String ip : ips) {
            output.add(TupleFactory.getInstance().newTuple(ip));
        }
        return output;
    }

    public static List<String> getIps(String startIp, String endIp) {
        long start = IpUtil.convertToLongFromStrIP(startIp);
        long end = IpUtil.convertToLongFromStrIP(endIp);
        List<String> output = new ArrayList<>();
        output.add(startIp);
        for (long i = start + 1; i < end; i++) {
            output.add(IpUtil.convertToStrIPFromLong(i));
        }
        output.add(endIp);
        return output;
    }
    
    
}
