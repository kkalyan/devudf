package com.kkalyan.devudf.util;

import java.io.IOException;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Algebraic UDF takes BAG generated from GROUP BY and returns N records.
 *
 * Following code data = FOREACH input GENERATE key,val1,val2; grouped = data by
 * (key); sample_data = foreach grouped { key_records = FOREACH data generate
 * val1; sample_records = LIMIT key_records 10; GENERATE group as key,
 * sample_records;
 *
 * can be rewritten as sample_data = foreach grouped group as key,
 * LimitN(data.val1);
 *
 */
public class LimitN extends EvalFunc<DataBag> implements Algebraic {

    //records will limited to this number
    private int maxRecords;
    public static String DEFAULT_MAX = "5";

    public LimitN(String maxRecordsString) {
        //pig udfs can't have integer parameters, only strings hence parseInt 
        this.maxRecords = Integer.parseInt(maxRecordsString);
    }

    public LimitN() {
        //default constructor is required
        this(DEFAULT_MAX);
    }

    public DataBag exec(Tuple tuple) throws IOException {
        return limitN(tuple, maxRecords);
    }

    @Override
    public String getInitial() {
        return Initial.class.getName();
    }

    @Override
    public String getIntermed() {
        return Intermediate.class.getName();
    }

    @Override
    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {

        //need to match signatures of parent UDF
        public Initial(String maxRecordsString) {
        }

        //default constructor is required 
        public Initial() {

        }

        @Override
        public Tuple exec(Tuple input) throws IOException {
            //Initial will get only one record 
            return input;
        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {

        private int maxRecords;

        public Intermediate(String maxRecordsString) {
            this.maxRecords = Integer.parseInt(maxRecordsString);
        }

        public Intermediate() {

        }

        @Override
        public Tuple exec(Tuple input) throws IOException {
            DataBag outputBag = BagFactory.getInstance().newDefaultBag();
            DataBag bag = (DataBag) input.get(0);
            int i = 0;
            for (Tuple tuple : bag) {
                for (Tuple t : (DataBag) tuple.get(0)) {
                    i++;
                    if (i >= maxRecords) {
                        break;
                    }
                    outputBag.add(t);
                }
                if (i > maxRecords) {
                    break;
                }
            }
            return TupleFactory.getInstance().newTuple(outputBag);
        }
    }

    static public class Final extends EvalFunc<DataBag> {

        private int maxRecords;

        public Final(String maxRecordsString) {
            this.maxRecords = Integer.parseInt(maxRecordsString);
        }

        public Final() {

        }

        @Override
        public DataBag exec(Tuple input) throws IOException {
            return limitN(input, maxRecords);
        }
    }

    public static DataBag limitN(Tuple input, int maxRecords) throws IOException {
        DataBag records = (DataBag) input.get(0);
        int i = 0;
        DataBag outputBag = BagFactory.getInstance().newDefaultBag();
        for (Tuple tuple : records) {
            Object item = tuple.get(0);
            //if there is only item in the bag, Final method gets String else Bag
            if (item instanceof String) {
                outputBag.add(TupleFactory.getInstance().newTuple((String) item));
                i++;
                if (i >= maxRecords) {
                    return outputBag;
                }
            } else if (item instanceof DataBag) {
                for (Tuple innerTuple : (DataBag) tuple.get(0)) {
                    outputBag.add(innerTuple);
                    i++;
                    if (i >= maxRecords) {
                        return outputBag;
                    }
                }
            }
        }
        return outputBag;

    }

    public Schema outputSchema(Schema inputSchema) {
        return inputSchema;
    }

}
