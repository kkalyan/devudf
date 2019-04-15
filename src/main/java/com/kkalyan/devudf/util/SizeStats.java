package com.kkalyan.devudf.util;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * Give a JSON input file, and spec to group the fields, returns a tuple of
 * sizes on disk
 */
public class SizeStats extends EvalFunc<Tuple> {

    Map<String, List<String>> specGroups;
    List<String> includeFields;
    Gson gson = new Gson();

    public SizeStats(String spec, String includeFields) {
        this.includeFields = Arrays.asList(includeFields.split(","));
        String[] groups = spec.split(";");
        specGroups = new HashMap<>();
        for (String group : groups) {
            String[] fields = group.split("=");
            String key = fields[0];
            String[] values = fields[1].split(",");
            specGroups.put(key, Arrays.asList(values));

        }
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        String json = (String) input.get(0);
        JsonParser parser = new JsonParser();
        JsonObject profile = parser.parse(json).getAsJsonObject();
        //groups, total_bytes and include fields
        Tuple outputTuple = TupleFactory.getInstance().newTuple(2 + includeFields.size());
        Iterator<Entry<String, JsonElement>> it = profile.entrySet().iterator();
        Set<String> allKeys = new HashSet<>();
        while (it.hasNext()) {
                Entry entry = it.next();
                allKeys.add((String) entry.getKey());
        }
        Long totalSize = new Long(json.getBytes().length);
        DataBag groups = BagFactory.getInstance().newDefaultBag();
        for (String group : specGroups.keySet()) {
                List<String> groupObj = specGroups.get(group);
                for (String field : groupObj) {
                    Long fieldSize = getSize(profile, field);
                    if (fieldSize == 0) {
                        continue;
                    }
                    Tuple groupTuple = TupleFactory.getInstance().newTuple(3);
                    groupTuple.set(0, group);
                    groupTuple.set(1, field);
                    groupTuple.set(2, fieldSize);
                    groups.add(groupTuple);
                    allKeys.remove(field);
                }
            }
        String groupName="remainingData";
            for (String field : allKeys) {
                Tuple groupTuple = TupleFactory.getInstance().newTuple(3);
                groupTuple.set(0, groupName);
                groupTuple.set(1, field);
                Long size = getSize(profile, field);
                if (size == 0) {
                    continue;
                }
                groupTuple.set(2, getSize(profile, field));
                groups.add(groupTuple);
            }
           outputTuple.set(0, groups);
           outputTuple.set(1, totalSize);
           int i=2;
           for(String key: includeFields){
                outputTuple.set(i, profile.get(key).getAsString());
                i++;
           } 
           return outputTuple;
    }

    private Long getSize(JsonObject profile, String key) {
        if (profile.has(key)) {
            return new Long(gson.toJson(profile.get(key)).getBytes().length);
        } else {
            return 0L;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema stats = new Schema();
            Schema groups = new Schema();
            groups.add(new Schema.FieldSchema("group_name", DataType.CHARARRAY));
            groups.add(new Schema.FieldSchema("field_name", DataType.CHARARRAY));
            groups.add(new Schema.FieldSchema("bytes", DataType.LONG));
            stats.add(new Schema.FieldSchema("groups", groups, DataType.BAG));
            groups.add(new Schema.FieldSchema("total_bytes", DataType.LONG));
            for (String field : includeFields) {
                stats.add(new Schema.FieldSchema(field, groups, DataType.CHARARRAY));
            }
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(),
                    input), stats, DataType.TUPLE));

        } catch (Exception e) {
            return null;
        }
    }
}
