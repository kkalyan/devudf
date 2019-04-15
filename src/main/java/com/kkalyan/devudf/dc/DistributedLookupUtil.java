
package com.kkalyan.devudf.dc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class DistributedLookupUtil {
    private String path;
    private String separator;
    private int lookupIndex, outputIndex;
    private HashMap<String, String> map;
    
     public DistributedLookupUtil(String path, String separator, String lookupIndex, String outputIndex) {
        this.path = path;
        this.lookupIndex = Integer.parseInt(lookupIndex);
        this.outputIndex = Integer.parseInt(outputIndex);
    }
     
     public List<String> getCacheFiles() {
        ArrayList<String> list = new ArrayList<>(1);
        list.add(path + "#" + path.substring(path.lastIndexOf('/') + 1));
        return list;
    }
     
     public void load() {
        if (map == null) {
            synchronized (DistributedLookup.class) {
                if (map == null) {
                    init();
                }
            }
        }
    }

    public void init() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            String line;
            while ((line = br.readLine()) != null) {
                String fs[] = line.split(separator);
                if (fs.length < lookupIndex || fs.length < outputIndex) {
                    continue;
                }
                String key = fs[lookupIndex];
                String value = fs[outputIndex];
                map.put(key, value);
            }
        } catch (Exception ex) {
            Logger.getLogger(DistributedLookupUtil.class.getName()).log(Level.SEVERE, "error in loading " + path, ex);
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ex1) {
                    Logger.getLogger(DistributedLookupUtil.class.getName()).log(Level.SEVERE, "error in closing", ex1);
                }
            }
        }
    }
    
    public String get(String key){
        load();
        return map.getOrDefault(key, key);
    }
}
