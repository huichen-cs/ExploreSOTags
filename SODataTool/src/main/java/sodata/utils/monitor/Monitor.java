package sodata.utils.monitor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitor {
    private static Logger logger = LoggerFactory.getLogger(Monitor.class);
    
    private final static long MEGABYTES = 0x100000;
    private String csv_fn;
    private PrintWriter out;
    private Runtime runtime;

    public Monitor(String fn) {
        runtime = Runtime.getRuntime();
        this.csv_fn = fn + "_" 
                + (new SimpleDateFormat("yyyyMMdd_HHmmss")).format(new Date()) + ".csv";
        try {
            out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(this.csv_fn), StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("Cannot create file for writing monitor statistics.", e);
            out = null;
        }
        
        if (out != null) {
            logger.info("Monitor created.");
        }
    }
    
    public void cleanup() {
        if (out != null) {
            out.close();
            out = null;
        }        
    }
    
    public void write(long workDone) {
        double usedMemoryInMB = (double)(runtime.totalMemory() - runtime.freeMemory()) / (double)MEGABYTES;
        if (out != null) {
            out.println(workDone + ", " + usedMemoryInMB);
            out.flush();
            logger.info("WorkDone = " + workDone + " Memory = " + (long)usedMemoryInMB + " MB");
        }
    }    
}
