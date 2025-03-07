package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable {
    
    // Required constructor
    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    public DoubleArrayWritable(double[] values) {
        super(DoubleWritable.class);
        DoubleWritable[] doubles = new DoubleWritable[values.length];
        for(int i = 0; i < values.length; i++) {
            doubles[i] = new DoubleWritable(values[i]);
        }
        set(doubles);
    }

    @Override
    public DoubleWritable[] get() {
        Writable[] writables = super.get();
        DoubleWritable[] doubles = new DoubleWritable[writables.length];
        
        // Safe type conversion
        for(int i = 0; i < writables.length; i++) {
            doubles[i] = (DoubleWritable) writables[i];
        }
        return doubles;
    }

    @Override
    public String toString() {
        DoubleWritable[] values = get();
        StringBuilder sb = new StringBuilder();
        for(DoubleWritable dw : values) {
            sb.append(dw.get()).append(",");
        }
        return sb.substring(0, sb.length() > 0 ? sb.length()-1 : 0);
    }
}

