package com.bda.sanitizer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Collapses records that share an Employee_ID. The mapper keys every valid
 * record by its Employee_ID, so all copies of one ID arrive together here.
 * We keep the first and drop the rest, counting the drops.
 *
 * Output is the cleaned record only (NullWritable value) so the final files
 * are plain CSV with no trailing tab/key artefact.
 */
public class DataSanitizerReducer
        extends Reducer<Text, Text, Text, NullWritable> {

    public enum Counters {
        UNIQUE_CLEAN_RECORDS,
        DUPLICATE_RECORDS_DROPPED
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Iterator<Text> it = values.iterator();

        // Keep the first record for this Employee_ID.
        if (it.hasNext()) {
            Text first = it.next();
            context.write(new Text(first.toString()), NullWritable.get());
            context.getCounter(DataSanitizerMapper.GROUP,
                    Counters.UNIQUE_CLEAN_RECORDS.name()).increment(1);
        }

        // Every further record with this key is a duplicate.
        while (it.hasNext()) {
            it.next();
            context.getCounter(DataSanitizerMapper.GROUP,
                    Counters.DUPLICATE_RECORDS_DROPPED.name()).increment(1);
        }
    }
}
