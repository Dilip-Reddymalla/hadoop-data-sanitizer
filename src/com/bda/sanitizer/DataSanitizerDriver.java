package com.bda.sanitizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver for the Data Sanitizer MapReduce job.
 *
 *   hadoop jar data-sanitizer.jar com.bda.sanitizer.DataSanitizerDriver \
 *          &lt;input path&gt; &lt;output path&gt; [numReducers]
 *
 * Filters invalid records out of a messy HR employee CSV and writes the
 * surviving, normalised, de-duplicated records to the output path. On success it
 * prints a sanitisation report built from the job counters, including three
 * integrity checks that must all reconcile.
 */
public class DataSanitizerDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: DataSanitizerDriver <input> <output> [numReducers]");
            return 2;
        }

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int reducers = (args.length >= 3) ? Integer.parseInt(args[2]) : 1;

        Configuration conf = getConf();

        // Remove a previous output directory so reruns are painless.
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            System.out.println("Removing existing output path: " + output);
            fs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "Data Sanitizer - HR employee record filter");
        job.setJarByClass(DataSanitizerDriver.class);

        job.setMapperClass(DataSanitizerMapper.class);
        job.setReducerClass(DataSanitizerReducer.class);
        job.setNumReduceTasks(reducers);

        // Mapper emits (Employee_ID -> record); reducer emits (record -> nothing).
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // NOTE: no combiner. The reducer is not associative in the way a
        // combiner needs - it drops duplicates and counts them, so running it
        // map-side would double-count DUPLICATE_RECORDS_DROPPED.

        System.out.println("Input   : " + input);
        System.out.println("Output  : " + output);
        System.out.println("Reducers: " + reducers);

        long t0 = System.currentTimeMillis();
        boolean ok = job.waitForCompletion(true);
        long elapsed = System.currentTimeMillis() - t0;

        if (ok) {
            printReport(job, elapsed);
        }
        return ok ? 0 : 1;
    }

    private void printReport(Job job, long elapsedMs) throws Exception {
        CounterGroup g = job.getCounters().getGroup(DataSanitizerMapper.GROUP);

        long total   = value(g, DataSanitizerMapper.Counters.TOTAL_RECORDS.name());
        long valid   = value(g, DataSanitizerMapper.Counters.VALID_RECORDS.name());
        long invalid = value(g, DataSanitizerMapper.Counters.INVALID_RECORDS.name());
        long unique  = value(g, DataSanitizerReducer.Counters.UNIQUE_CLEAN_RECORDS.name());
        long dupes   = value(g, DataSanitizerReducer.Counters.DUPLICATE_RECORDS_DROPPED.name());
        long quoted  = value(g, DataSanitizerMapper.Counters.QUOTED_FIELD_RECORDS.name());

        System.out.println();
        System.out.println("============================================================");
        System.out.println("               DATA SANITIZATION REPORT");
        System.out.println("============================================================");
        System.out.printf("  Records read (excl. header)  : %,d%n", total);
        System.out.printf("  VALID (passed all 7 rules)   : %,d  (%s)%n",
                valid, pct(valid, total));
        System.out.printf("  INVALID (discarded)          : %,d  (%s)%n",
                invalid, pct(invalid, total));
        System.out.println("------------------------------------------------------------");
        System.out.println("  Rejection reasons (first failing rule wins):");
        report(g, "  1 malformed / wrong columns",
                DataSanitizerMapper.Counters.MALFORMED_RECORDS.name(), total);
        report(g, "  2 blank required field",
                DataSanitizerMapper.Counters.BLANK_FIELD_RECORDS.name(), total);
        report(g, "  3 bad Employee_ID format",
                DataSanitizerMapper.Counters.INVALID_ID_FORMAT_RECORDS.name(), total);
        report(g, "  4 invalid / inconsistent date",
                DataSanitizerMapper.Counters.INVALID_DATE_RECORDS.name(), total);
        report(g, "  5 value outside category set",
                DataSanitizerMapper.Counters.INVALID_CATEGORY_RECORDS.name(), total);
        report(g, "  6 numeric out of range",
                DataSanitizerMapper.Counters.INVALID_NUMERIC_RECORDS.name(), total);
        report(g, "  7 logical inconsistency",
                DataSanitizerMapper.Counters.LOGICAL_INCONSISTENCY_RECORDS.name(), total);
        System.out.println("------------------------------------------------------------");
        System.out.printf("  Quote-aware parses (rescued) : %,d%n", quoted);
        System.out.printf("  Duplicate Employee_ID dropped: %,d%n", dupes);
        System.out.printf("  FINAL unique clean records   : %,d%n", unique);
        System.out.printf("  Wall-clock (driver-measured) : %.1f s%n", elapsedMs / 1000.0);
        System.out.println("------------------------------------------------------------");

        // Integrity checks: the counters must reconcile.
        long reasons =
              value(g, DataSanitizerMapper.Counters.MALFORMED_RECORDS.name())
            + value(g, DataSanitizerMapper.Counters.BLANK_FIELD_RECORDS.name())
            + value(g, DataSanitizerMapper.Counters.INVALID_ID_FORMAT_RECORDS.name())
            + value(g, DataSanitizerMapper.Counters.INVALID_DATE_RECORDS.name())
            + value(g, DataSanitizerMapper.Counters.INVALID_CATEGORY_RECORDS.name())
            + value(g, DataSanitizerMapper.Counters.INVALID_NUMERIC_RECORDS.name())
            + value(g, DataSanitizerMapper.Counters.LOGICAL_INCONSISTENCY_RECORDS.name());

        System.out.printf("  CHECK valid + invalid == read : %s (%,d + %,d = %,d)%n",
                (valid + invalid == total) ? "PASS" : "FAIL", valid, invalid, total);
        System.out.printf("  CHECK reasons == invalid      : %s (%,d vs %,d)%n",
                (reasons == invalid) ? "PASS" : "FAIL", reasons, invalid);
        System.out.printf("  CHECK unique + dupes == valid : %s (%,d + %,d = %,d)%n",
                (unique + dupes == valid) ? "PASS" : "FAIL", unique, dupes, valid);
        System.out.println("============================================================");
    }

    private static long value(CounterGroup g, String name) {
        Counter c = g.findCounter(name);
        return (c == null) ? 0L : c.getValue();
    }

    private static void report(CounterGroup g, String label, String name, long total) {
        long v = value(g, name);
        System.out.printf("  %-30s : %,9d  (%s)%n", label, v, pct(v, total));
    }

    private static String pct(long n, long total) {
        if (total <= 0) {
            return "0.00%";
        }
        return String.format("%.2f%%", (100.0 * n) / total);
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Configuration(), new DataSanitizerDriver(), args);
        System.exit(rc);
    }
}
