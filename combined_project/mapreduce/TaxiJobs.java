import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Batch layer: two MapReduce jobs over the cleaned trips Spark wrote to HDFS.
 *
 *   zone <in> <out>   key = PU_Borough, PU_Zone      -> trips, revenue
 *   hour <in> <out>   key = PU_Borough, pickup_hour  -> trips, revenue
 *
 * Input is tab-separated, in the column order written by spark/stream_to_hdfs.py.
 * Money is summed in integer cents so that totals do not depend on the order of addition,
 * which also makes the results directly comparable with Spark's.
 */
public class TaxiJobs {
    // Column positions in the HDFS files
    static final int PU_BOROUGH = 0, PU_ZONE = 1, PICKUP_HOUR = 4, TOTAL_AMOUNT = 8, N_COLS = 9;

    public enum Quality { RECORDS, MALFORMED }

    public static class TripMapper extends Mapper<LongWritable, Text, Text, Text> {
        private boolean byZone;
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context ctx) {
            byZone = "zone".equals(ctx.getConfiguration().get("taxi.job"));
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split("\t", -1);
            if (f.length != N_COLS) {
                ctx.getCounter(Quality.MALFORMED).increment(1);
                return;
            }
            long cents;
            try {
                cents = Math.round(Double.parseDouble(f[TOTAL_AMOUNT]) * 100);
            } catch (NumberFormatException e) {
                ctx.getCounter(Quality.MALFORMED).increment(1);
                return;
            }
            ctx.getCounter(Quality.RECORDS).increment(1);
            outKey.set(f[PU_BOROUGH] + "\t" + (byZone ? f[PU_ZONE] : f[PICKUP_HOUR]));
            outVal.set("1," + cents);          // emit (key, (1 trip, cents))
            ctx.write(outKey, outVal);
        }
    }

    /** Adds up (trips, cents). The operation is associative, so the same class also serves as the combiner. */
    public static class SumCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            long trips = 0, cents = 0;
            for (Text v : values) {
                String[] p = v.toString().split(",");
                trips += Long.parseLong(p[0]);
                cents += Long.parseLong(p[1]);
            }
            outVal.set(trips + "," + cents);
            ctx.write(key, outVal);
        }
    }

    /** Final output: key, trips, revenue in dollars. */
    public static class SumReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            long trips = 0, cents = 0;
            for (Text v : values) {
                String[] p = v.toString().split(",");
                trips += Long.parseLong(p[0]);
                cents += Long.parseLong(p[1]);
            }
            outVal.set(trips + "\t" + String.format("%d.%02d", cents / 100, Math.abs(cents % 100)));
            ctx.write(key, outVal);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !(args[0].equals("zone") || args[0].equals("hour"))) {
            System.err.println("usage: TaxiJobs zone|hour <input> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        conf.set("taxi.job", args[0]);
        Job job = Job.getInstance(conf, "taxi-" + args[0]);
        job.setJarByClass(TaxiJobs.class);
        job.setMapperClass(TripMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
