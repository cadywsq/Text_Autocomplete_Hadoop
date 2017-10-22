import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * @author Siqi Wang siqiw1 on 4/8/16.
 */
public class AutoComplete {
    public static class AutoMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<KVPair> kvPairs = formatKVpair(value.toString());
            for (KVPair kvPair : kvPairs) {
                context.write(new Text(kvPair.getKey()), new Text(kvPair.getValue()));
            }
        }
    }

    public static class AutoReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<KVPair> pairs = new ArrayList<>();
            for (Text value : values) {
                String[] tokens = value.toString().split("\\|");
                String word = tokens[0];
                int count = Integer.valueOf(tokens[1]);
                pairs.add(new KVPair(key.toString(), word, count));
            }
            Collections.sort(pairs);
            Put put = new Put(Bytes.toBytes(key.toString()));
            for (int i = 0; i < 5 && i < pairs.size(); i++) {
                KVPair kvPair = pairs.get(i);
                put.add(Bytes.toBytes("data"), Bytes.toBytes(kvPair.getWord()), Bytes.toBytes(Integer
                        .toString(kvPair.getCount())));
            }
            context.write(new ImmutableBytesWritable(key.getBytes()), put);
        }
    }

    public static void main(String[] args) throws Exception {
        final String TABLE_NAME = "auto";
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config, "Auto_complete");
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(AutoComplete.class);
        job.setMapperClass(AutoMapper.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TableMapReduceUtil.initTableReducerJob(TABLE_NAME, AutoReducer.class, job);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static ArrayList<KVPair> formatKVpair(String line) {
        String[] tokens = line.split("\t");
        char[] chars = tokens[0].toCharArray();
        ArrayList<KVPair> kvPairs = new ArrayList<>();
        String key = "";
        for (int i = 0; i < chars.length - 1; i++) {
            key += chars[i];
            KVPair kvPair = new KVPair(key, tokens[0] + "|" + tokens[1]);
            kvPairs.add(kvPair);
        }
        return kvPairs;
    }

    public static class KVPair implements Comparable<KVPair> {
        private String key;
        private String value;
        private int count;
        private String word;

        public KVPair(String key, String value) {
            this.setKey(key);
            this.setValue(value);
        }

        public KVPair(String key, String word, int count) {
            this.setKey(key);
            this.setWord(word);
            this.setCount(count);
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        @Override
        public int compareTo(KVPair o) {
            if (this.getCount() != o.getCount()) {
                return o.getCount() - this.getCount();
            }
            return this.getWord().compareTo(o.getWord());
        }
    }
}
