package du00.tests;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.*;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

public class WordCountTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCount.Map mapper = new WordCount.Map();
        WordCount.Reduce reducer = new WordCount.Reduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("a b a"))
                .withAllOutput(Lists.newArrayList(
                        new Pair<Text, IntWritable>(new Text("a"), new IntWritable(1)),
                        new Pair<Text, IntWritable>(new Text("b"), new IntWritable(1)),
                        new Pair<Text, IntWritable>(new Text("a"), new IntWritable(1))
                ))
                .runTest();
    }

    /**
     * 有时候结果会比较复杂，取出来抽取结果的一部分比较会是比较好的选择。比如对象的某个字段是double类型的。
     *
     * @throws IOException
     */
    @Test
    public void testMpper2() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "a b a"));
        List<Pair<Text, IntWritable>> actual = mapDriver.run();

        List<Pair<Text, IntWritable>> expected = Lists.newArrayList(
                new Pair<Text, IntWritable>(new Text("a"), new IntWritable(1)),
                new Pair<Text, IntWritable>(new Text("b"), new IntWritable(1)),
                new Pair<Text, IntWritable>(new Text("a"), new IntWritable(1))
        );

        // apache commons-collection: 判断元素相等，考虑了每个元素的频次
        assertTrue(CollectionUtils.isEqualCollection(actual, expected));

        assertEquals(actual.get(0).getSecond().get(), 1);
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = Lists.newArrayList();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("a"), values);
        reduceDriver.withOutput(new Text("a"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("a b a"))
                .withInput(new LongWritable(), new Text("a b b"))
                .withAllOutput(Lists.newArrayList(
                        new Pair<Text, IntWritable>(new Text("a"), new IntWritable(3)),
                        new Pair<Text, IntWritable>(new Text("b"), new IntWritable(3))))
                .runTest();
    }
}
