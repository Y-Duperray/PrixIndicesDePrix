import Writer.FileWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FileWriterTest {
    String outputPathStr = "";
    FileWriter fileWriter = new FileWriter(outputPathStr);
    SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

    @Test
    public void test(){
        List<Float> floatList = Arrays.asList((float)1, (float)2, (float)3, (float)4, (float)5);
        Dataset<Float> input = sparkSession.createDataset(floatList, Encoders.FLOAT());
        List<String> expected = Arrays.asList(String.valueOf(1), String.valueOf(2), String.valueOf(3), String.valueOf(4), String.valueOf(5));
        fileWriter.accept(input);
        try{
            List<String> actual = Files.readAllLines(Paths.get(outputPathStr+"resultat.csv"));
            assertThat(actual).isEqualTo(expected);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
