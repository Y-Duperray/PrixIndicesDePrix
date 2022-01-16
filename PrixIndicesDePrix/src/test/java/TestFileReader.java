import functions.reader.FileReader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFileReader {
    @Test
    public void test() {
        String inputFile ="src/test/valeurs_test.csv";
        String outputDirectory ="target/test-classes";

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        FileReader fileReader = new FileReader(inputFile, sparkSession);
        List<String> l = new ArrayList<String>();
        l.add("2021-11,71.0,A");
        l.add("2021-10,72.0,A");
        l.add("2021-09,63.3,A");
        l.add("2021-08,60.2,A");
        l.add("2021-07,63.5,A");
        Dataset<Row> d = sparkSession.createDataset(l, Encoders.STRING()).toDF();
        Dataset<Row> expectedOutput = d.selectExpr("split(value, ',')[0] as Periode", "split(value, ',')[1] as Prix","split(value, ',')[2] as Code");
        Dataset<Row> actualOutput = fileReader.get();

        assertThat(actualOutput.collectAsList()).isEqualTo(expectedOutput.collectAsList());
    }
}
