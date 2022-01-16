import functions.funct.RowsToYears;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.junit.Test;
import functions.Year;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestRowsToYears {
    String OutputFile = "target/classes/resultat.csv";

    @Test
    public void test() {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        List<String> l = new ArrayList<String>();
        l.add("2021-11,71.0,A");
        l.add("2021-10,72.0,A");
        l.add("2021-09,63.3,A");
        l.add("2021-08,60.2,A");
        l.add("2021-07,63.5,A");
        Dataset<Row> d = sparkSession.createDataset(l, Encoders.STRING()).toDF();
        Dataset<Row> inputDataset = d.selectExpr("split(value, ',')[0] as Periode", "split(value, ',')[1] as Prix","split(value, ',')[2] as Code");

        RowsToYears rowsToYears = new RowsToYears(sparkSession);
        Dataset<Year> actual = rowsToYears.apply(inputDataset);

        String[] expected = new String[]{};

        assertThat(actual);
    }
}
