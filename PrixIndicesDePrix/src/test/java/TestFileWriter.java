import functions.writer.FileWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestFileWriter {
    String OutputFile = "target/classes/output/resultat.csv";

    @Before
    public void setup(){
        cleanup();
    }

    public void cleanup(){
        try {
            Files.list(Paths.get(OutputFile)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                }catch(IOException e){
                    e.printStackTrace();
                }
            });
            Files.deleteIfExists(Paths.get(OutputFile));
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    @Test
    public void test() {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> inputDataset = sparkSession
                .range(0,5,2,2)
                .map((MapFunction<Long, Row>) c -> RowFactory.create(String.format("line %d", c)), Encoders.bean(Row.class));
        FileWriter fileWriter = new FileWriter(OutputFile);
        fileWriter.accept(inputDataset);

        List<String> actual = Files.readAllLines(Paths.get(OutputFile));
        String[] expected = new String[]{
                "\"2021-11\";\"71.0\";\"A\"",
                "\"2021-10\";\"72.0\";\"A\"",
                "\"2021-09\";\"63.3\";\"A\"",
                "\"2021-08\";\"60.2\";\"A\"",
                "\"2021-07\";\"63.5\";\"A\""
        };

        assertThat(actual).containsExactlyInAnyOrder(expected);
    }

    @After
    public void tearDown(){
        cleanup();
    }
}
