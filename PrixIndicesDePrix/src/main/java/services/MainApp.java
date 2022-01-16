package services;

import functions.Year;
import functions.funct.RowsToYears;
import functions.reader.FileReader;
import functions.writer.FileWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class MainApp {
    public static void main( String[] args ) {
        System.out.println("Main app. ");
        String inputFile ="src/main/resources/valeurs_mensuelles.csv";
        String outputDirectory = "target/classes/output";

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        FileReader fileReader = new FileReader(inputFile, sparkSession);
        Dataset<Row> lines =  fileReader.get();
        lines.show();

        RowsToYears rowsToYears = new RowsToYears(sparkSession);
        Dataset<Year> years = rowsToYears.apply(lines);
        years.show();

        FileWriter fileWriter = new FileWriter(outputDirectory);
        fileWriter.accept(years.toDF());
    }
}
