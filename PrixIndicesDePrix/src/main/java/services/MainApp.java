package services;

import functions.reader.FileReader;
import functions.writer.FileWriter;
import functions.Cotation;
import functions.funct.ConvertRowsToCotations;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class MainApp {
    public static void main( String[] args ) {
        System.out.println("Main app. ");
        //String inputFile = "C:/Users/Y.DUPERRAY/Documents/ARBORESCENCE/Scolaire/3iL/M2/Exploitation des données de masse/serie_010002078_11012022/valeurs_mensuelles.csv";
        //String outputDirectory = "C:/Users/Y.DUPERRAY/Documents/ARBORESCENCE/Scolaire/3iL/M2/Exploitation des données de masse/output";
        String inputFile ="target/test-classes/valeurs_mensuelles.csv";
        String outputDirectory ="target/test-classes";

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        FileReader fileReader = new FileReader(inputFile, sparkSession);
        Dataset<Row> lines =  fileReader.get();
        lines.printSchema();

        ConvertRowsToCotations c = new ConvertRowsToCotations(sparkSession);
        Dataset<Cotation> test = c.apply(lines);
        test.printSchema();

        FileWriter fileWriter = new FileWriter(outputDirectory);
        fileWriter.accept(lines);
    }
}
