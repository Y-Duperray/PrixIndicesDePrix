package functions.funct;

import functions.Cotation;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class ConvertRowsToCotations implements Function<Dataset<Row>, Dataset<Cotation>> {
    SparkSession sparkSession;
    public ConvertRowsToCotations(SparkSession session){
        System.out.println("New ConvertRowsToCotations created. ");
        this.sparkSession = session;
    }
    @Override
    public Dataset<Cotation> apply(Dataset<Row> lines) {
        System.out.println("ConvertRowsToCotations.apply()");
        List<Cotation> liste = new ArrayList<Cotation>();
        lines.foreach(line -> {
            Cotation cotation = new Cotation(line);
            System.out.println("cotation : ");
            System.out.println(cotation);
            liste.add(cotation);
        });
        Dataset<Cotation> cotations = sparkSession.createDataset(liste, Encoders.bean(Cotation.class));

        //Dataset<Cotation> cotations = lines.map((line) -> {Cotation c = new Cotation(line);return c;}, Encoders.bean(Cotation.class));


        return cotations;
    }
}
