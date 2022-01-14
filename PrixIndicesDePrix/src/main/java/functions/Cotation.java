package functions;

import org.apache.spark.sql.Row;

import java.io.Serializable;

public class Cotation {
    private String periode;
    private String valeur;
    private String code;

    public Cotation(Row line){
        System.out.println("New Cotation created. ");
        this.periode = line.get(0).toString();
        this.valeur = line.get(1).toString();
        this.code = line.get(2).toString();
    }

    public Cotation(String line){
        System.out.println("New Cotation created. ");
        String[] splitLine = line.split(",");
        this.periode = splitLine[0];
        this.valeur = splitLine[1];
        this.code = splitLine[2];
    }

    public String getPeriode() {
        return periode;
    }

    public String getValeur() {
        return valeur;
    }

    public String getCode() {
        return code;
    }
}
