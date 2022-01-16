package functions;

import org.apache.spark.sql.Row;

public class Cotation {
    private String annee;
    private String mois;
    private String valeur;
    private String code;

    public Cotation(Row line){
        System.out.println("New Cotation created. ");
        String periode = line.get(0).toString();
        this.annee = periode.split("-")[0];
        this.mois = periode.split("-")[1];
        this.valeur = line.get(1).toString();
        this.code = line.get(2).toString();
    }

    public Cotation(String line){
        System.out.println("New Cotation created. ");
        String[] splitLine = line.split(",");
        String periode = splitLine[0];
        this.annee = periode.split("-")[0];
        this.mois = periode.split("-")[1];
        this.valeur = splitLine[1];
        this.code = splitLine[2];
    }

    public String getAnnee() {
        return annee;
    }

    public String getMois() {
        return mois;
    }

    public String getValeur() {
        return valeur;
    }

    public String getCode() {
        return code;
    }
}
