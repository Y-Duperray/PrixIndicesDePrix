package functions;

public class Year {
    private String prixMoyen;
    private String prixMax;
    private String prixMin;

    public String getPrixMoyen() {
        return prixMoyen;
    }

    public String getPrixMax() {
        return prixMax;
    }

    public String getPrixMin() {
        return prixMin;
    }

    public String getEcart() {
        return String.valueOf(Float.parseFloat(prixMax) - Float.parseFloat(prixMin));
    }
}
