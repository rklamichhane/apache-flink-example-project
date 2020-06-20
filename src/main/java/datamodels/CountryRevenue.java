package main.java.datamodels;

public class CountryRevenue {
    private String country;
    private Double totalRevenue;

    public CountryRevenue(String country,Double totalRevenue){
        this.country = country;
        this.totalRevenue = totalRevenue;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Double getTotalRevenue() {
        return totalRevenue;
    }

    public void setTotalRevenue(Double totalRevenue) {
        this.totalRevenue = totalRevenue;
    }
}
