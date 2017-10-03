package de.martin70m.weather.data;

public class StationDTO {
	private int ID;
	private long vonDatum;
	private long bisDatum;
	private int height;
	private String latitude;
	private String longitude;
	private String name;
	private String land;
	
	public int getID() {
		return ID;
	}
	public void setID(int iD) {
		ID = iD;
	}
	public long getVonDatum() {
		return vonDatum;
	}
	public void setVonDatum(long vonDatum) {
		this.vonDatum = vonDatum;
	}
	public long getBisDatum() {
		return bisDatum;
	}
	public void setBisDatum(long bisDatum) {
		this.bisDatum = bisDatum;
	}
	public int getHeight() {
		return height;
	}
	public void setHeight(int height) {
		this.height = height;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getLand() {
		return land;
	}
	public void setLand(String land) {
		this.land = land;
	}
	

}
