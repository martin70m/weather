package de.martin70m.weather.data;

public class StationDTO {
	private int ID;
	private long vonDatum;
	private long bisDatum;
	private int height;
	private int latitude;
	private int longitude;
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
	public int getLatitude() {
		return latitude;
	}
	public void setLatitude(int latitude) {
		this.latitude = latitude;
	}
	public int getLongitude() {
		return longitude;
	}
	public void setLongitude(int longitude) {
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
