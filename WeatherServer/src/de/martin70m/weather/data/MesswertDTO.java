package de.martin70m.weather.data;

public class MesswertDTO {
	private int stationID;
	private long date;
	private int hour;
	private String temperatur;
	private String humidity;
	
	public int getStationID() {
		return stationID;
	}
	public void setStationID(int stationID) {
		this.stationID = stationID;
	}
	public long getDate() {
		return date;
	}
	public void setDate(long date) {
		this.date = date;
	}
	public int getHour() {
		return hour;
	}
	public void setHour(int hour) {
		this.hour = hour;
	}
	public String getHumidity() {
		return humidity;
	}
	public void setHumidity(String humidity) {
		this.humidity = humidity;
	}
	public String getTemperatur() {
		return temperatur;
	}
	public void setTemperatur(String temperatur) {
		this.temperatur = temperatur;
	}
	
}
