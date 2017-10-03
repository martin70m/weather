package de.martin70m.weather.data;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import de.martin70m.common.ftp.FTPDownloader;
import de.martin70m.common.sql.MySqlConnection;
import de.martin70m.common.zip.ZipReader;

public class WetterTransfer {
	
    private static final String STATIONEN = "TU_Stundenwerte_Beschreibung_Stationen.txt";
    private static final String WETTERDATEN = "stundenwerte_TU_[ID]_akt.zip";
	private static final String LOCAL_DIRECTORY = "C:/Temp/Wetterdaten";
	
    private static final String MOEHRENDORF_TEMP = "stundenwerte_TU_01279_akt.zip";
	private static final String AIR_TEMPERATURE_RECENT = "/pub/CDC/observations_germany/climate/hourly/air_temperature/recent/";
	private static final String FTP_CDC_DWD_DE = "ftp-cdc.dwd.de";


	public static void start(boolean withDownload)
	{
		
		int numberFiles = 0;
		long seconds = 0;
		
		try {
			MySqlConnection mySqlDB = new MySqlConnection();
			try(final Connection conn = mySqlDB.getConnection()) {				
				try(final PreparedStatement prep = conn.prepareStatement("SELECT * FROM MyGuests")) {
					try(final ResultSet rs = prep.executeQuery()) {
						while(rs.next()) {
							System.out.println(rs.getString("email") + " - " + rs.getString("reg_date"));
						}
						
					}				
					
				}
			}
			
		} catch(SQLException se) {
			System.out.println(se.getMessage());			
		}

		if(withDownload) {
		
			LocalTime startTime = LocalTime.now(ZoneId.of("Europe/Berlin"));
			
			try {
	            FTPDownloader ftpDownloader = new FTPDownloader(FTP_CDC_DWD_DE, "anonymous", "");
	            
	            numberFiles = ftpDownloader.downloadFiles(AIR_TEMPERATURE_RECENT, LOCAL_DIRECTORY);
	            if(numberFiles > 0)
	                System.out.println("FTP File downloaded successfully");
	            
	            ftpDownloader.disconnect();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
			LocalTime endTime = LocalTime.now(ZoneId.of("Europe/Berlin"));
			
			Duration duration = Duration.between(startTime, endTime);
	
	        seconds = duration.getSeconds();
		}
		
        List<String> stations = null;
		try {
			File infile=new File(LOCAL_DIRECTORY + "/" + STATIONEN);
		
		    stations = java.nio.file.Files.readAllLines(infile.toPath(), StandardCharsets.ISO_8859_1);
		    List<String> badLines = new ArrayList<String>();
		    
		    if(stations != null && !stations.isEmpty()) {
			    for (String station : stations) {
			    	if(station.startsWith("---") || station.startsWith("Station"))
			    		badLines.add(station);
			    }
		    }
		    for(String badLine : badLines) {
		    	stations.remove(badLine);
		    }
		    for (String station : stations) {
		    	System.out.println(station);	    	
		    }		    
		    
		} catch(IOException fe) {
			System.out.println(fe.getMessage());
		}
		try {
			MySqlConnection mySqlDB = new MySqlConnection();
			try(final Connection conn = mySqlDB.getConnection()) {
				if(withDownload)
					try(final PreparedStatement prep = conn.prepareStatement("INSERT INTO FtpDownload (numberFiles, successful, duration, location) VALUES (?,?,?,?);")) {
						prep.setInt (1, numberFiles);		
						prep.setString(2, "Y");
						prep.setLong(3, seconds);
						prep.setString(4, FTP_CDC_DWD_DE + AIR_TEMPERATURE_RECENT);
						prep.execute();
					}
				
				for(String station : stations) {
					//String[] data = station.split("\\s",20);
					StationDTO stationData = new StationDTO();
					String[] data = Arrays.asList(station.split("[ ]")).stream().filter(str -> !str.isEmpty()).collect(Collectors.toList()).toArray(new String[0]);
					stationData.setID(new Integer(data[0]).intValue());
					stationData.setVonDatum(new Integer(data[1]).intValue());
					stationData.setBisDatum(new Integer(data[2]).intValue());
					stationData.setHeight(new Integer(data[3]).intValue());
					stationData.setLatitude(data[4]);
					stationData.setLongitude(data[5]);
					stationData.setName(data[6]);
					stationData.setLand(data[7]);
					PreparedStatement prep = conn.prepareStatement("SELECT * FROM Station WHERE id = ?;");
					prep.setInt (1, stationData.getID());	
					try(final ResultSet rs = prep.executeQuery()) {
						if(rs.next()) {
							
							if(stationData.getBisDatum() != rs.getLong("bisDatum")) { 
								try(final PreparedStatement prep1 = conn.prepareStatement("UPDATE Station set bisDatum = ? WHERE id = ?;")) {
									prep1.setLong(1, stationData.getBisDatum());
									prep1.setInt(2, stationData.getID());
									prep1.execute();
									System.out.println(rs.getString("name") + " updated");
								}
							} else {
								System.out.println(rs.getString("name"));
							}
						} else {
							try(final PreparedStatement prep2 = conn.prepareStatement("INSERT INTO Station (id, name, vonDatum, bisDatum, geoBreite, geoLaenge, hoehe, bundesland) VALUES (?,?,?,?,?,?,?,?);")) {
								prep2.setInt (1, stationData.getID());		
								prep2.setString(2, stationData.getName());
								prep2.setLong(3, stationData.getVonDatum());
								prep2.setLong(4, stationData.getBisDatum());
								prep2.setString(5, stationData.getLatitude());
								prep2.setString(6, stationData.getLongitude());
								prep2.setInt(7, stationData.getHeight());
								prep2.setString(8, stationData.getLand());
								
								prep2.execute();				
							}							
						}
						
					}	
					String id = "0000" + stationData.getID();
					while(id.length() > 5) {
						id = id.substring(1, id.length());
					}
					String filename = WETTERDATEN.replace("[ID]", id);
					int filecounter = ZipReader.upzip(LOCAL_DIRECTORY + "/" + filename, LOCAL_DIRECTORY + "/" + id);
					System.out.println(filename + " extraced to " + filecounter + " files.");
				}
				
			}
			
		} catch(SQLException se) {
			System.out.println(se.getMessage());			
		}

		
        
	}

}
