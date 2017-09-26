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

public class WetterTransfer {
	
    private static final String STATIONEN = "TU_Stundenwerte_Beschreibung_Stationen.txt";
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
					stationData.setName(data[6]);
					try(final PreparedStatement prep = conn.prepareStatement("INSERT INTO Station (id, name) VALUES (?,?);")) {
						prep.setInt (1, stationData.getID());		
						prep.setString(2, stationData.getName());
						prep.execute();				
					}
					
				}
			}
			
		} catch(SQLException se) {
			System.out.println(se.getMessage());			
		}

		
        
	}

}
