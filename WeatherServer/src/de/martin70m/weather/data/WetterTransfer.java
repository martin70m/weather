package de.martin70m.weather.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;

import de.martin70m.common.ftp.FTPDownloader;
import de.martin70m.common.sql.MySqlConnection;

public class WetterTransfer {
	
    private static final String STATIONEN = "TU_Stundenwerte_Beschreibung_Stationen.txt";
	private static final String LOCAL_STATIONEN = "C:/Temp/stationen.txt";
	private static final String LOCAL_DIRECTORY = "C:/Temp/Wetterdaten";
	private static final String LOCAL_DATA = "C:/Temp/data.zip";
    private static final String MOEHRENDORF_TEMP = "stundenwerte_TU_01279_akt.zip";
	private static final String AIR_TEMPERATURE_RECENT = "/pub/CDC/observations_germany/climate/hourly/air_temperature/recent/";
	private static final String FTP_CDC_DWD_DE = "ftp-cdc.dwd.de";


	public static void start()
	{
		
		int numberFiles = 0;
		
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

        long seconds = duration.getSeconds();

		
		try {
			MySqlConnection mySqlDB = new MySqlConnection();
			try(final Connection conn = mySqlDB.getConnection()) {				
				try(final PreparedStatement prep = conn.prepareStatement("INSERT INTO FtpDownload (numberFiles, successful, duration, location) VALUES (?,?,?,?);")) {
					prep.setInt (1, numberFiles);		
					prep.setString(2, "Y");
					prep.setLong(3, seconds);
					prep.setString(4, FTP_CDC_DWD_DE + AIR_TEMPERATURE_RECENT);
					prep.execute();
				}
			}
			
		} catch(SQLException se) {
			System.out.println(se.getMessage());			
		}

		
        
	}

}
