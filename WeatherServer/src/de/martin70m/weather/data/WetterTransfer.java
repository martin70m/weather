package de.martin70m.weather.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    	try {
            FTPDownloader ftpDownloader =
                new FTPDownloader(FTP_CDC_DWD_DE, "anonymous", "");
            System.out.println("FTP File downloaded successfully");
            boolean result = ftpDownloader.listFiles(AIR_TEMPERATURE_RECENT, STATIONEN);
            System.out.println("File " + STATIONEN + " = " + result);
            if(result) {
                ftpDownloader.downloadFiles(AIR_TEMPERATURE_RECENT, LOCAL_DIRECTORY);
                //ftpDownloader.downloadFileAIR_TEMPERATURE_RECENT + MOEHRENDORF_TEMP, LOCAL_DATA);            	
            }
            ftpDownloader.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
		try {
			MySqlConnection mySqlDB = new MySqlConnection();
			try(Connection conn = mySqlDB.getConnection()) {				
				try(PreparedStatement prep = conn.prepareStatement("SELECT * FROM MyGuests")) {
					try(ResultSet rs = prep.executeQuery()) {
						while(rs.next()) {
							System.out.println(rs.getString("email") + " - " + rs.getString("reg_date"));
						}
						
					}				
					
				}
			}
			
		} catch(SQLException se) {
			System.out.println(se.getMessage());			
		}
	}

}
