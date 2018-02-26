package de.martin70m.weather.data;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import de.martin70m.common.io.FTPDownloader;
import de.martin70m.common.io.FileFinder;
import de.martin70m.common.io.ZipReader;
import de.martin70m.common.sql.MySqlConnection;

public class WetterTransfer {

	private static final String STATIONEN = "TU_Stundenwerte_Beschreibung_Stationen.txt";
	private static final String WETTERDATEN = "stundenwerte_TU_[ID]_akt.zip";
	private static final String LOCAL_DIRECTORY = "/deployments/Wetterdaten";

	private static final String AIR_TEMPERATURE_RECENT = "/pub/CDC/observations_germany/climate/hourly/air_temperature/recent/";
	private static final String FTP_CDC_DWD_DE = "ftp-cdc.dwd.de";

	public static void start(boolean withDownload) {

		int numberFiles = 0; 
		long seconds = 0;
		String path = LOCAL_DIRECTORY;

		//printOutVisitors();

		if (withDownload) {

			LocalTime startTime = LocalTime.now(ZoneId.of("Europe/Berlin"));

			numberFiles = downloadFromWeatherServer(numberFiles, path);
			LocalTime endTime = LocalTime.now(ZoneId.of("Europe/Berlin"));

			Duration duration = Duration.between(startTime, endTime);

			seconds = duration.getSeconds();
		}

		List<String> stations = extractStationsFromFile(path);

		try {
			MySqlConnection mySqlDB = new MySqlConnection();
			try (final Connection conn = mySqlDB.getConnection()) {
				if (withDownload)
					saveStaticData(numberFiles, seconds, conn);
				conn.setAutoCommit(false);

				for (String station : stations) {
					// String[] data = station.split("\\s",20);
					int stationID = updateStation(conn, station);

					String stationDirName = "0000" + stationID;
					while (stationDirName.length() > 5) {
						stationDirName = stationDirName.substring(1, stationDirName.length());
					}
					String filename = WETTERDATEN.replace("[ID]", stationDirName);
					String unzippedDir = path + "/" + stationDirName;
					// create Directory for unzipping Files, if not exists
					Path outDir = Paths.get(unzippedDir);
					try {
						Files.createDirectories(outDir);
					} catch (FileAlreadyExistsException e4) {
						//e4.printStackTrace();
						//OK
					} catch (IOException e3) {
						//e3.printStackTrace();
						//OK
					}

					int filecounter = ZipReader.upzip(path + "/" + filename, unzippedDir);
					System.out.println(filename + " extraced to " + filecounter + " files.");
					// produkt_tu_stunde
					if (filecounter > 0) {
						String filename1 = FileFinder.find("produkt_tu_stunde", unzippedDir);
						File infile = new File(unzippedDir + "/" + filename1);
						List<String> temperatures = null;
						try {
							int alteStationsID = 0;
							long maxDatum = 20170101;
							int maxUhrzeit = 0;
							temperatures = readDataFromFile(infile);

							for (String temperature : temperatures) {
								
								MesswertDTO messwert = new MesswertDTO();
								List<String> data1 = Arrays.asList(temperature.split(";"));
								messwert.setStationID(new Integer(data1.get(0).trim()).intValue());
								if (alteStationsID != messwert.getStationID()) {
									conn.commit();

									try (final PreparedStatement prep3 = conn.prepareStatement(
											"SELECT max(datum) as maxdatum, max(uhrzeit) as maxuhrzeit FROM Messwert WHERE stationid = ?;")) {
										prep3.setInt(1, messwert.getStationID());
										try (final ResultSet rs = prep3.executeQuery()) {
											if (rs.next()) {
												if (rs.getLong("maxdatum") != 0) {
													maxDatum = rs.getLong("maxdatum");
													maxUhrzeit = rs.getInt("maxuhrzeit");
												}
												alteStationsID = messwert.getStationID();
											}
										}

									}
								}
								long datetime = new Integer(data1.get(1).trim()).intValue();
								messwert.setDate(datetime / 100);
								long time = datetime - messwert.getDate() * 100;
								messwert.setHour((int) time);

								if (messwert.getDate() > maxDatum
										|| messwert.getDate() == maxDatum && messwert.getHour() > maxUhrzeit) {
									messwert.setTemperatur(data1.get(3).trim());
									messwert.setHumidity(data1.get(4).trim());
									// try (final PreparedStatement prep3 = conn.prepareStatement(
									// "SELECT count(*) as anzahl FROM Messwert WHERE stationid = ? AND datum = ?
									// and uhrzeit = ?;")) {
									// prep3.setInt(1, messwert.getStationID());
									// prep3.setLong(2, messwert.getDate());
									// prep3.setInt(3, messwert.getHour());
									// }

									// try (final ResultSet rs = prep3.executeQuery()) {
									// if (rs.next()) {
									// if (rs.getLong("anzahl") == 0) {
									try (final PreparedStatement prep2 = conn.prepareStatement(
											"INSERT INTO Messwert (stationid, datum, uhrzeit, temperatur, luftfeuchte) VALUES (?,?,?,?,?);")) {
										prep2.setInt(1, messwert.getStationID());
										prep2.setLong(2, messwert.getDate());
										prep2.setInt(3, messwert.getHour());
										prep2.setString(4, messwert.getTemperatur());
										prep2.setString(5, messwert.getHumidity());

										prep2.execute();
										System.out.println(temperature + " inserted to database");
									}
									// }
									// }
									// }

								}

							}
							conn.commit();

						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}
				}

			}

		} catch (SQLException se) {
			System.out.println(se.getMessage());
		}

	}

	private static int updateStation(final Connection conn, String station) throws SQLException {
		int stationID;
		StationDTO stationData = new StationDTO();
		String[] data = Arrays.asList(station.split("[ ]")).stream().filter(str -> !str.isEmpty())
				.collect(Collectors.toList()).toArray(new String[0]);
		stationData.setID(new Integer(data[0]).intValue());
		stationData.setVonDatum(new Integer(data[1]).intValue());
		stationData.setBisDatum(new Integer(data[2]).intValue());
		stationData.setHeight(new Integer(data[3]).intValue());
		stationData.setLatitude(data[4]);
		stationData.setLongitude(data[5]);
		stationData.setName(data[6]);
		stationData.setLand(data[7]);

		stationID = stationData.getID();

		PreparedStatement prep = conn.prepareStatement("SELECT * FROM Station WHERE id = ?;");
		prep.setInt(1, stationData.getID());
		try (final ResultSet rs = prep.executeQuery()) {
			if (rs.next()) {

				if (stationData.getBisDatum() != rs.getLong("bisDatum")) {
					try (final PreparedStatement prep1 = conn
							.prepareStatement("UPDATE Station set bisDatum = ? WHERE id = ?;")) {
						prep1.setLong(1, stationData.getBisDatum());
						prep1.setInt(2, stationData.getID());
						prep1.execute();
						conn.commit();
						System.out.println(rs.getString("name") + " updated");
					}
				} else {
					System.out.println(rs.getString("name"));
				}
			} else {
				try (final PreparedStatement prep2 = conn.prepareStatement(
						"INSERT INTO Station (id, name, vonDatum, bisDatum, geoBreite, geoLaenge, hoehe, bundesland) VALUES (?,?,?,?,?,?,?,?);")) {
					prep2.setInt(1, stationData.getID());
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
		return stationID;
	}

	private static void saveStaticData(int numberFiles, long seconds, final Connection conn) throws SQLException {
		try (final PreparedStatement prep = conn.prepareStatement(
				"INSERT INTO FtpDownload (numberFiles, successful, duration, location) VALUES (?,?,?,?);")) {
			prep.setInt(1, numberFiles);
			prep.setString(2, "Y");
			prep.setLong(3, seconds);
			prep.setString(4, FTP_CDC_DWD_DE + AIR_TEMPERATURE_RECENT);
			prep.execute();
		}
	}
/*
	private static void printOutVisitors() {
		try {
			MySqlConnection mySqlDB = new MySqlConnection();
			try (final Connection conn = mySqlDB.getConnection()) {
				try (final PreparedStatement prep = conn.prepareStatement("SELECT * FROM MyGuests")) {
					try (final ResultSet rs = prep.executeQuery()) {
						while (rs.next()) {
							System.out.println(rs.getString("email") + " - " + rs.getString("reg_date"));
						}

					}

				}
			}

		} catch (SQLException se) {
			System.out.println(se.getMessage());
		}
	}
*/
	private static List<String> extractStationsFromFile(String path) {
		List<String> stations = null;
		try {
			File infile = new File(path + "/" + STATIONEN);

			stations = readDataFromFile(infile);
			for (String station : stations) {
				System.out.println(station);
			}

		} catch (IOException fe) {
			System.out.println(fe.getMessage());
		}
		return stations;
	}

	private static int downloadFromWeatherServer(int numberFiles, String path) {
		try {
			FTPDownloader ftpDownloader = new FTPDownloader(FTP_CDC_DWD_DE, "anonymous", "");
			numberFiles = ftpDownloader.downloadFiles(AIR_TEMPERATURE_RECENT, path);
			if (numberFiles > 0)
				System.out.println("FTP File downloaded successfully");

			ftpDownloader.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return numberFiles;
	}

	private static List<String> readDataFromFile(File infile) throws IOException {
		List<String> allLines;
		allLines = Files.readAllLines(infile.toPath(), StandardCharsets.ISO_8859_1);
		List<String> badLines = new ArrayList<String>();

		if (allLines != null && !allLines.isEmpty()) {
			for (String line : allLines) {
				if (line.startsWith("---") || line.toUpperCase().startsWith("STATION"))
					badLines.add(line);
			}
		}
		for (String badLine : badLines) {
			allLines.remove(badLine);
		}
		return allLines;
	}

}
