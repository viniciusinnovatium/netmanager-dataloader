package br.com.innovatium.netmanager.dataload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

class TableFilesReader {
	private Connection connection;
	private PreparedStatement ps = null;
	private final String SEPARATOR = "#=";
	private String dburl;
	private String dbuser;
	private String dbpassword;
	private String tbschema;
	private String driver;
	private File directory;
	private SQLResolver resolver;
	private Map<String, Integer[]> linesCount = new HashMap<String, Integer[]>();

	/**
	 * Its was created to support unit tests
	 */
	public TableFilesReader() {

	}

	public void read() throws FileReadingException {
		config();

		final Date begin = new Date();
		if (directory == null) {
			return;
		}
		final File[] files = directory.listFiles();

		if (files == null || files.length == 0) {
			System.out.println("There is no files to import!!!");
			return;
		}

		BufferedReader reader = null;
		String line;
		String tableName = null;
		try {
			connection = openConnection();
		} catch (Exception e1) {
			throw new FileReadingException(
					"It was not possible read file to migrate data", e1);
		}

		String insert = null;
		String[] keyValue = null;
		int lineNumber = 0;
		int insertOK = 0;

		// Read all files to be loaded.
		for (File file : files) {
			try {
				reader = new BufferedReader(new FileReader(file));
			} catch (FileNotFoundException e) {
				System.err.println("WARNING on reading directory import files "
						+ file.getName() + ". Message: " + e.getMessage());
			}

			System.out.println("Reading file: " + file.getName());

			tableName = generateTableName(file);
			dropTable(tableName);
			createTable(tableName);

			insert = "insert into " + tableName + " values (?,?)";
			lineNumber = 0;
			insertOK = 0;
			try {
				while ((line = reader.readLine()) != null) {
					lineNumber++;
					keyValue = line.split(SEPARATOR);
					if (keyValue.length == 0) {
						System.out.println("WARNING on a splitting line "
								+ lineNumber + " from the file "
								+ file.getName());
						continue;
					}
					try {
						ps = connection.prepareStatement(insert);
						if (keyValue.length == 1) {
							ps.setString(1, keyValue[0]);
							ps.setString(2, null);
						} else {
							ps.setString(1, keyValue[0]);
							ps.setString(2, keyValue[1]);
						}
						ps.execute();
						connection.commit();
						insertOK++;
					} catch (SQLException e) {
						System.out.println("WARNING on processing line "
								+ lineNumber + " of the file " + file.getName()
								+ ". Message: " + e.getMessage());
					}

					if (ps != null) {

						try {
							ps.close();
						} catch (SQLException e) {
							System.out
									.println("Fail to close insert statement when during line "
											+ lineNumber + " treatment");
						}
					}
				} // fim da leitura do arquivo
				linesCount.put(tableName,
						new Integer[] { insertOK, lineNumber });
			} catch (IOException e) {
				System.out.println("WARNING on reading file " + file.getName()
						+ ". Message: " + e.getMessage());

			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e) {
						System.out
								.println("Fail to close file reader of the file: "
										+ file.getName());
					}
				}
			}
		}

		try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			System.out
					.println("Fail to close connection after data import. Message: "
							+ e.getMessage());
		}
		final Date end = new Date();
		System.out.println("----------- Report ----------");
		System.out.println("-----------------------------");
		System.out.println("Time expended to import: "
				+ formatTimeExpended(begin, end));
		printCountLines();
		System.out.println("-----------------------------");
	}

	private String formatTimeExpended(Date begin, Date end) {
		long time= (end.getTime() - begin.getTime())/1000L;
		int hours = (int) (time / 3600);
		int minutes = (int) ((time % 3600) / 60);
		int seconds = (int) ((time % 3600) % 60);
		return formatTime(hours)+":"+formatTime(minutes)+":"+formatTime(seconds);
	}
	
	private static String formatTime(int value){
		return value <= 9 ? "0"+value : String.valueOf(value);
	}

	private void printCountLines() {
		Set<Entry<String, Integer[]>> entries = linesCount.entrySet();
		boolean hasProblem = false;
		for (Entry<String, Integer[]> entry : entries) {
			hasProblem = !entry.getValue()[0].equals(entry.getValue()[1]);
			System.out.println("File " + entry.getKey() + " imported "
					+ entry.getValue()[0] + " from " + entry.getValue()[1]
					+ " line(s) "
					+ (hasProblem ? "-----> VERIFY CONTENT FILE" : ""));
		}
	}

	private void dropTable(String tableName) {
		StringBuilder dropTable = new StringBuilder();
		dropTable.append(resolver.resolve(SQLType.DROP_TABLE)).append(" ")
				.append(tbschema).append(".").append(tableName);
		try {
			ps = connection.prepareStatement(dropTable.toString());
			ps.execute();
			connection.commit();
		} catch (SQLException e) {
			System.out.println("WARNING on drop table " + tableName
					+ ". Message: " + e.getMessage());
		}
	}

	private void createTable(String tableName) {
		final String dataType = resolver.resolve(SQLType.STRING);

		StringBuilder createTable = new StringBuilder();
		createTable.append(resolver.resolve(SQLType.CREATE_TABLE));
		createTable.append(" ");
		createTable.append(tbschema);
		createTable.append(".");
		createTable.append(tableName);
		createTable.append("( key_ ").append(dataType).append(" not null ");
		createTable.append(", value_ ").append(dataType).append(" ) ");
		try {
			ps = connection.prepareStatement(createTable.toString());
			ps.execute();
			connection.commit();
		} catch (SQLException e) {
			System.out.println("WARNING on creating table " + tableName
					+ ". Message: " + e.getMessage());
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					System.out
							.println("WARNING to close create table statement of the table "
									+ tableName
									+ ". Message: "
									+ e.getMessage());
				}
			}
		}
	}

	private String generateTableName(File file) {
		return file.getName().split("\\.")[0];
	}

	private Connection openConnection() throws Exception {
		System.out.println("Waiting to open database connection...");
		try {
			try {
				if (driver != null) {
					Class.forName(driver);
				}
			} catch (ClassNotFoundException e) {
				System.out.println("Fail to load jdbc driver. Message: "
						+ e.getMessage());
				throw e;
			}
			connection = DriverManager.getConnection(dburl, dbuser, dbpassword);
			connection.setAutoCommit(false);
			return connection;
		} catch (SQLException e) {
			System.err.println("Fail to open database connection. Message: "
					+ e.getMessage());
			throw e;
		}

	}

	private void config() throws FileReadingException {
		Properties properties = new Properties();
		try {
			properties.load(new FileReader(new File("data-import.properties")));

			dburl = properties.getProperty("dburl");
			dbuser = properties.getProperty("dbuser");
			dbpassword = properties.getProperty("dbpassword");
			tbschema = properties.getProperty("tbschema");
			driver = properties.getProperty("driver");
			directory = new File("tables");

			configSQLResolver();

			System.out.println("-----------------------------");
			System.out.println("--- CONFIG IMPORTING DATA ---");
			System.out.println("-----------------------------");
			System.out.println("dburl: " + dburl);
			System.out.println("dbuser: " + dbuser);
			System.out.println("dbpassword: " + dbpassword);
			System.out.println("tbschema: " + tbschema);
			System.out.println("driver: " + driver);
			System.out.println("-----------------------------");

		} catch (IOException e) {
			System.out.println("Fail on reading properties file. "
					+ e.getMessage());
			throw new FileReadingException("Fail on reading properties file", e);
		}

	}

	private void configSQLResolver() {
		SQLResolverType type = null;
		if (dburl.contains("postgre")) {
			type = SQLResolverType.POSTGRE;
		} else if (dburl.contains("oracle")) {
			type = SQLResolverType.ORACLE;
		} else if (dburl.contains("mysql")) {
			type = SQLResolverType.MYSQL;
		}
		resolver = SQLResolver.getResolver(type);
	}

}