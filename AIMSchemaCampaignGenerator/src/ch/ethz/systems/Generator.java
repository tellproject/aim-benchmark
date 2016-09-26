package ch.ethz.systems;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class Generator {
	
	// Database parameters
	public final static String
			DEFAULT_INSTANCE = "mysql",
			DEFAULT_DATABASE = "meta_db",
			DEFAULT_HOST = "localhost",
			DEFAULT_USER = "root",
			DEFAULT_PASSWORD = "";
	
	public final static int DEFAULT_PORT = 3306;
	
	// supported aggregated functions
//	private static String[] aggregateFunctions = new String[]{"sum", "avg", "min", "max"};
	private static String[] aggregateFunctions = new String[]{"sum", "min", "max"};
	
	// Workload Parameters
	public final static int DEFAULT_SCALING_FACTOR = 1,
			DEFAULT_SEED = 0,
			BASE_NUMBER_OF_CAMPAIGNS = 300,	//real number is SF * base
			NUMBER_OF_REPETITIONS = 1,		//number of repetitions of unique wt attributes
			NUMBER_OF_UNIQUE_WT_ATTRIBUTES = 6 * (2 * aggregateFunctions.length + 1),
			NUMBER_OF_WT_ATTRIBUTES = NUMBER_OF_REPETITIONS * NUMBER_OF_UNIQUE_WT_ATTRIBUTES,
			RECORD_SIZE = NUMBER_OF_REPETITIONS * 6 * ((4 + 8) * aggregateFunctions.length + 4),
			NUMBER_OF_PIVOT_ATTRIBUTES = 20,
			MAX_NUMBER_OF_CONJUNCTS_PER_CAMPAIGN = 5,
			MAX_NUMBER_OF_PREDICATES_PER_CONJUNCT = 5;
	
	/* Assumptions about workload:
	 * - Pivot attributes have IDs from 1 to NUMBER_OF_PIVOT_ATTRIBUTES
	 * - Other WT attributes have IDs
	 * 		from NUMBER_OF_PIVOT_ATTRIBUTES+1 to NUMBER_OF_WT_ATTRIBUTES
	 */
	
	// Private Variables
	private static Connection connection;
	private static PreparedStatement addConstantStmt, addPredicateStmt,
	addConjunctStmt, addConjunctPredicateStmt;
	private static Random random;
	private static int conjunctIDCounter = 0;
	private static final Map<String, Integer> constants =
			new HashMap<String, Integer>();
	private static final Map<String, Integer> predicates =
			new HashMap<String, Integer>();
	private static final String[] wtAttributeDatatypes =
			new String[NUMBER_OF_WT_ATTRIBUTES+1];
	
	private static boolean createConnection(String instance, String host, int port,
			String database, String user, String password) {		
		Properties connectionProperties = new Properties();
		connectionProperties.setProperty("user", user);
		connectionProperties.setProperty("password", password);
		String connectionURL = "jdbc:" +
                                instance + ":";
                if (!instance.equals("sqlite"))
                    connectionURL +=  "//" + host + ":" + port + "/";
                connectionURL += database;
		try {
			connection = DriverManager.getConnection(connectionURL,
					connectionProperties);
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}		
		return true;
	}
	
	
	private static boolean createDatabaseSchema() {
		try {
			Statement statement = connection.createStatement();
			
			String table_definition = "CREATE TABLE metric (" +
					" id		INTEGER NOT NULL PRIMARY KEY," +
					" name		VARCHAR(25) NOT NULL," +
					" data_type VARCHAR(6)	NOT NULL );";
			statement.execute(table_definition);
			
			table_definition = "CREATE TABLE wt_attribute (" +
					" id				INTEGER NOT NULL PRIMARY KEY," +
					" metric			INTEGER NOT NULL," +
					" is_pivot			TINYINT(1) NOT NULL DEFAULT 0," +
					" window_type		VARCHAR(4) NOT NULL," +
					" window_size		VARCHAR(1) NOT NULL," +
					" aggr_fun			VARCHAR(3) NOT NULL," +
					" aggr_data_type	VARCHAR(6) NOT NULL," +
					" FOREIGN KEY (metric)" +
					" REFERENCES metric (id) );";
			statement.execute(table_definition);
			
			table_definition = "CREATE TABLE constant (" +
					" id		INTEGER NOT NULL PRIMARY KEY," +
					" value		VARCHAR(50) NOT NULL," +
					" data_type VARCHAR(6)	NOT NULL );";
			statement.execute(table_definition);
			
			table_definition = "CREATE TABLE predicate (" +
					" id			INTEGER NOT NULL PRIMARY KEY," +
					" wt_attribute	INTEGER NOT NULL," +
					" operator		VARCHAR(4) NOT NULL," +
					" constant		INTEGER NOT NULL," +
					" FOREIGN KEY (wt_attribute) REFERENCES wt_attribute (id)," +
					" FOREIGN KEY (constant) REFERENCES constant (id) );";
			statement.execute(table_definition);
			
			table_definition = "CREATE TABLE campaign (" +
					" id						INTEGER NOT NULL PRIMARY KEY," +
					" valid_from				BIGINT NOT NULL," +
					" valid_to					BIGINT NOT NULL," +
					" firing_interval			VARCHAR(2) NOT NULL," +
					" firing_start_condition	VARCHAR(7) NOT NULL )";
			statement.execute(table_definition);
			
			table_definition = "CREATE TABLE conjunct (" +
					" id		INTEGER NOT NULL PRIMARY KEY," +
					" campaign	INTEGER NOT NULL," +
					" FOREIGN KEY (campaign) REFERENCES campaign (id) );";
			statement.execute(table_definition);
			
			table_definition = "CREATE TABLE conjunct_predicate (" +
					" predicate	INTEGER NOT NULL," +
					" conjunct	INTEGER NOT NULL," +
					" PRIMARY KEY (predicate, conjunct)," +
					" FOREIGN KEY (predicate) REFERENCES predicate (id)," +
					" FOREIGN KEY (conjunct) REFERENCES conjunct (id) );";
			statement.execute(table_definition);
			
			connection.commit();
			statement.close();
			
			addConstantStmt = connection.prepareStatement("INSERT INTO constant" +
					"(id, value, data_type) VALUES (?,?,?);");
			addPredicateStmt = connection.prepareStatement("INSERT INTO predicate" +
					"(id, wt_attribute, operator, constant) VALUES (?,?,?,?);");
			addConjunctStmt = connection.prepareStatement("INSERT INTO conjunct" +
					"(id, campaign) VALUES (?, ?);");
			addConjunctPredicateStmt = connection.prepareStatement(
					"INSERT INTO conjunct_predicate (predicate, conjunct) " +
					"VALUES (?,?);");
			
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	private static boolean populateMetrics() {
		try {
			PreparedStatement prep = connection.prepareStatement("INSERT INTO " +
					"metric(id, name, data_type) " +
					"VALUES (?,?,?);");
			
			// hard coded values as specified
			
			prep.setInt(1, 1);
			prep.setString(2, "cost");
			prep.setString(3, "double");
			prep.execute();
			
			prep.setInt(1, 2);
			prep.setString(2, "call");
			prep.setString(3, "uint");
			prep.execute();
			
			prep.setInt(1, 3);
			prep.setString(2, "duration");
			prep.setString(3, "uint");
			prep.execute();
			
			prep.setInt(1, 4);
			prep.setString(2, "local cost");
			prep.setString(3, "double");
			prep.execute();
			
			prep.setInt(1, 5);
			prep.setString(2, "local call");
			prep.setString(3, "uint");
			prep.execute();
			
			prep.setInt(1, 6);
			prep.setString(2, "local duration");
			prep.setString(3, "uint");
			prep.execute();
			
			prep.setInt(1, 7);
			prep.setString(2, "non local cost");
			prep.setString(3, "double");
			prep.execute();
			
			prep.setInt(1, 8);
			prep.setString(2, "non local call");
			prep.setString(3, "uint");
			prep.execute();
			
			prep.setInt(1, 9);
			prep.setString(2, "non local duration");
			prep.setString(3, "uint");
			prep.execute();
			
			connection.commit();
			prep.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

/*** Not used any more as we make WT attribute generation deterministic */
//	/**
//	 * This function is highly dependent on the hard-coded values for metrics!
//	 * 
//	 * @return 	[0]: attribute-id of metric
//	 * 			[1]: aggregation function
//	 * 			[2]: aggregation data type
//	 */
//	private static Object[] getRandomAggregationValues() {
//		Object[] result = new Object[3];
//		double d = random.nextDouble();
//		int function = random.nextBoolean()?1:3;	//1: cost, 3: duration
//		int multiplicator = random.nextInt(3);		// changes between local/distant/...
//		if (d <= 0.4) {
//			// cumulative
//			String aggregate = random.nextBoolean()?"sum":"avg";
//			result[0] = 3*multiplicator + function;
//			result[1] = aggregate;
//			result[2] = (aggregate.equals("avg") || function == 1)?"double":"ulong";
//		} else if (d <= 0.6) {
//			// selective
//			String aggregate = random.nextDouble()<=0.8?"max":"min";
//			result[0] = 3*multiplicator + function;
//			result[1] = aggregate;
//			result[2] = (function == 1)?"double":"ulong";
//		} else {
//			// count
//			result[0] = 3*multiplicator + 2;
//			result[1] = "sum";
//			result[2] = "int";
//		}
//		return result;
//	}
	
	private static boolean populateWTAttributes() {
		try {
			PreparedStatement prep = connection.prepareStatement("INSERT INTO " +
					"wt_attribute(id, metric, is_pivot, window_type, " +
					"window_size, aggr_fun, aggr_data_type) " +
					"VALUES (?,?,?,?,?,?,?);");
			
/** random algorithm */
//			for (int id = 1; id <= NUMBER_OF_WT_ATTRIBUTES; id++) {
//				Object[] aggregationParams = getRandomAggregationValues();
//				prep.setInt(1, id);
//				prep.setInt(2, (Integer) aggregationParams[0]);
//				prep.setInt(3, id > NUMBER_OF_PIVOT_ATTRIBUTES?0:1);
//				prep.setString(4, "tumb");
//				prep.setString(5, random.nextBoolean()?"d":"w");
//				prep.setString(6, (String) aggregationParams[1]);
//				prep.setString(7, (String) aggregationParams[2]);
//				prep.execute();
//				wtAttributeDatatypes[id] = (String) aggregationParams[2];
//			}
			
			String type;
			int id = 0;
			for (int r = 0; r < NUMBER_OF_REPETITIONS; r++) {
				for (String windowSize: new String[]{"d", "w"}) {
					for (int mult = 0; mult < 3; mult++) {
						// for cost and duration
						for (int func: new int []{1, 3}) {
							for (String aggregate: aggregateFunctions) {
								id++;
								type = (func == 1 || aggregate.equals("avg"))?
										"double":"uint";
								prep.setInt(1, id);
								prep.setInt(2, 3* mult + func);
								prep.setInt(3, id > NUMBER_OF_PIVOT_ATTRIBUTES?0:1);
								prep.setString(4, "tumb");
								prep.setString(5, windowSize);
								prep.setString(6, aggregate);
								prep.setString(7, type);
								prep.execute();
								wtAttributeDatatypes[id] = type;
							}
						}
						// for number of calls
						id++;
						type = "uint";
						prep.setInt(1, id);
						prep.setInt(2, 3* mult + 2);
						prep.setInt(3, id > NUMBER_OF_PIVOT_ATTRIBUTES?0:1);
						prep.setString(4, "tumb");
						prep.setString(5, windowSize);
						prep.setString(6, "sum");
						prep.setString(7, type);
						prep.execute();
						wtAttributeDatatypes[id] = type;
					}
				}
			}
			
			connection.commit();
			prep.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * creates a random constant of the given datatype and adds it to the db
	 * (if it doesn't exist already)
	 * 
	 * @param datatype
	 * @return the id of the created constant
	 */
	private static int createRandomConstant(String datatype) {
		int value = random.nextInt(90) + 11;
		String key = datatype + value;
		if (constants.containsKey(key)) {
			return constants.get(key);
		} else {
			int newId = constants.size()+1;
			try {
				addConstantStmt.setInt(1, newId);
				addConstantStmt.setString(2, datatype.equals("double")?
						Double.valueOf(value).toString():String.valueOf(value));
				addConstantStmt.setString(3, datatype);
				addConstantStmt.execute();
				constants.put(key, newId);
				return newId;
			} catch (SQLException e) {
				e.printStackTrace();
				return -1;
			}
		}
	}
	
	/**
	 * creates a random predicate and adds it to the db
	 * (if it doesn't exist already)
	 * 
	 * @param isPivotPredicate
	 * @return the id of the created predicate
	 */
	private static int createRandomPredicate(boolean isPivotPredicate) {
		int wtAttribute = isPivotPredicate?
				(random.nextInt(NUMBER_OF_PIVOT_ATTRIBUTES)+1):
					(NUMBER_OF_PIVOT_ATTRIBUTES +
							random.nextInt(NUMBER_OF_WT_ATTRIBUTES
									- NUMBER_OF_PIVOT_ATTRIBUTES) + 1);
		double d = random.nextDouble();
		String operator;
		if (d <= 0.4) {
			operator = "gr";
		} else if (d <= 0.5) {
			operator = "lt";
		} else if (d <= 0.9) {
			operator = "gre";
		} else {
			operator = "lte";
		}		
		int constant = createRandomConstant(wtAttributeDatatypes[wtAttribute]);
		String key = wtAttribute + operator + constant;
		if (predicates.containsKey(key)) {
			return predicates.get(key);
		} else {
			int newId = predicates.size()+1;
			try {
				addPredicateStmt.setInt(1, newId);
				addPredicateStmt.setInt(2, wtAttribute);
				addPredicateStmt.setString(3, operator);
				addPredicateStmt.setInt(4, constant);
				addPredicateStmt.execute();
				predicates.put(key, newId);
				return newId;
			} catch (SQLException e) {
				e.printStackTrace();
				return -1;
			}
			
		}
	}
	
	/**
	 * creates a random conjunct and adds it to the db
	 * 
	 * @param campaignId
	 * @return the id of the created conjunct
	 */
	private static int createRandomConjunct(int campaignId) {
		int id = ++conjunctIDCounter;
		try {
			// create conjunct entry
			addConjunctStmt.setInt(1, id);
			addConjunctStmt.setInt(2, campaignId);
			addConjunctStmt.execute();
			
			// add a random number of predicates
			int numberOfPredicates = random.nextInt(
					MAX_NUMBER_OF_PREDICATES_PER_CONJUNCT) + 1;
			// add pivot predicate (if at all)
			double d = random.nextDouble();
			addConjunctPredicateStmt.setInt(1, createRandomPredicate((d<=0.9)?true:false));
			addConjunctPredicateStmt.setInt(2, id);
			addConjunctPredicateStmt.execute();
			// add other predicates
			for (int i = 1; i < numberOfPredicates; i++) {
				addConjunctPredicateStmt.setInt(1, createRandomPredicate(false));
				addConjunctPredicateStmt.setInt(2, id);
				addConjunctPredicateStmt.execute();
			}
		} catch (SQLException e) {
			conjunctIDCounter--;
			e.printStackTrace();
			return -1;
		}
		return id;
	}
	
	private static boolean populateCampaigns(int numberOfCampaigns) {
		try {
			PreparedStatement prep = connection.prepareStatement("INSERT INTO " +
					"campaign (id, valid_from, valid_to, firing_interval, " +
					"firing_start_condition) VALUES (?,?,?,?,?);");
			
			Calendar start = Calendar.getInstance();
			start.clear();
			start.set(2012, Calendar.JANUARY, 1);
			
			int duration, randomStart, numberOfConjuncts;
			double d;
			String tmp;
			
			for (int i = 1; i <= numberOfCampaigns; i++) {
				prep.setInt(1, i);
				
				duration = random.nextInt(25) + 7;
				randomStart = random.nextInt(32-duration);
				start.add(Calendar.DAY_OF_YEAR, randomStart);
				prep.setLong(2, start.getTimeInMillis());
				start.add(Calendar.DAY_OF_YEAR, duration);
				prep.setLong(3, start.getTimeInMillis());
				
				// reset calendar
				start.add(Calendar.DAY_OF_YEAR, -randomStart - duration);
				
				d = random.nextDouble();
				if (d <= 0.2) {
					tmp = "0";
				} else if (d <= 0.4) {
					tmp = "1d";
				} else if (d <= 0.6) {
					tmp = "2d";
				} else {
					tmp = "w";
				}
				prep.setString(4, tmp);
				
				d = random.nextDouble();
				prep.setString(5, d<=0.9?"fixed":"sliding");
				prep.execute();
				
				numberOfConjuncts =
						random.nextInt(MAX_NUMBER_OF_CONJUNCTS_PER_CAMPAIGN) + 1;
				for (int c = 0; c < numberOfConjuncts; c++) {
					createRandomConjunct(i);
				}
				connection.commit();
			}
			prep.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	private static boolean populateDatabase(int seed, int numberOfCampaigns) {
		boolean result = true;
		random = new Random(seed);
		
		result = result && populateMetrics();
		result = result && populateWTAttributes();
		result = result && populateCampaigns(numberOfCampaigns);
		
		return result;
	}
	
	private static final String CAMPAIGN_FILE_NAME = "campaigns.html";
	
	private static boolean createCampaignFile(int numberOfCampaigns) {
		try {
			PreparedStatement prep1 = connection.prepareStatement(
					"SELECT * FROM campaign WHERE id = ?;");
			
			PreparedStatement prep2 = connection.prepareStatement("select c.id " +
					"as cid, p.id as pid, m.is_pivot, m.aggr_fun, d.name," +
					" p.operator, co.value from conjunct c, conjunct_predicate" +
					" cp, predicate p, constant co, wt_attribute m," +
					" metric d" +
					" WHERE cp.predicate = p.id AND cp.conjunct = c.id AND" +
					" p.wt_attribute = m.id AND p.constant = co.id AND" +
					" m.metric = d.id AND c.campaign = ?" +
					" ORDER BY c.id, p.id;");
			connection.setAutoCommit(true);
			
			DateFormat formatter = DateFormat.getDateInstance();
			
			FileWriter writer = new FileWriter(new File(CAMPAIGN_FILE_NAME));
			writer.write("<html><head><title>Campaigns</title></head><body>\n");
			int newConjunctID, oldConjunctID;
			for (int i = 1; i <= numberOfCampaigns; i++) {
				// write campaign attributes
				prep1.setInt(1, i);
				ResultSet result = prep1.executeQuery();
				result.next();
				writer.write("<h3>Campaign " + i + "</h3>\n" +
						"<table style='border-collapse: collapse'>\n" +
						"<tr><td><b>validity period:&nbsp;&nbsp;</b></td><td>");
				writer.write(formatter.format(new Date(
						result.getLong("valid_from"))));
				writer.write(" &ndash; ");
				writer.write(formatter.format(new Date(
						result.getLong("valid_to"))));
				writer.write("</td></tr>\n<tr><td><b>firing interval:</b>" +
						"</td><td>");
				writer.write(result.getString("firing_interval"));
				writer.write("</td></tr>\n<tr><td><b>firing start:</b>" +
						"</td><td>");
				writer.write(result.getString("firing_start_condition"));
				writer.write("</td></tr>\n<tr><td valign='top'>" +
						"<b>condition:<b/></td><td>");
				
				// write campaign condition
				prep2.setInt(1, i);
				result = prep2.executeQuery();
				oldConjunctID = -1;
				while (result.next()) {
					newConjunctID = result.getInt("cid");
					if (newConjunctID != oldConjunctID) {
						if (oldConjunctID > -1) {
							writer.write("] &or;<br/>");
						}
						writer.write("\n[");
						oldConjunctID = newConjunctID;
					} else {
						writer.write(" &and; ");
					}
					writer.write(result.getString("aggr_fun"));
					writer.write("(" + result.getString("name") + ") ");
					writer.write("&" + result.getString("operator")
							.replace("gr", "gt").replace("te", "e") + ";");
					writer.write(" " + result.getString("value"));
				}
				
				writer.write("]\n</ul>\n</td></tr>\n</table>");
			}
			writer.write("</body></html>");
			writer.flush();
			writer.close();
			prep1.close();
			prep2.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
	private static void checkError(boolean isCorrect) {
		if (!isCorrect) {
			System.out.println("An error occurred, program exits.");
			System.exit(0);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        // load SQLite-JDBC driver
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (Exception ex) {
            System.out.println("Warning: SQlite not in class path. You can still use mysql.");
        }
                
		// fill hash map with default parameters
		Map<String, String> params = new HashMap<String, String>();
        params.put("instance", DEFAULT_INSTANCE);
		params.put("host", DEFAULT_HOST);
		params.put("port", String.valueOf(DEFAULT_PORT));
		params.put("database", DEFAULT_DATABASE);
		params.put("user", DEFAULT_USER);
		params.put("password", DEFAULT_PASSWORD);
		params.put("scaling-factor", String.valueOf(DEFAULT_SCALING_FACTOR));
		params.put("seed", String.valueOf(DEFAULT_SEED));
		
		// store parameters in hash map
		String[] split;
		for (String arg: args) {
			split = arg.split("=");
			if (split.length == 2) {
				params.put(split[0], split[1]);
			} else {
				System.out.println("Argument " + args + " has no meaning.");
			}
		}
		
		int numberOfCampaigns = BASE_NUMBER_OF_CAMPAIGNS
				* Integer.valueOf(params.get("scaling-factor"));
		
		// output parameters
		System.out.println("\nData is generated with the follwing parameters:");
		for (String key: params.keySet()) {
			System.out.println(key + ": " + params.get(key));
		}
		System.out.println("Parameters can be changed by adding arguments of " +
				"the form param=value...\n");
		
		// output record size
		System.out.println("Record size: " + RECORD_SIZE + " bytes (" +
				NUMBER_OF_WT_ATTRIBUTES + " columns)\n");
		
		// create connection
		checkError(createConnection(params.get("instance"),
                                params.get("host"),
				Integer.valueOf(params.get("port")),
				params.get("database"), params.get("user"),
				params.get("password")));
		
		// create schema
		System.out.print("Creating schema...");
		checkError(createDatabaseSchema());
		System.out.println("done");
		
		// create schema
		System.out.print("Populating database...");
		checkError(populateDatabase(Integer.valueOf(params.get("seed")), numberOfCampaigns));
		System.out.println("done");
		
		// write campaign file
		System.out.print("Writing campaign file (campaigns.html)...");
		checkError(createCampaignFile(numberOfCampaigns));
		System.out.println("done");
		
		// close all the connections
		try {
			addConjunctPredicateStmt.close();
			addConjunctStmt.close();
			addConstantStmt.close();
			addPredicateStmt.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
