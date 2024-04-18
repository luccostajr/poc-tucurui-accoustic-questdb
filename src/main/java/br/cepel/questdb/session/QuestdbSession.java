package br.cepel.questdb.session;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.questdb.client.Sender;

/**
 * The QuestdbSession class is a class for managing the QuestDB session. It
 * provides methods for creating a sender, creating a PostgreSQL connection,
 * getting the number of columns of a query, getting the number of result rows
 * of a query execution, creating a database statement based on a query and its
 * parameters, getting the resultset based on a query, its parameters and a page
 * size, getting the result of a query execution as a list of rows, each
 * containing several columns, and executing a query.
 * 
 * @note 1.1: use of default values for the QuestDB properties.
 * 
 * @version 1.1
 * @since 2024-03-07
 */
@SuppressWarnings("unused")
public final class QuestdbSession {
  private static Logger logger = LogManager.getLogger(QuestdbSession.class);

  private static final String QUESTDB_VERSION = "7.4.0";
  private static final int NUM_MAX_ERRORS = 10;

  private static final int QUESTDB_BUFFER_CAPACITY = 1000;
  private static final int QUESTDB_FLUSH_LIMIT = 10;
  private static final int QUESTDB_RETRY_TIMEOUT = 10;
  private static final String DEFAULT_ILP_CONNECTION_TYPE = "TCP";

  public static final String QDB_DEFAULT_ID = "soma";

  private static final String QUESTDB_USE_TAG = ".questdb.use";
  private static final String QUESTDB_INFXPORT_TAG = ".questdb.infxport";
  private static final String QUESTDB_HOST_TAG = ".questdb.host";
  private static final String QUESTDB_SSLMODE_TAG = ".questdb.sslmode";
  private static final String QUESTDB_URL_TAG = ".questdb.url";
  private static final String QUESTDB_DRIVER_TAG = ".questdb.driver";
  private static final String QUESTDB_PASSW_TAG = ".questdb.pass";
  private static final String QUESTDB_USER_TAG = ".questdb.user";
  private static final String QUESTDB_INFXURL_TAG = ".questdb.infxurl";

  public static final String DEFAULT_QUESTDB_USE_TAG = QDB_DEFAULT_ID + QUESTDB_USE_TAG;
  public static final String DEFAULT_QUESTDB_INFXPORT_TAG = QDB_DEFAULT_ID + QUESTDB_INFXPORT_TAG;
  public static final String DEFAULT_QUESTDB_HOST_TAG = QDB_DEFAULT_ID + QUESTDB_HOST_TAG;
  public static final String DEFAULT_QUESTDB_SSLMODE_TAG = QDB_DEFAULT_ID + QUESTDB_SSLMODE_TAG;
  public static final String DEFAULT_QUESTDB_URL_TAG = QDB_DEFAULT_ID + QUESTDB_URL_TAG;
  public static final String DEFAULT_QUESTDB_DRIVER_TAG = QDB_DEFAULT_ID + QUESTDB_DRIVER_TAG;
  public static final String DEFAULT_QUESTDB_PASSW_TAG = QDB_DEFAULT_ID + QUESTDB_PASSW_TAG;
  public static final String DEFAULT_QUESTDB_USER_TAG = QDB_DEFAULT_ID + QUESTDB_USER_TAG;
  public static final String DEFAULT_QUESTDB_INFXURL_TAG = QDB_DEFAULT_ID + QUESTDB_INFXURL_TAG;

  private static final String DEFAULT_QUESTDB_USE_VALUE = "true";
  private static final String DEFAULT_QUESTDB_PASSW_VALUE = "quest";
  private static final String DEFAULT_QUESTDB_USER_VALUE = "admin";
  private static final String DEFAULT_QUESTDB_DRIVER_VALUE = "org.postgresql.Driver";
  private static final String DEFAULT_QUESTDB_SSLMODE_VALUE = "disable";
  private static final String DEFAULT_QUESTDB_HOST_VALUE = "localhost";
  private static final String DEFAULT_QUESTDB_INFXPORT_VALUE = "9009";
  private static final String DEFAULT_QUESTDB_INFXURL_VALUE = "localhost:9009";
  private static final String DEFAULT_QUESTDB_URL_VALUE = "jdbc:postgresql://localhost:8812/qdb";

  protected String questdbUseTag = DEFAULT_QUESTDB_USE_TAG;
  protected String questdbInfxPortTag = DEFAULT_QUESTDB_INFXPORT_TAG;
  protected String questdbHostTag = DEFAULT_QUESTDB_HOST_TAG;
  protected String questdbSslModeTag = DEFAULT_QUESTDB_SSLMODE_TAG;
  protected String questdbUrlTag = DEFAULT_QUESTDB_URL_TAG;
  protected String questdbDriverTag = DEFAULT_QUESTDB_DRIVER_TAG;
  protected String questdbPasswTag = DEFAULT_QUESTDB_PASSW_TAG;
  protected String questdbUserTag = DEFAULT_QUESTDB_USER_TAG;
  protected String questdbInfluxUrlTag = DEFAULT_QUESTDB_INFXURL_TAG;

  private String qdbDatabaseId = QDB_DEFAULT_ID;
  private Properties questdbProperties = null;

  private static Map<String, Connection> connectionMap = new java.util.HashMap<String, Connection>();

  private static String senderInfluxUrl = null;

  /**
   * The constructor for the QuestdbSession class.
   * 
   * @implNote The constructor is private to prevent the creation of multiple
   *           instances of the QuestdbSession class.
   * @param qdbDatabaseId the system identification used to access the specific
   *                      QuestDB database and configurations (empty =
   *                      QDB_DEFAULT_ID).
   * @throws Exception if an error occurs
   */
  public QuestdbSession(String qdbDatabaseId) throws Exception {
    super();

    this.qdbDatabaseId = qdbDatabaseId;
    defineConnectionProperties();

    logger.trace(
        "\n" +
            "\n\t" + "  ___                  _   ____  ____" +
            "\n\t" + " / _ \\ _   _  ___  ___| |_|  _ \\| __ )" +
            "\n\t" + "| | | | | | |/ _ \\/ __| __| | | |  _ \\" +
            "\n\t" + "| |_| | |_| |  __/\\__ \\ |_| |_| | |_) |" +
            "\n\t" + " \\__\\_\\\\__,_|\\___||___/\\__|____/|____/" +
            "\n");

    logger.info(
        "\nQuestDB Instance [" + qdbDatabaseId.toUpperCase() + "]: " +
            "\n\t'" + questdbUseTag + "': '" + System.getProperty(questdbUseTag) + "'" +
            "\n\t'" + questdbSslModeTag + "': '" + System.getProperty(questdbSslModeTag) + "'" +
            "\n\t'" + questdbUrlTag + "': '" + System.getProperty(questdbUrlTag) + "'" +
            "\n\t'" + questdbInfluxUrlTag + "': '" + System.getProperty(questdbInfluxUrlTag) + "'");

    defineInfluxUrl();
  }

  public QuestdbSession() throws Exception {
    this(QDB_DEFAULT_ID);
  }

  private void defineInfluxUrl() throws Exception {
    senderInfluxUrl = questdbProperties.getProperty(questdbInfluxUrlTag);
    if (senderInfluxUrl == null || senderInfluxUrl.trim().isEmpty()) {
      throw new Exception(
          "*** Error creating QuestDB InfluxDB Sender for [" + qdbDatabaseId.toUpperCase() + "]: "
              + "InfluxDB URL is null or empty.");
    }
  }

  private void setProperty(String propertyTag, String propertyDefault) {
    String propertyValue = System.getProperty(propertyTag);
    if (propertyValue == null || propertyValue.trim().isEmpty()) {
      propertyValue = System.getenv(propertyTag);
      if (propertyValue == null || propertyValue.trim().isEmpty()) {
        logger.warn("Questdb parameter for [" + qdbDatabaseId.toUpperCase() + ":" +
            propertyTag + "] is undefined in System and Environment vars. Setting default value: [" + propertyDefault
            + "] ...");
        propertyValue = propertyDefault;
      }
    }
    logger.debug("Questdb parameter for [" + qdbDatabaseId.toUpperCase() + ":" +
        propertyTag + "] is defined as: [" + propertyValue + "]");
    questdbProperties.setProperty(propertyTag, propertyValue);
    System.setProperty(propertyTag, propertyValue);
  }

  /**
   * The method for checking the configuration parameters and setting the default
   * values if its values are not defined.
   */
  private void getConfigurationProperties() {
    questdbProperties = new Properties();

    String propertyTag = questdbUseTag;
    String propertyDefault = DEFAULT_QUESTDB_USE_VALUE;
    setProperty(propertyTag, propertyDefault);

    propertyTag = questdbInfxPortTag;
    propertyDefault = DEFAULT_QUESTDB_INFXPORT_VALUE;
    setProperty(propertyTag, propertyDefault);

    propertyTag = questdbHostTag;
    propertyDefault = DEFAULT_QUESTDB_HOST_VALUE;
    setProperty(propertyTag, propertyDefault);

    propertyTag = questdbSslModeTag;
    propertyDefault = DEFAULT_QUESTDB_SSLMODE_VALUE;
    setProperty(propertyTag, propertyDefault);

    propertyTag = questdbUrlTag;
    propertyDefault = DEFAULT_QUESTDB_URL_VALUE;
    setProperty(propertyTag, propertyDefault);

    propertyTag = questdbDriverTag;
    propertyDefault = DEFAULT_QUESTDB_DRIVER_VALUE;
    setProperty(propertyTag, propertyDefault);

    propertyTag = questdbPasswTag;
    propertyDefault = DEFAULT_QUESTDB_PASSW_VALUE;
    setProperty(propertyTag, propertyDefault);

    propertyTag = questdbUserTag;
    propertyDefault = DEFAULT_QUESTDB_USER_VALUE;
    setProperty(propertyTag, propertyDefault);

    propertyTag = questdbInfluxUrlTag;
    propertyDefault = DEFAULT_QUESTDB_INFXURL_VALUE;
    setProperty(propertyTag, propertyDefault);
  }

  /**
   * The method for defining the connection properties.
   * 
   * @return a Properties object with the connection properties (@see
   *         java.util.Properties).
   */
  protected void defineConnectionProperties() {
    questdbUseTag = qdbDatabaseId + QUESTDB_USE_TAG;
    questdbInfxPortTag = qdbDatabaseId + QUESTDB_INFXPORT_TAG;
    questdbHostTag = qdbDatabaseId + QUESTDB_HOST_TAG;
    questdbSslModeTag = qdbDatabaseId + QUESTDB_SSLMODE_TAG;
    questdbUrlTag = qdbDatabaseId + QUESTDB_URL_TAG;
    questdbDriverTag = qdbDatabaseId + QUESTDB_DRIVER_TAG;
    questdbPasswTag = qdbDatabaseId + QUESTDB_PASSW_TAG;
    questdbUserTag = qdbDatabaseId + QUESTDB_USER_TAG;
    questdbInfluxUrlTag = qdbDatabaseId + QUESTDB_INFXURL_TAG;

    getConfigurationProperties();
  }

  /**
   * The method for getting the QuestDB properties.
   * 
   * @return a Properties object with the QuestDB properties (@see
   *         java.util.Properties)
   */
  public Properties getQuestdbProperties() {
    return questdbProperties;
  }

  /**
   * The method for creating a sender. The sender is used to send data to the
   * Questdb InfluxDB server.
   * 
   * @return a sender (@see io.questdb.client.Sender)
   * @throws Exception if an error occurs
   */
  public synchronized static Sender createSender() {
    return Sender.builder()
        .address(senderInfluxUrl)
        .build();
  }

  @Deprecated
  public synchronized Sender createSenderOld(String connectionType) throws Exception {
    String influxUrl = questdbProperties.getProperty(questdbInfluxUrlTag);
    if (influxUrl == null || influxUrl.trim().isEmpty()) {
      throw new Exception(
          "*** Error creating QuestDB InfluxDB Sender for [" + qdbDatabaseId.toUpperCase() + "]: "
              + "InfluxDB URL is null or empty.");
    }

    @SuppressWarnings("unused")
    String connInfluxUrl = null;

    Sender sender = null;
    switch (connectionType) {
      /*
       * case "HTTP":
       * connInfluxUrl = "http::addr=" + influxUrl + ";";
       * // sender = Sender.builder(Sender.Transport.HTTP) // Questdb 7.4.0
       * sender = Sender.builder()
       * .http()
       * .address(influxUrl)
       * .autoFlushRows(QUESTDB_FLUSH_LIMIT)
       * .retryTimeoutMillis(QUESTDB_RETRY_TIMEOUT)
       * .bufferCapacity(QUESTDB_BUFFER_CAPACITY)
       * .build();
       * break;
       * 
       * case "TCP":
       * connInfluxUrl = "tcp::addr=" + influxUrl + ";";
       * // sender = Sender.builder(Sender.Transport.TCP) // Questdb 7.4.0
       * sender = Sender.builder()
       * .tcp()
       * .address(influxUrl)
       * .bufferCapacity(QUESTDB_BUFFER_CAPACITY)
       * .build();
       * break;
       */

      default:
        sender = Sender.builder()
            .address(influxUrl)
            .bufferCapacity(QUESTDB_BUFFER_CAPACITY)
            .build();
        break;
    }

    if (sender == null) {
      String errorMessage = "\n*** Error creating QuestDB InfluxDB Sender for [" + qdbDatabaseId.toUpperCase() + "]: "
          + "InfluxDB URL ('" + connectionType + "'): ['" + influxUrl + "']";
      logger.error(errorMessage);
      throw new Exception(errorMessage);
    } else {
      logger.debug("QuestDB Sender for [" + qdbDatabaseId.toUpperCase() + "] created.\nInfluxDB URL ('" + connectionType
          + "'): ['" + influxUrl + "']");
    }

    return sender;
  }

  // public synchronized Sender createSender() throws Exception {
  //   return createSender(DEFAULT_ILP_CONNECTION_TYPE);
  // }

  /**
   * The method for creating a PostgreSQL connection for the Questdb SQL queries.
   * 
   * @return a connection (@see java.sql.Connection)
   * @throws SQLException         if a SQL error occurs
   * @throws Exception if an error occurs
   */
  private synchronized Connection createConnection() throws SQLException, Exception {
    Connection connection = null;
    try {
      logger.trace("Creating QuestDB Postgresql connection for [" + qdbDatabaseId.toUpperCase() + "] ...");
      Properties connectionProperties = new Properties();
      connectionProperties.setProperty("user", questdbProperties.getProperty(questdbUserTag));
      connectionProperties.setProperty("password", questdbProperties.getProperty(questdbPasswTag));
      connectionProperties.setProperty("sslmode", questdbProperties.getProperty(questdbSslModeTag));
      connection = DriverManager.getConnection(
          questdbProperties.getProperty(questdbUrlTag), connectionProperties);
    } catch (Exception e) {
      logger.error("*** Error creating QuestDB connection for [" + qdbDatabaseId.toUpperCase() + "]: "
          + e.getMessage());
      throw new Exception(e.getMessage());
    }

    logger.trace("QuestDB Postgresql connection for [" + qdbDatabaseId.toUpperCase() + "] created.");
    return connection;
  }

  /**
   * The method for getting a existing connection or creating a new one.
   * 
   * @return a connection (@see java.sql.Connection)
   * @throws SQLException         if a SQL error occurs
   * @throws Exception if an error occurs
   */
  public synchronized Connection getConnection()
      throws SQLException, Exception {
    Connection connection = connectionMap.get(qdbDatabaseId);
    if (connection == null) {
      connection = createConnection();
      connectionMap.put(qdbDatabaseId, connection);
    } 

    return connection;
  }

  protected synchronized void recreateConnection() throws SQLException, Exception {
    try {
      Connection connection = connectionMap.get(qdbDatabaseId);
      if (connection != null)
        stop(qdbDatabaseId);
    } catch (Exception t) {
    } 

    Connection connection = createConnection();
    connectionMap.put(qdbDatabaseId, connection);
  }

  /**
   * The method for getting the number of columns of a query.
   * 
   * @param rs the result set (@see java.sql.ResultSet)
   * @return the number of columns
   * @throws SQLException if an error occurs
   */
  private synchronized int getColumnsCount(ResultSet rs) throws SQLException {
    ResultSetMetaData rsmd = rs.getMetaData();
    return rsmd.getColumnCount();
  }

  /**
   * The method for setting the row of a result set.
   * 
   * @param rs       the result set (@see java.sql.ResultSet)
   * @param position the position of the row
   * @throws SQLException if an error occurs
   */
  private synchronized void setRowPosition(ResultSet rs, long position) throws SQLException {
    rs.beforeFirst();
    for (long count = 0; count < position; count++) {
      rs.next();
    }
  }

  /**
   * The method for getting the number of result rows of a query execution.
   * 
   * @param rs the result set (@see java.sql.ResultSet)
   * @return the number of rows
   * @throws SQLException if an error occurs
   */
  private synchronized int getRowsCount(ResultSet rs) throws SQLException {
    int currentPosition = rs.getRow();

    rs.last();
    int rowsCount = rs.getRow();

    setRowPosition(rs, currentPosition);
    return rowsCount;
  }

  private boolean isNumber(Object value) {
    return value instanceof Number;
  }

  /**
   * The method for creating a database statement based on a query and its
   * parameters.
   * 
   * @param connection the connection (@see java.sql.Connection)
   * @param query      the query
   * @param parameters the parameters
   * @param pageSize   the page size (optional = null)
   * @return a prepared statement (@see java.sql.PreparedStatement)
   * @throws Exception if an error occurs
   */
  private synchronized PreparedStatement createStatement(Connection connection, String query,
      Set<Entry<String, Object>> parameters, Integer pageSize) throws Exception {
    if (query == null || query.trim().isEmpty()) {
      throw new Exception(
          "Error in QuestDB createStatement for [" + qdbDatabaseId.toUpperCase()
              + "]: the submitted query is null or empty.");
    }

    try {
      logger.trace("Creating QuestDB statement for [" + qdbDatabaseId.toUpperCase() + "]: " +
          "Submitted query: [" + query + "], parameters: [" + parameters.toString()
          + "]" + (pageSize != null ? ", page size: [" + pageSize + "]" : ""));

      PreparedStatement stmt = connection.prepareStatement(
          query,
          ResultSet.TYPE_SCROLL_INSENSITIVE,
          ResultSet.CONCUR_READ_ONLY);

      if (parameters != null) {
        int position = 1;
        for (Entry<String, Object> parameter : parameters) {
          Object value = parameter.getValue();
          if (isNumber(value)) {
            stmt.setObject(position++, value);
          } else {
            stmt.setString(position++, value.toString());
          }
        }

        if (pageSize != null && pageSize > 0) {
          stmt.setMaxRows(pageSize);
          stmt.setFetchSize(pageSize);
        }
      }

      return stmt;
    } catch (SQLException e) {
      throw new Exception("Error in QuestDB createStatement for [" + qdbDatabaseId.toUpperCase()
          + "]: " + e.getMessage());
    }
  }

  /**
   * The method for getting the resultset based on a query, its parameters and a
   * page size.
   * 
   * @param stmt     the prepared statement (@see java.sql.PreparedStatement)
   * @param pageSize the page size
   * @return a result set (@see java.sql.ResultSet)
   * @throws Exception if an error occurs
   */
  private synchronized ResultSet creaResultSet(PreparedStatement stmt, Integer pageSize) throws Exception {
    try {
      ResultSet rs = stmt.executeQuery();

      if (pageSize != null && pageSize > 0)
        rs.setFetchSize(pageSize);
      rs.beforeFirst();

      return rs;
    } catch (SQLException e) {
      throw new Exception("Error in QuestDB createResultSet for [" + qdbDatabaseId.toUpperCase()
          + "]: " + e.getMessage());
    }
  }

  /**
   * The method for reading the result set of a query execution. The result set is
   * read and returned as a list of rows, each containing several columns.
   * 
   * @param rs the result set (@see java.sql.ResultSet)
   * @return a list of rows, each containing several columns
   * @throws Exception if an error occurs
   */
  private ConcurrentHashMap<String, Integer> countErrors = new ConcurrentHashMap<>();

  private synchronized List<Object[]> readResultSet(ResultSet rs) throws Exception {
    try {
      List<Object[]> resultList = new ArrayList<>();

      int columnsCount = getColumnsCount(rs);
      int rowsCount = getRowsCount(rs);

      if (rowsCount > 0) {
        while (rs.next()) {
          Object[] results = new Object[columnsCount];
          for (int i = 0; i < columnsCount; i++) {
            Object value = rs.getObject(i + 1);
            results[i] = value;
          }
          resultList.add(results);
        }
      }

      return resultList;
    } catch (SQLException e) {
      throw new Exception("Error in QuestDB readResultSet for [" + qdbDatabaseId.toUpperCase()
          + "]: " + e.getMessage());
    }
  }

  /**
   * The method for getting the result of a query execution as a list of rows,
   * each
   * containing several columns.
   * 
   * @param query      the query
   * @param parameters the parameters
   * @param pageSize   the page size
   * @return a list of rows, each containing several columns
   * @throws Exception if an error occurs
   */
  public synchronized List<Object[]> getResultsList(String query, Set<Entry<String, Object>> parameters,
      Integer pageSize)
      throws Exception {
    if (query == null || query.trim().isEmpty()) {
      throw new Exception("Error in QuestDB getResultsList for [" + qdbDatabaseId.toUpperCase()
          + "]: the submitted query is null or empty.");
    }

    logger.trace("QuestDB getResultsList for [" + qdbDatabaseId.toUpperCase() + "]: " +
        "Submitted query: [" + query + "], parameters: [" + parameters.toString() + "], page size: ["
        + pageSize + "]");

    try {
      List<Object[]> resultList = null;

      logger.trace("QuestDB getResultsList for [" + qdbDatabaseId.toUpperCase() + "]: " +
          "Creating connection, statement and result set...");
      Connection connection = getConnection();
      PreparedStatement stmt = createStatement(connection, query, parameters, pageSize);
      ResultSet rs = creaResultSet(stmt, pageSize);

      logger.trace("QuestDB getResultsList for [" + qdbDatabaseId.toUpperCase() + "]: " +
          "Reading result set...");
      resultList = readResultSet(rs);

      logger.trace("QuestDB getResultsList for [" + qdbDatabaseId.toUpperCase() + "]: " +
          "Closing result set, statement and connection...");
      rs.close();
      stmt.close();
      // connection.close();
      countErrors.remove(qdbDatabaseId);

      return resultList;
    } catch (Exception e) {
      Integer count = countErrors.get(qdbDatabaseId);
      if (count == null) {
        count = 1;
      } else {
        count++;
      }
      if (count > NUM_MAX_ERRORS) {
        countErrors.remove(qdbDatabaseId);
        throw new Exception("Error in QuestDB getResultsList for [" + qdbDatabaseId.toUpperCase()
            + "], query: [" + query.toUpperCase() + "], parameters: [" + parameters.toString() + "]: "
            + e.getMessage());
      } else {
        countErrors.put(qdbDatabaseId, count);
        try {
          recreateConnection();
        } catch (Exception e1) {
          logger.error("Error in QuestDB recreateConnection for [" + qdbDatabaseId.toUpperCase() + "]: "
              + e1.getMessage());
          countErrors.remove(qdbDatabaseId);
          return null;
        }
        return getResultsList(query, parameters, pageSize);
      }
    }
  }

  /**
   * The method for getting the result of a query execution as a list of rows,
   * each
   * containing several columns.
   * 
   * @param query      the query
   * @param parameters the parameters
   * @param pageSize   the page size
   * @return a list of rows, each containing several columns
   * @throws Exception if an error occurs
   */
  public synchronized void executeQuery(String query, Set<Entry<String, Object>> parameters)
      throws Exception {
    if (query == null || query.trim().isEmpty()) {
      throw new Exception("Error in QuestDB getResultsList for [" + qdbDatabaseId.toUpperCase()
          + "]: the submitted query is null or empty.");
    }

    logger.trace("QuestDB executeQuery for [" + qdbDatabaseId.toUpperCase() + "]: " +
        "Submitted query: [" + query + "], parameters: [" + parameters.toString() + "]");

    try {
      logger.trace("QuestDB executeQuery for [" + qdbDatabaseId.toUpperCase() + "]: " +
          "Creating connection and statement ...");

      Connection connection = getConnection();
      PreparedStatement stmt = createStatement(connection, query, parameters, null);
      stmt.executeUpdate(query);
      stmt.close();
      // connection.close();
      countErrors.remove(qdbDatabaseId);
    } catch (SQLException e) {
      Integer count = countErrors.get(qdbDatabaseId);
      if (count == null) {
        count = 1;
      } else {
        count++;
      }
      if (count > NUM_MAX_ERRORS) {
        countErrors.remove(qdbDatabaseId);
        throw new Exception("Error in QuestDB executeQuery for [" + qdbDatabaseId.toUpperCase()
            + "], query: [" + query.toUpperCase() + "], parameters: [" + parameters.toString() + "]: "
            + e.getMessage());
      } else {
        countErrors.put(qdbDatabaseId, count);
        try {
          recreateConnection();
        } catch (Exception e1) {
          logger.error("Error in QuestDB recreateConnection for [" + qdbDatabaseId.toUpperCase() + "]: "
              + e1.getMessage());
          countErrors.remove(qdbDatabaseId);
          return;
        }
        executeQuery(query, parameters);
      }
    }
  }

  /**
   * The method for executing a SQL query.
   * 
   * @param query the SQL query
   * @throws Exception if an error occurs
   */
  public synchronized void executeQuery(String query) throws Exception {
    try {
      Connection connection = getConnection();
      Statement stmt = connection.createStatement();
      stmt.executeUpdate(query);
      stmt.close();
      // connection.close();
      countErrors.remove(qdbDatabaseId);
    } catch (Exception e) {
      Integer count = countErrors.get(qdbDatabaseId);
      if (count == null) {
        count = 1;
      } else {
        count++;
      }
      if (count > NUM_MAX_ERRORS) {
        countErrors.remove(qdbDatabaseId);
        throw new Exception("Error in QuestDB executeQuery for [" + qdbDatabaseId.toUpperCase()
            + "], query: [" + query.toUpperCase() + "]: " + e.getMessage());
      } else {
        countErrors.put(qdbDatabaseId, count);
        try {
          recreateConnection();
        } catch (Exception e1) {
          logger.error("Error in QuestDB recreateConnection for [" + qdbDatabaseId.toUpperCase() + "]: "
              + e1.getMessage());
          countErrors.remove(qdbDatabaseId);
          return;
        }
        executeQuery(query);
      }
    }
  }

  /**
   * The method for checking the connection to the Questdb via PostgreSQL driver.
   * 
   * @return a message with the connection status or null if the connection is OK
   * @throws Exception if an error occurs
   */
  public synchronized String checkConnection() throws Exception {
    try {
      logger.trace("Checking QuestDB connection for [" + qdbDatabaseId.toUpperCase() + "] ...");
      executeQuery("SELECT 1");
      logger.trace("QuestDB connection for [" + qdbDatabaseId.toUpperCase() + "] is OK.");
      return null;
    } catch (Exception e) {
      return "Error in QuestDB checkConnection for [" + qdbDatabaseId.toUpperCase() + "]: " + e.getMessage();
    }
  }

  /**
   * The method for stopping the Questdb session.
   * 
   * @throws SQLException if an error occurs
   */
  public void stop(String qdbDatabaseId) throws SQLException {
    Connection connection = connectionMap.remove(qdbDatabaseId);
    connection.close();
    connection = null;
  }

  /**
   * The method for stopping all Questdb sessions.
   * 
   * @throws SQLException if an error occurs
   */
  public static void stopAll() throws SQLException {
    for (Connection connection : connectionMap.values()) {
      connection.close();
    }
    connectionMap.clear();
  }

  /**
   * The method for getting the QuestDB database identification.
   * 
   * @return the QuestDB database identification
   */
  public String getQdbDatabaseId() {
    return qdbDatabaseId;
  }

  /**
   * The method for verifying if the QuestDB is in use.
   * 
   * @return true if the QuestDB is in use, false otherwise
   */
  public synchronized static Boolean useQuestdb() {
    return useQuestdb(QuestdbSession.DEFAULT_QUESTDB_USE_TAG);
  }

  private synchronized static Boolean useQuestdb(String questdbUseTag) {
    Boolean useQuestDb = false;
    String strUseQuestDb = System.getProperty(questdbUseTag);
    if (strUseQuestDb == null || strUseQuestDb.isEmpty()) {
      strUseQuestDb = System.getenv(questdbUseTag);
      if (strUseQuestDb == null || strUseQuestDb.isEmpty()) {
        logger.warn(
            "*** Questdb Use: Unable to parse system and environment variable '" + questdbUseTag
                + "' - defaulting to 'false'.");
        strUseQuestDb = "false";
      }
    }

    try {
      logger.trace("Questdb Use: Parsing '" + questdbUseTag + "' (boolean value: '" + strUseQuestDb + "').");
      useQuestDb = Boolean.parseBoolean(strUseQuestDb);
    } catch (Exception e) {
      logger.warn(
          "*** Questdb Use: Error parsing '" + questdbUseTag + "' (boolean value: '" + strUseQuestDb
              + "') - defaulting to 'false': " + e.getMessage());
      useQuestDb = false;
    }
    return useQuestDb;
  }

}
