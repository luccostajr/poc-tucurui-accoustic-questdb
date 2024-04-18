package br.cepel.questdb.business;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import br.cepel.helper.Helper;
import br.cepel.questdb.base.BaseQdbBO;
import br.cepel.questdb.model.Accoustic;
import br.cepel.questdb.session.QuestdbSession;
import io.questdb.client.Sender;

/**
 * This class is a concrete implementation of the abstract class
 * TimeseriesQdbBO. It is responsible for managing the persistence of
 * Accoustic objects in the QuestDB database.
 * 
 * @since 2024-04-18
 * @version 1.0 - 2024-04-18 - Initial implementation
 */
public class AccousticQdbBO extends BaseQdbBO {
  private static final Logger logger = LogManager.getLogger(AccousticQdbBO.class);
  public static final String QDB_ID = "accoustic";

  /**
   * The AccousticQdbQueries object. It is responsible for managing the queries
   * for
   * the AccousticQdbBO object.
   */
  private static AccousticQdbQueries accousticQdbQueries = null;

  /**
   * The semaphore for controlling access to the QuestDB database. It is used to
   * synchronize access to the QuestDB database in the delete / update operations.
   */
  protected static Semaphore localSemaphore = new Semaphore(1);

  @SuppressWarnings("unused")
  private static String timestampColumnName = null;
  private static String tableName = null;
  private static String idColumnName = null;
  private static String timeColumnName = null;
  private static String frequencyColumnName = null;
  private static String amplitudeColumnName = null;

  @SuppressWarnings("unused")
  private static QuestdbSession questdbSession = null;

  @SuppressWarnings("unused")
  private static ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

  /**
   * The constructor for the AccousticQdbBO class.
   * 
   * @implNote This method is responsible for creating the AccousticQdbQueries
   *           object
   *           and the QuestDB table if it does not exist.
   * @throws Exception if an error occurs
   */
  public AccousticQdbBO(String qdbDatabaseId, Semaphore semaphore) throws Exception {
    super(qdbDatabaseId, semaphore);
    if (accousticQdbQueries == null) {
      accousticQdbQueries = new AccousticQdbQueries();
      createTableIfNotExists();

      tableName = getQdbQueries().getTableName();
      idColumnName = getQdbQueries().getIdColumnName();
      timeColumnName = getQdbQueries().getTimeColumnName();
      frequencyColumnName = getQdbQueries().getFrequencyColumnName();
      amplitudeColumnName = getQdbQueries().getAmplitudeColumnName();
      timestampColumnName = getQdbQueries().getTimestampColumnName();

      try {
        questdbSession = getQuestdbSession(getQdbDatabaseId());
      } catch (Exception e) {
        String errorMessage = "*** Error getting QuestDB session: " + e.getMessage();
        logger.error(errorMessage);
        throw new Exception(errorMessage);
      }
    }
    logger.trace("QuestDB Accoustic Persistence created.");
  }

  public AccousticQdbBO() throws Exception {
    this(QDB_ID, localSemaphore);
  }

  @Override
  protected AccousticQdbQueries getQdbQueries() {
    if (accousticQdbQueries == null) {
      accousticQdbQueries = new AccousticQdbQueries();
    }
    return accousticQdbQueries;
  }

  /**
   * This method is responsible for returning the QuestDB database id. It is used
   * to manage the QuestDB database.
   * 
   * @return the QuestDB database id
   */
  @Override
  protected String getQdbDatabaseId() {
    return QDB_ID;
  }

  /**
   * This method is responsible for creating the query command for the
   * Accoustic table in the QuestDB database.
   * 
   * @return the query command
   * @throws Exception if an error occurs
   */
  @Override
  protected String getCreateSQLCommand() {
    String createQuery = "CREATE TABLE IF NOT EXISTS " +
        getQdbQueries().getTableName() + " (" +
        getQdbQueries().getIdColumnName() + " SYMBOL CAPACITY " + getQdbQueries().getSymbolCapacity()
        + " NOCACHE INDEX CAPACITY " + getQdbQueries().getIndexCapacity() + ", " +
        getQdbQueries().getTimeColumnName() + " double, " +
        getQdbQueries().getAmplitudeColumnName() + " double, " +
        getQdbQueries().getFrequencyColumnName() + " double, " +
        getQdbQueries().getTimestampColumnName() + " timestamp) " +
        "timestamp(" + getQdbQueries().getTimestampColumnName()
        + ") PARTITION BY " + getQdbQueries().getPartitionBy() + " WAL DEDUP UPSERT KEYS("
        + getQdbQueries().getTimestampColumnName() + ", "
        + getQdbQueries().getIdColumnName() + ")";
    logger.trace("Create table sql command: '" + createQuery.toUpperCase() + "'.");
    return createQuery;
  }

  // BUSINESS METHODS

  /**
   * This method is responsible for retrieving the semaphore for controlling
   * access
   * to the QuestDB database. It is used to synchronize access to the QuestDB
   * database in the delete / update operations.
   * 
   * @return the Semaphore object
   */
  protected Semaphore getLocalSemaphore() {
    return AccousticQdbBO.localSemaphore;
  }

  /**
   * This method is responsible for inserting a Accoustic object
   * into the QuestDB database.
   * 
   * @param dsId the uuid of the data source
   * @param data the Accoustic object (@see Accoustic)
   * @return the Accoustic object (@see Accoustic)
   * @throws Exception if an error occurs
   */
  @SuppressWarnings("unchecked")
  public synchronized void insert(Object dsId, Object data) throws Exception {
    try {
      List<String[]> lines = (List<String[]>) data;
      if (dsId == null) {
        throw new Exception("*** Accoustic QuestDB insert: dsId is null. Unable to insert waveform.");
      }

      if (lines == null || lines.size() == 0) {
        throw new Exception(
            "*** Accoustic QuestDB insert for dsId [" + dsId + "]: Accoustic list is null or empty.");
      }

      String dsIdStr = dsId.toString();
      try (Sender sender = QuestdbSession.createSender()) {
        System.out.println("Inserting Accoustic list in QuestDB for dsId [" + dsId + "]. Size: " + lines.size());

        for (String[] dataArray : lines) {
          Double timeValue = dataArray[0] == null ? -1 : Double.parseDouble(dataArray[0]);
          Double frequencyValue = dataArray[1] == null ? -1 : Double.parseDouble(dataArray[1]);
          Double amplitudeValue = dataArray[2] == null ? -1 : Double.parseDouble(dataArray[2]);

          sender
              .table(tableName)
              .symbol(idColumnName, dsIdStr)
              .doubleColumn(timeColumnName, timeValue)
              .doubleColumn(frequencyColumnName, amplitudeValue)
              .doubleColumn(amplitudeColumnName, frequencyValue)
              .at(System.nanoTime(), ChronoUnit.NANOS);

          sender.flush();

          // The process is so fast that we need to sleep a little bit (zero!) to avoid
          // data loss in QuestDB due to same timestamp (in nano) for different data
          Helper.sleep(0);
        }
      } catch (Throwable t) {
        logger.error(
            "*** Failure in loop of insertion of a Buffer Accoustic Waveform in QuestDB:\nError: " + "'"
                + t.getMessage() + "'. Possible data loss.");
      } finally {
      }
    } catch (Exception e) {
      String errorMessage = "*** Error inserting Accoustic list in QuestDB:\nError: " + e.getMessage();
      logger.warn(errorMessage);
      throw new Exception(errorMessage);
    }
  }

  // BUSINESS METHODS

  /**
   * This method is responsible for retrieving the Accoustic waveform data
   * (sample and source) for a given indicator and time range.
   * 
   * @param dsId             the uuid of the data source
   * @param initialTimeStamp the initial time stamp
   * @param endTimeStamp     the end time stamp
   * @return a list of Object arrays containing the timestamp, sample and source
   *         values
   * @throws Exception if an error occurs
   */
  @Override
  public List<Object[]> getByIndicatorAndTime(Object dsId, Long initialTimeStamp, Long endTimeStamp)
      throws Exception {
    if (dsId == null) {
      logger.warn("*** Accoustic QuestDB getByIndicatorAndTime: dsId is null. Unable to execute.");
      return null;
    }

    if (initialTimeStamp == null || endTimeStamp == null) {
      logger.warn("*** Accoustic QuestDB getByIndicatorAndTime for dsId [" + dsId
          + "]: initialTimeStamp or endTimeStamp is null. Unable to execute.");
      return null;
    }

    String query = getQdbQueries().getQuery(AccousticQdbQueries.GET_BY_INDICATOR_AND_TIME_QUERY);

    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("dsUuid", dsId);
    parameters.put("initialTimeStamp", initialTimeStamp);
    parameters.put("endTimeStamp", endTimeStamp);

    List<Object[]> results = (List<Object[]>) getResultList(query, parameters.entrySet());
    return results;
  }

  /**
   * This method is responsible for retrieving the Accoustic waveform data
   * (sample and source) for a given indicator after a given time.
   * 
   * @param dsId             the uuid of the data source
   * @param initialTimeStamp the initial time stamp
   * @param pageSize         the page size
   * @return a list of Object arrays containing the timestamp, sample and source
   *         values
   * @throws Exception if an error occurs
   */
  @Override
  public List<Object[]> getByIndicatorAfterTime(Object dsId, Long initialTimeStamp, Integer pageSize)
      throws Exception {
    if (dsId == null) {
      logger.warn("*** Accoustic QuestDB getByIndicatorAfterTime: dsId is null. Unable to execute.");
      return null;
    }

    if (initialTimeStamp == null) {
      logger.warn("*** Accoustic QuestDB getByIndicatorAfterTime for dsId [" + dsId
          + "]: initialTimeStamp is null. Unable to execute.");
      return null;
    }

    String query = getQdbQueries().getQuery(AccousticQdbQueries.GET_BY_INDICATOR_AFTER_TIME_QUERY);
    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("dsUuid", dsId);
    parameters.put("initialTimeStamp", initialTimeStamp);

    List<Object[]> results = (List<Object[]>) getResultList(query, parameters.entrySet(), pageSize);
    return (List<Object[]>) results;
  }

  /**
   * This method is responsible for retrieving the Accoustic waveform data
   * (sample and source) for a given indicator before a given time.
   * 
   * @param dsId         the uuid of the data source
   * @param endTimeStamp the end time stamp
   * @param pageSize     the page size
   * @return a list of Object arrays containing the timestamp, sample and source
   *         values
   * @throws Exception if an error occurs
   */
  @Override
  public List<Object[]> getByIndicatorBeforeTime(Object dsId, Long endTimeStamp, Integer pageSize)
      throws Exception {
    if (dsId == null) {
      logger.warn("*** Accoustic QuestDB getByIndicatorBeforeTime: dsId is null. Unable to execute.");
      return null;
    }

    if (endTimeStamp == null) {
      logger.warn("*** Accoustic QuestDB getByIndicatorBeforeTime for dsId [" + dsId
          + "]: endTimeStamp is null. Unable to execute.");
      return null;
    }

    String query = getQdbQueries().getQuery(AccousticQdbQueries.GET_BY_INDICATOR_BEFORE_TIME_QUERY);

    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("dsUuid", dsId);
    parameters.put("endTimeStamp", endTimeStamp);
    if (pageSize == null) {
      pageSize = Integer.MAX_VALUE;
    }
    parameters.put("pageSize", pageSize);

    List<Object[]> results = getResultList(query, parameters.entrySet());
    return (List<Object[]>) results;
  }

  /**
   * This method is responsible for retrieving the statistics (min, max,
   * avg) of the Accoustic waveform data (sample and source) for a given
   * indicator and time range.
   * 
   * @param dsId             the uuid of the data source
   * @param initialTimeStamp the initial time stamp
   * @param endTimeStamp     the end time stamp
   * @return an array of Number (6 values) objects containing the min, max and avg
   *         values for the each sample and source
   * @throws Exception if an error occurs
   */
  @Override
  public synchronized Number[] getValueStatsForIndicator(Object dsId, Long initialTimeStamp, Long endTimeStamp)
      throws Exception {
    if (dsId == null) {
      logger.warn("*** Accoustic QuestDB getValueStatsForIndicator: dsId is null. Unable to execute.");
      return null;
    }

    if (initialTimeStamp == null || endTimeStamp == null) {
      logger.warn("*** Accoustic QuestDB getValueStatsForIndicator for dsId [" + dsId
          + "]: initialTimeStamp or endTimeStamp is null. Unable to execute.");
      return null;
    }

    String query = getQdbQueries().getQuery(AccousticQdbQueries.GET_VALUE_STATS_QUERY);
    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("dsUuid", dsId);
    parameters.put("initialTimeStamp", initialTimeStamp);
    parameters.put("endTimeStamp", endTimeStamp);

    Object[] results = (Object[]) getQuestdbSession(getQdbDatabaseId())
        .getResultsList(query, parameters.entrySet(), null)
        .get(0);

    Number[] actual = new Number[results.length];
    for (int i = 0; i < actual.length; i++) {
      actual[i] = (Number) results[i];
    }

    return actual;
  }

  /**
   * This method is responsible for getting the waveform data from the
   * QuestDB database by dsId and quantity.
   * 
   * @param dsId     the identification of the data source
   * @param quantity the quantity of data
   * @return a list of waveform objects (@see Accoustic)
   * @throws Exception if an error occurs
   */
  public List<Object[]> getByIndicatorAndSize(Object dsId, Integer quantity) throws Exception {
    if (dsId == null) {
      logger.warn("*** Accoustic Waveform QuestDB getByIndicatorAndSize: dsId is null. Unable to execute.");
      return null;
    }

    if (quantity == null) {
      logger.warn("*** Accoustic Waveform QuestDB getByIndicatorAndSize for dsId [" + dsId
          + "]: quantity is null. Assuming 1.");
      quantity = 1;
    }

    String query = getQdbQueries().getQuery(AccousticQdbQueries.GET_BY_INDICATOR_AND_SIZE_QUERY);

    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("dsId", dsId);
    parameters.put("quantity", quantity);

    List<Object[]> results = (List<Object[]>) getResultList(query, parameters.entrySet());
    return results;
  }

  public ArrayList<Accoustic> getQuestdbInterval(String dsId, Long startTime, Long endTime)
      throws Exception {
    List<Object[]> queryResults = null;
    String query = getQdbQueries().getQuery(AccousticQdbQueries.GET_INTERVAL_QUERY);

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("dsId", dsId);
    parameters.put("initialTimeStamp", startTime);
    parameters.put("endTimeStamp", endTime);

    String queryMsg = "\n\n*** ACCOUSTIC: getQuestdbInterval :: Query: " + query + "\nParameters:" + "\n\tdsId: " + dsId
        + "\n\tinitialTimeStamp: " + startTime + "\n\tendTimeStamp: " + endTime + "\n";
    logger.info(queryMsg);

    try {
      queryResults = (List<Object[]>) getResultList(query, parameters.entrySet());
    } catch (Exception e) {
      logger.warn("*** Unable to Get Interval in QuestDB to merge: " + e.getMessage());
      queryResults = null;
    }

    if (queryResults == null || queryResults.size() == 0)
      return null;

    long[] timestamps = new long[queryResults.size()];
    double[] frequencies = new double[queryResults.size()];
    double[] amplitudes = new double[queryResults.size()];

    // The results are in descending order, so we need to reverse the array
    ArrayList<Accoustic> accoustics = new ArrayList<>();
    for (int i = queryResults.size() - 1; i >= 0; i--) {
      Object[] result = queryResults.get(i);

      timestamps[i] = ((Long) result[0]).longValue();
      frequencies[i] = ((Double) result[1]).doubleValue();
      amplitudes[i] = ((Double) result[2]).doubleValue();

      Accoustic accoustic = new Accoustic(dsId, timestamps[i], frequencies[i], amplitudes[i]);
      accoustics.add(accoustic);
    }

    return accoustics;
  }

  /**
   * This class is a concrete implementation of the abstract class
   * QuestdbQueries. It is responsible for managing the queries for the
   * persistence of Accoustic objects in the QuestDB database.
   * 
   * @implNote This class is responsible for defining the table name and column
   *           names for the AccousticQdbQueries object.
   */
  protected class AccousticQdbQueries extends QuestdbQueries {
    public static final String GET_BY_INDICATOR_AND_TIME_QUERY = "getByIndicatorAndTime";
    public static final String GET_BY_INDICATOR_AFTER_TIME_QUERY = "getByIndicatorAfterTime";
    public static final String GET_BY_INDICATOR_BEFORE_TIME_QUERY = "getByIndicatorBeforeTime";
    public static final String GET_VALUE_STATS_QUERY = "getValueStatsForIndicator";
    public static final String GET_BY_INDICATOR_AND_SIZE_QUERY = "getByIndicatorAndSize";
    public static final String GET_MEASUREMENTS_BY_INDICATOR_BEFORE_TIME_QUERY = "getMeasurementsByIndicatorBeforeTime";
    public static final String GET_INTERVAL_QUERY = "getQuestdbInterval";

    protected String frequencyColumnName = null;
    protected String amplitudeColumnName = null;

    public AccousticQdbQueries() {
      super();
      defineTableAndColumns();
    }

    /**
     * This method is responsible for defining the table name and column
     * names for the AccousticQdbQueries object.
     */
    @Override
    protected void defineTableAndColumns() {
      tableName = "ACCOUSTIC";
      idColumnName = "DSID";
      timeColumnName = "TIME_VALUE";
      frequencyColumnName = "FREQUENCY_VALUE";
      amplitudeColumnName = "AMPLITUDE_VALUE";
      timestampColumnName = "TS";
      timestampQdbQuery = timeColumnName;
    }

    /**
     * This method is responsible for populating the questdbQueriesMap
     * with the queries for the AccousticQdbQueries object.
     */
    @Override
    public Map<String, String> populateQuestdbQueries() {
      questdbQueriesMap = super.populateQuestdbQueries();

      /**
       * This query is used to retrieve the Accoustic waveform data (sample and
       * source) for a given indicator and time range.
       * 
       * @implNote This query has 3 parameters: the uuid of the data source, the
       *           initial timestamp and the end timestamp.
       */
      questdbQueriesMap.put(GET_BY_INDICATOR_AND_TIME_QUERY,
          "select " + timeColumnName + ", " + frequencyColumnName + ", " + amplitudeColumnName + " " + "from "
              + tableName
              + " where " + idColumnName + " = ? and "
              + timestampQdbQuery + " between ? and ? order by " + timestampColumnName + " desc");

      /**
       * This query is used to retrieve the Accoustic waveform data (sample and
       * source) for a given indicator after a given time.
       * 
       * @implNote This query has 2 parameters: the uuid of the data source and the
       *           timestamp.
       */
      questdbQueriesMap.put(GET_BY_INDICATOR_AFTER_TIME_QUERY,
          "select " + timeColumnName + ", " + frequencyColumnName + ", " + amplitudeColumnName + " " + "from "
              + tableName
              + " where " + idColumnName + " = ? and "
              + timestampQdbQuery + " >= ? order by " + timestampColumnName + " desc");

      /**
       * This query is used to retrieve the Accoustic waveform data (sample and
       * source) for a given indicator before a given time.
       * 
       * @implNote This query has 2 parameters: the uuid of the data source and the
       *           timestamp.
       */
      questdbQueriesMap.put(GET_BY_INDICATOR_BEFORE_TIME_QUERY,
          "select " + timeColumnName + ", " + frequencyColumnName + ", " + amplitudeColumnName + " " + "from "
              + tableName
              + " where " + idColumnName + " = ? and "
              + timestampQdbQuery + " <= ? order by " + timestampColumnName + " desc limit ?");

      /**
       * This query is used to retrieve the statistics (min, max, avg) of the
       * Accoustic
       * waveform data (sample and source) for a given indicator and time range.
       * 
       * @implNote This query has 3 parameters: the uuid of the data source, the
       *           initial timestamp and the end timestamp.
       */
      questdbQueriesMap.put(GET_VALUE_STATS_QUERY,
          "select " +
              "MIN(" + frequencyColumnName + "), MAX(" + frequencyColumnName + "), AVG(" + frequencyColumnName + ") " +
              ", MIN(" + amplitudeColumnName + "), MAX(" + amplitudeColumnName + "), AVG(" + amplitudeColumnName + ") "
              +
              "from " + tableName
              + " where " + idColumnName + " = ? and " + timestampQdbQuery + " between ? and ?");

      /**
       * This method is responsible for getting the waveform data from the QuestDB
       * database by uuid and size.
       * 
       * @implNote This query has 2 parameters: the uuid of the data source and the
       *           quantity of data.
       */
      questdbQueriesMap.put(GET_BY_INDICATOR_AND_SIZE_QUERY,
          "select " + timeColumnName + ", " + frequencyColumnName + ", " + amplitudeColumnName + " " + "from "
              + tableName
              + " where " + idColumnName + " = ? order by " + timestampColumnName + " desc limit ?");

      /**
       * This method is responsible for getting the waveform data from the QuestDB
       * database by uuid and before determined time.
       * 
       * @implNote This query has 2 parameters: the ID of the data source and the
       *           timestamp.
       */
      questdbQueriesMap.put(GET_MEASUREMENTS_BY_INDICATOR_BEFORE_TIME_QUERY,
          "select " + timeColumnName + ", " + frequencyColumnName + ", " + amplitudeColumnName + " " + "from "
              + tableName
              + " where " + idColumnName + " = ? and " + timestampQdbQuery + " <= ? order by " + timestampColumnName
              + " desc limit ?");

      /**
       * This method is responsible for getting the waveform data from the QuestDB
       * database by uuid and time range (initial and end). The ascending order is
       * used to get the data from the oldest to the newest.
       * 
       * @implNote This query has 3 parameters: the ID of the data source, the
       *           initial timestamp and the end timestamp.
       */
      questdbQueriesMap.put(GET_INTERVAL_QUERY,
          "select " + timeColumnName + ", " + frequencyColumnName + ", " + amplitudeColumnName + " " + "from "
              + tableName
              + " where " + idColumnName + " = ? and " + timestampQdbQuery + " between ? and ? order by "
              + timestampColumnName + " asc");

      return questdbQueriesMap;
    }

    public String getTableName() {
      return tableName;
    }

    public String getIdColumnName() {
      return idColumnName;
    }

    public String getTimeColumnName() {
      return timeColumnName;
    }

    public String getAmplitudeColumnName() {
      return amplitudeColumnName;
    }

    public String getFrequencyColumnName() {
      return frequencyColumnName;
    }

    @Override
    public String getQuery(String queryName) {
      return questdbQueriesMap.get(queryName);
    }
  }

  public String getSQLQuery(String queryId) {
    return getQdbQueries().getQuery(queryId);
  }

  @Override
  public void setPurgePeriodType(String keepPeriodType) {
    getQdbQueries().setPurgePeriodType(keepPeriodType);
  }

  @Override
  public void setPurgePeriod(Integer keepPeriod) {
    getQdbQueries().setPurgePeriod(keepPeriod);
  }
}
