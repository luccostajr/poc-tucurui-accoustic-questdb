package br.cepel.questdb.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import br.cepel.questdb.session.QuestdbSession;

/**
 * This class is an abstract class that defines the basic operations
 * for managing the persistence of timeseries data in the QuestDB
 * database.
 * 
 * @implNote It is responsible for managing the persistence of timeseries data
 *           in the QuestDB database.
 * 
 * @since 2024-02-29
 * @version 1.0
 */
public abstract class BaseQdbBO implements GenericBaseBO, IndicatorBO {
  private static final Logger logger = LogManager.getLogger(BaseQdbBO.class);

  /**
   * The QuestDB session.
   */
  // private QuestdbSession questdbSession = null;

  /**
   * The flag to control if the table was created.
   */
  protected Boolean tableCreated = false;

  /**
   * The map of QuestDB sessions to avoid creating a new session for
   * same request.
   */
  protected static Map<String, QuestdbSession> questdbSessionMap = new HashMap<String, QuestdbSession>();

  /**
   * The semaphore for controlling access to the QuestDB database. It is used to
   * synchronize access to the QuestDB database in the delete / update operations.
   */
  private Semaphore insertProtectionSemaphore = null;

  private String qdbDatabaseId = null;

  // ABSTRACT METHODS

  /**
   * This method is responsible for returning the specialized class for
   * the queries.
   * 
   * @return the specialized class for the queries
   * @throws Exception if an error occurs
   */
  protected abstract QuestdbQueries getQdbQueries() throws Exception;

  /**
   * This method is responsible for inserting data into the QuestDB
   * database.
   * 
   * @param dsId the ID of the data source
   * @param data the data to be inserted
   * @return the object inserted
   * @throws Exception if an error occurs
   */
  public abstract void insert(Object dsId, Object data) throws Exception;

  /**
   * This method is responsible for returning the table create command
   * for the specialized class.
   * 
   * @return the table create command for the specialized class
   * @throws Exception if an error occurs
   */
  protected abstract String getCreateSQLCommand() throws Exception;

  /**
   * This method is responsible for returning the QuestDB database ID for
   * the specialized class.
   * 
   * @return the QuestDB database ID for the specialized class
   */
  protected abstract String getQdbDatabaseId();

  /**
   * This method is responsible for setting the data period type to maintain
   * in the QuestDB database for the specialized class. The period type can be
   * 'd' for DAY, 'h' for HOUR or 'm' for MINUTE.
   * 
   * @param keepPeriodType the data period type to maintain in the QuestDB
   *                       database - 'd' for DAY, 'h' for HOUR or 'm' for MINUTE
   */
  public abstract void setPurgePeriodType(String keepPeriodType);

  /**
   * This method is responsible for setting the data period to maintain in
   * the QuestDB database for the specialized class. The predessessor data is
   * purged from the QuestDB database.
   * 
   * @param keepPeriod the data period to maintain in the QuestDB database
   */
  public abstract void setPurgePeriod(Integer keepPeriod);

  // CONSTRUCTOR

  public BaseQdbBO(String qdbDatabaseId, Semaphore concreteSemaphore) throws Exception {
    super();
    this.qdbDatabaseId = qdbDatabaseId;
    registerSemaphore(concreteSemaphore);
  }

  /**
   * This method is responsible for registering the semaphore for the
   * specialized class.
   * 
   * @throws Exception if an error occurs
   */
  protected void registerSemaphore(Semaphore semaphore) throws Exception {
    insertProtectionSemaphore = semaphore;
  }

  public synchronized static QuestdbSession createQuestdbSession(String qdbDatabaseId) throws Exception {
    try {
      QuestdbSession createdQuestDbSession = null;
      createdQuestDbSession = questdbSessionMap.get(qdbDatabaseId);
      if (createdQuestDbSession == null) {
        createdQuestDbSession = new QuestdbSession(qdbDatabaseId);
        questdbSessionMap.put(qdbDatabaseId, createdQuestDbSession);
        logger.trace("TimeseriesQdbBO for " + qdbDatabaseId.toUpperCase() + " created.");
      }
      return createdQuestDbSession;
    } catch (Exception e) {
      throw new Exception(e);
    }
  }

  /**
   * This method is responsible for creating the table if it does not
   * exist. It is called by the constructor of the specialized class.
   * 
   * @throws Exception if an error occurs
   */
  protected void createTableIfNotExists() throws Exception {
    String createQuery = getCreateSQLCommand();
    if (createQuery == null || createQuery.isEmpty())
      logger.trace("createQuery is null or empty. No table created.");
    logger.trace("Creating table if not exists: " + createQuery);
    getQuestdbSession(qdbDatabaseId).executeQuery(createQuery);
    tableCreated = true;
  }

  /**
   * This method is responsible for purging old records from the
   * QuestDB database.
   * 
   * @param periodType the period type (e.g. 'DAY', 'HOUR', 'MINUTE')
   * @param duration   the duration (e.g. 1, 2, 3)
   * @throws Exception if an error occurs
   */
  public void purgeOldRecords() throws Exception {
    String maintenanceQuery = String.format(getQdbQueries().getQuery(QuestdbQueries.PURGE_QUERY),
        getQdbQueries().getPurgePeriodType(), getQdbQueries().getPurgePeriod());

    logger.info("\nPurging old records: " + maintenanceQuery);

    getQuestdbSession(qdbDatabaseId).executeQuery(maintenanceQuery);
  }

  // QUESTDB CUSTODY METHODS

  /**
   * The method for getting the QuestDB session.
   * 
   * @return the QuestDB session
   * @throws Exception if an error occurs
   */
  public static QuestdbSession getQuestdbSession(String qdbDatabaseId) throws Exception {
    QuestdbSession questdbSession = questdbSessionMap.get(qdbDatabaseId);
    if (questdbSession == null) {
      questdbSession = createQuestdbSession(qdbDatabaseId);
    }
    return questdbSession;
  }

  /**
   * This method is responsible for get a single result from the
   * QuestDB database.
   * 
   * @param query            the query to be executed
   * @param dsId             the ID of the data source
   * @param initialTimeStamp the initial timestamp (optional = null)
   * @param endTimeStamp     the end timestamp (optional = null)
   * @param pageSize         the page size (optional = null)
   * @return the single result from the QuestDB database. It can be a single
   *         value, a single row or a single column. It depends on the query.
   * @throws Exception if an error occurs
   */
  public Object getSingleResult(String query, Object dsId, Long initialTimeStamp, Long endTimeStamp, Integer pageSize)
      throws Exception {
    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("dsId", dsId);
    if (initialTimeStamp != null)
      parameters.put("initialTimeStamp", initialTimeStamp);
    if (endTimeStamp != null)
      parameters.put("endTimeStamp", endTimeStamp);

    List<Object[]> objList = getQuestdbSession(qdbDatabaseId).getResultsList(query, parameters.entrySet(), pageSize);
    if (objList == null || objList.size() == 0)
      throw new Exception("No results: List is null or empty.");

    Object[] results = (Object[]) objList.get(0);
    if (results == null || results.length == 0)
      throw new Exception("No results: results is null or empty.");
    return results[0];
  }

  public Object getSingleResult(String query, Object dsId, Long initialTimeStamp, Long endTimeStamp)
      throws Exception {
    return getSingleResult(query, dsId, initialTimeStamp, endTimeStamp, null);
  }

  /**
   * This method is responsible for get a list of results from the
   * QuestDB database.
   * 
   * @param query      the query to be executed
   * @param parameters the parameters for the query
   * @param pageSize   the page size (optional = null)
   * @return the list of results from the QuestDB database. It can be a list of
   *         values, a list of rows or a list of columns. It depends on the query.
   * @throws Exception if an error occurs
   */
  public synchronized List<Object[]> getResultList(String query, Set<Entry<String, Object>> parameters,
      Integer pageSize) throws Exception {
    return getQuestdbSession(qdbDatabaseId).getResultsList(query, parameters, pageSize);
  }

  public List<Object[]> getResultList(String query, Set<Entry<String, Object>> parameters)
      throws Exception {
    return getQuestdbSession(qdbDatabaseId).getResultsList(query, parameters, null);
  }

  // TIMESERIES BO METHODS

  /**
   * This method converts a list of objects to a list of longs.
   * 
   * @param list the list of objects to be converted to a list of longs.
   * @return the list of longs
   */
  public static List<Long> convertListToLong(List<Object> list) {
    List<Long> ids = new ArrayList<>();
    for (Object id : list) {
      ids.add((Long) id);
    }
    return ids;
  }

  /**
   * This method converts a list of objects to a list of strings.
   * 
   * @param list the list of objects to be converted to a list of strings.
   * @return the list of strings
   */
  public static List<String> convertListToString(List<Object> list) {
    List<String> ids = new ArrayList<>();
    for (Object id : list) {
      ids.add((String) id);
    }
    return ids;
  }

  /**
   * This method is responsible for getting the count of entries for a
   * given dsid in a given time range.
   * 
   * @param dsId             the ID of the data source
   * @param initialTimeStamp the initial timestamp
   * @param endTimeStamp     the end timestamp
   * @return the count of entries for a given dsid in a given time range
   * @throws Exception if an error occurs
   */
  @Override
  public Long getMaxDateForIndicator(Object dsId, Long initialTimeStamp, Long endTimeStamp)
      throws Exception {
    String query = getQdbQueries().getQuery(QuestdbQueries.GET_MAX_DATE_FOR_INDICATOR_QUERY);
    return (Long) getSingleResult(query, dsId, initialTimeStamp, endTimeStamp);
  }

  /**
   * This method is responsible for getting the maximum date for a given
   * dsid in a given time range.
   * 
   * @param dsId             the ID of the data source
   * @param initialTimeStamp the initial timestamp
   * @param endTimeStamp     the end timestamp
   * @return the maximum date for a given dsid in a given time range
   * @throws Exception if an error occurs
   */
  @Override
  public Long getMinDateForIndicator(Object dsId, Long initialTimeStamp, Long endTimeStamp)
      throws Exception {
    String query = getQdbQueries().getQuery(QuestdbQueries.GET_MIN_DATE_FOR_INDICATOR_QUERY);
    return (Long) getSingleResult(query, dsId, initialTimeStamp, endTimeStamp);
  }

  /**
   * This method is responsible for getting the minimum date for a given
   * dsid in a given time range.
   * 
   * @param dsId             the ID of the data source
   * @param initialTimeStamp the initial timestamp
   * @param endTimeStamp     the end timestamp
   * @return the minimum date for a given dsid in a given time range
   * @throws Exception if an error occurs
   */
  @Override
  public Long getCountForIndicator(Object dsId, Long initialTimeStamp, Long endTimeStamp)
      throws Exception {
    String query = getQdbQueries().getQuery(QuestdbQueries.GET_COUNT_FOR_INDICATOR_QUERY);
    return (Long) getSingleResult(query, dsId, initialTimeStamp, endTimeStamp);
  }

  /**
   * This method is responsible for getting the total count of entries
   * for a given dsid.
   * 
   * @param dsId the ID of the data source
   * @return the total count of entries for a given dsid
   * @throws Exception if an error occurs
   */
  @Override
  public Long getTotalCountForIndicator(Object dsId) throws Exception {
    String query = getQdbQueries().getQuery(QuestdbQueries.GET_TOTAL_COUNT_QUERY);
    return (Long) getSingleResult(query, dsId, null, null);
  }

  // BASE BO METHODS

  /**
   * This method is responsible for getting all entries.
   * 
   * @return a list of all entries
   * @throws Exception if an error occurs
   */
  @Override
  public List<?> getAllEntries() throws Exception {
    String query = getQdbQueries().getQuery(QuestdbQueries.GET_ALL_ENTRIES_QUERY);
    return getResultList(query, null, null);
  }

  /**
   * This method is responsible for getting an entry by dsid.
   * 
   * @param dsId the ID of the data source
   * @return the entry object
   * @throws Exception if an error occurs
   */
  @Override
  public Object getEntryById(Object dsId) throws Exception {
    String query = getQdbQueries().getQuery(QuestdbQueries.GET_ENTRY_BY_ID_QUERY);
    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("dsId", dsId);

    List<Object[]> results = (List<Object[]>) getResultList(query, parameters.entrySet(), null);
    return (List<Object[]>) results;
  }

  // THE FOLLOWING METHODS ARE FOR DELETE / UPDATE OPERATIONS

  /**
   * This is a helper method used for create a string with the dsids for
   * the delete / update operations (using the 'in' clause).
   * 
   * @param dsIds the list of dsids
   * @return the string with the dsids separated by comma and enclosed by
   *         'single quotes' and terminated by a closing parenthesis
   */
  private String mountdsIdsForQuery(List<Object> dsIds) {
    if (dsIds == null || dsIds.size() == 0)
      return "'')";

    String query = "";
    for (Object dsId : dsIds) {
      query += "'" + dsId + "',";
    }
    query = query.substring(0, query.length() - 1) + ")";

    return query;
  }

  /**
   * This method is responsible for get all entries for a given
   * dsid list.
   * 
   * @param dsIds the list of dsids
   * @return a list of entry objects
   * @throws Exception if an error occurs
   */
  @Override
  public List<?> getEntriesById(List<Object> dsIds) throws Exception {
    String query = getQdbQueries().getQuery(QuestdbQueries.GET_ENTRIES_BY_ID_QUERY);

    query += mountdsIdsForQuery(dsIds);

    List<Object[]> results = (List<Object[]>) getResultList(query, null, null);
    return (List<Object[]>) results;
  }

  /**
   * This method is responsible for removing all entries.
   * 
   * @implNote It uses the sequence of operations: drop copy table, create copy
   *           table, drop table, rename copy table.
   * @return the number of entries removed
   * @throws Exception if an error occurs
   */
  @Override
  public int removeAllEntries() throws Exception {
    try {
      if (insertProtectionSemaphore != null)
        insertProtectionSemaphore.acquire();

      QuestdbSession questdbSession = getQuestdbSession(qdbDatabaseId);
      String dropCopyQuery = getQdbQueries().getQuery(QuestdbQueries.DROP_COPY_QUERY);
      questdbSession.executeQuery(dropCopyQuery);

      String copyQuery = getQdbQueries().getQuery(QuestdbQueries.COPY_TABLE_QUERY);
      questdbSession.executeQuery(copyQuery);

      String dropTableQuery = getQdbQueries().getQuery(QuestdbQueries.DROP_TABLE_QUERY);
      questdbSession.executeQuery(dropTableQuery);

      String renameQuery = getQdbQueries().getQuery(QuestdbQueries.RENAME_COPY_QUERY);
      questdbSession.executeQuery(renameQuery);

      return 0;
    } catch (Exception e) {
      throw new Exception(e);
    } finally {
      if (insertProtectionSemaphore != null)
        insertProtectionSemaphore.release();
    }
  }

  /**
   * This method is responsible for removing an entry by a list of dsids.
   * 
   * @implNote It uses the sequence of operations: drop copy table, create copy
   *           table, copy table selected, drop table, rename copy table.
   * @param dsId the ID of the data source
   * @return the number of entries removed
   * @throws Exception if an error occurs
   */
  @Override
  public int removeEntriesById(List<Object> dsIds) throws Exception {
    try {
      if (insertProtectionSemaphore != null)
        insertProtectionSemaphore.acquire();
      QuestdbSession questdbSession = getQuestdbSession(qdbDatabaseId);
      String dropCopyQuery = getQdbQueries().getQuery(QuestdbQueries.DROP_COPY_QUERY);
      questdbSession.executeQuery(dropCopyQuery);

      String copyQuery = getQdbQueries().getQuery(QuestdbQueries.COPY_TABLE_SELECTED_QUERY);
      copyQuery += mountdsIdsForQuery(dsIds) + getQdbQueries().getQuery(QuestdbQueries.COPY_COMPLEMENT_QUERY);
      questdbSession.executeQuery(copyQuery);

      String dropTableQuery = getQdbQueries().getQuery(QuestdbQueries.DROP_TABLE_QUERY);
      questdbSession.executeQuery(dropTableQuery);

      String renameQuery = getQdbQueries().getQuery(QuestdbQueries.RENAME_COPY_QUERY);
      questdbSession.executeQuery(renameQuery);

      return 0;
    } catch (Exception e) {
      throw new Exception(e);
    } finally {
      if (insertProtectionSemaphore != null)
        insertProtectionSemaphore.release();
    }
  }

  // ABSTRACT INNER CLASS TO DEFINE THE QUERIES MAP

  /**
   * This class is an abstract inner class that defines the basic
   * operations for managing the persistence of timeseries data in the
   * QuestDB database.
   * 
   * @implNote It is responsible for managing the persistence of timeseries data
   *           in the QuestDB database.
   * @implNote It defines the basic column names and table name for the
   *           specialized classes.
   * @implNote It also defines the basic queries (timestamp operations) for the
   *           specialized classes. The specialized classes will define the table
   *           and columns names and specific queries.
   */
  protected abstract class QuestdbQueries {
    /**
     * The period type (e.g. 'd', 'h', 'm')
     */
    protected static final String DEFAULT_PURGE_PERIOD_TYPE = "d";

    /**
     * The duration (e.g. 1, 2, 3)
     */
    protected static final Integer DEFAULT_PURGE_PERIOD = 20;

    /**
     * The partition type (e.g. 'DAY', 'HOUR', 'MINUTE')
     */
    protected static final String DEFAULT_PARTITION_TYPE = "DAY";

    /**
     * The Questdb symbol capacity. Specified to estimate how many unique symbol
     * values to expect in the table. Should be the same or slightly larger than the
     * count of distinct symbol values. The default value is 100000 data sources.
     */
    protected static final int DEFAULT_SYMBOL_CAPACITY = 100000;

    /**
     * The Questdb index capacity. Specified for the symbol column with a storage
     * block value in the table. Provides faster read access to a table. However,
     * indexes have a noticeable cost in terms of disk space and ingestion rate -
     * It's recommended starting with no indexes and adding them later, only if they
     * appear to improve query performance. The default value is 256.
     */
    protected static final int DEFAULT_INDEX_CAPACITY = 256;

    /**
     * tag names for queries related to the timeseries data
     */
    protected static final String GET_COUNT_FOR_INDICATOR_QUERY = "getCountForIndicator";
    protected static final String GET_MAX_DATE_FOR_INDICATOR_QUERY = "getMaxDateForIndicator";
    protected static final String GET_MIN_DATE_FOR_INDICATOR_QUERY = "getMinDateForIndicator";
    protected static final String GET_TOTAL_COUNT_QUERY = "getTotalCountForIndicator";
    protected static final String GET_ALL_ENTRIES_QUERY = "getAllEntries";
    protected static final String GET_ENTRY_BY_ID_QUERY = "getEntryById";
    protected static final String GET_ENTRIES_BY_ID_QUERY = "getEntriesById";
    protected static final String GET_INTERVAL_QUERY = "getQuestdbInterval";
    protected static final String CREATE_QUERY = "createTableIfNotExists";
    protected static final String PURGE_QUERY = "purgeOldRecords";

    /**
     * tag names for queries related to delete / update operations
     */
    protected static final String DROP_COPY_QUERY = "dropCopyTable";
    protected static final String COPY_TABLE_QUERY = "copyTable";
    protected static final String COPY_TABLE_SELECTED_QUERY = "copyTableSelected";
    protected static final String DROP_TABLE_QUERY = "dropTable";
    protected static final String RENAME_COPY_QUERY = "renameCopyTable";

    /**
     * tag names for help complement create copy table (using 'in' clause)
     */
    protected static final String COPY_COMPLEMENT_QUERY = "copyComplementTable";

    /**
     * table and column names for timeseries data operations
     */
    protected String tableName = null;
    protected String idColumnName = null;
    protected String valueColumnName = null;
    protected String timestampColumnName = null;
    protected String timestampQdbQuery = null;
    protected String tableCopyName = null;
    protected String purgePeriodType = DEFAULT_PURGE_PERIOD_TYPE;
    protected Integer purgePeriod = DEFAULT_PURGE_PERIOD;
    protected Integer symbolCapacity = DEFAULT_SYMBOL_CAPACITY;
    protected Integer indexCapacity = DEFAULT_INDEX_CAPACITY;
    protected String partitionBy = DEFAULT_PARTITION_TYPE;

    /**
     * This method is responsible for defining the table name and column
     * in the specialized class. It is called by the constructor of the
     * specialized class.
     */
    protected abstract void defineTableAndColumns();

    /**
     * Map to store the queries for the specialized class
     */
    protected Map<String, String> questdbQueriesMap = null;

    /**
     * Contructor for the QuestdbQueries class
     * 
     * @implNote It is responsible for creating the table and columns names for the
     *           specialized class.
     * @implNote It is responsible for creating the map to store the queries for the
     *           specialized class.
     */
    public QuestdbQueries() {
      super();
      defineTableAndColumns();
      populateQuestdbQueries();

      tableCopyName = tableName + "_copy";
    }

    public Integer getIndexCapacity() {
      return indexCapacity;
    }

    public Integer getSymbolCapacity() {
      return symbolCapacity;
    }

    public String getPartitionBy() {
      return partitionBy;
    }

    /**
     * This method is responsible for populating the questdbQueriesMap
     * with the queries for the specialized class. It is called by the
     * constructor of the specialized class.
     * 
     * @return the map with the queries for the specialized class
     */
    public Map<String, String> populateQuestdbQueries() {
      questdbQueriesMap = new HashMap<String, String>();

      /**
       * The following query get the count of entries for a given dsid
       * in a given time range.
       */
      questdbQueriesMap.put(GET_COUNT_FOR_INDICATOR_QUERY,
          "select COUNT(*) from " + tableName
              + " where " + idColumnName + " = ? and " + timestampQdbQuery + " between ? and ?");

      /**
       * The following query get the maximum date for a given dsid in a
       * given time range.
       */
      questdbQueriesMap.put(GET_MAX_DATE_FOR_INDICATOR_QUERY,
          "select MAX(" + timestampQdbQuery + ") from " + tableName
              + " where " + idColumnName + " = ? and " + timestampQdbQuery + " between ? and ?");

      /**
       * The following query get the minimum date for a given dsid in a
       * given time range.
       */
      questdbQueriesMap.put(GET_MIN_DATE_FOR_INDICATOR_QUERY,
          "select MIN(" + timestampQdbQuery + ") from " + tableName
              + " where " + idColumnName + " = ? and " + timestampQdbQuery + " between ? and ?");

      /**
       * The following query get the total count of entries for a given
       * dsid.
       */
      questdbQueriesMap.put(GET_TOTAL_COUNT_QUERY,
          "select COUNT(*) from " + tableName
              + " where " + idColumnName + " = ?");

      /**
       * The following query get all entries.
       */
      questdbQueriesMap.put(GET_ALL_ENTRIES_QUERY,
          "select * from " + tableName);

      /**
       * The following query get the most recent entry by dsid.
       */
      questdbQueriesMap.put(GET_ENTRY_BY_ID_QUERY,
          "select * from " + tableName
              + " where " + idColumnName + " = ? order by " + timestampColumnName + " desc limit 1");

      /**
       * The following query get the entries by dsid within a given list of
       * dsids.
       */
      questdbQueriesMap.put(GET_ENTRIES_BY_ID_QUERY,
          "select * from " + tableName
              + " where " + idColumnName + " in ("); // must be completed with the list of ids (IN clause)

      // THE FOLLOWING QUERIES ARE FOR DELETE / UPDATE OPERATIONS

      /**
       * The following query drop the copy table if it exists.
       */
      questdbQueriesMap.put(DROP_COPY_QUERY,
          "drop table if exists " + tableCopyName);

      /**
       * The following query drop the table if it exists.
       */
      questdbQueriesMap.put(DROP_TABLE_QUERY,
          "drop table if exists " + tableName);

      /**
       * The following query is a helper to complement the copy table operation(which
       * is used in the create query).
       */
      questdbQueriesMap.put(COPY_COMPLEMENT_QUERY,
          " timestamp(" + timestampColumnName + ")"
              + " PARTITION BY DAY");

      /**
       * The following query create a empty copy table based on the original table.
       */
      questdbQueriesMap.put(COPY_TABLE_QUERY,
          "create table " + tableCopyName
              + " as (select * from "
              + tableName + " where 0=1)" + questdbQueriesMap.get(COPY_COMPLEMENT_QUERY));

      /**
       * The following query create a copy table based on the original table, but
       * excluding the entries with the given list of dsids.
       */
      questdbQueriesMap.put(COPY_TABLE_SELECTED_QUERY,
          "create table " + tableCopyName
              + " as (select * from "
              + tableName + " where " + idColumnName + " not in ("); // must be completed with the list of ids (IN
                                                                     // clause)

      /**
       * The following query rename the copy table to the original table name.
       */
      questdbQueriesMap.put(RENAME_COPY_QUERY,
          "rename table " + tableCopyName + " to " + tableName);

      /**
       * The following query will be used to purge old records from the
       * QuestDB database, using the timestamp column to define the time range and the
       * partitioning information. The period type and duration are defined in the
       * specialized class. The purge period is determined by now() - duration (past
       * time)
       */
      questdbQueriesMap.put(PURGE_QUERY,
          "ALTER TABLE " + tableName + " DROP PARTITION WHERE " + timestampColumnName + " < dateadd('%s', -%d, now())");

      return questdbQueriesMap;
    }

    public String getTableName() {
      return tableName;
    }

    public String getIdColumnName() {
      return idColumnName;
    }

    public String getTimestampColumnName() {
      return timestampColumnName;
    }

    public String getTimestampQdbQuery() {
      return timestampQdbQuery;
    }

    public String getPurgePeriodType() {
      return purgePeriodType;
    }

    public Integer getPurgePeriod() {
      return purgePeriod;
    }

    public void setPurgePeriodType(String purgePeriodType) {
      this.purgePeriodType = purgePeriodType;
    }

    public void setPurgePeriod(Integer purgePeriod) {
      this.purgePeriod = purgePeriod;
    }

    public String getQuery(String queryTag) {
      return questdbQueriesMap.get(queryTag);
    }
  }

  // FORMATTING DATE TIME

  /**
   * This method is responsible for formatting the date time.
   * 
   * @param ts the timestamp in milliseconds
   * @return the formatted date time
   */
  public static String getFormattedDateTime(double ts) {
    long timestamp = ((Double) ts).longValue();
    return String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", timestamp);
  }

  /**
   * This method is responsible for formatting the date time.
   * 
   * @param ts the timestamp in milliseconds
   * @return the formatted date time
   */
  public static String getFormattedDateTime(long ts) {
    return String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", ts);
  }
}
