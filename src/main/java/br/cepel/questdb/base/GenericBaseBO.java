package br.cepel.questdb.base;


import java.util.List;

/**
 * Base class for all business objects
 * 
 * @since 2024-02-29
 * @version 1.0
 */
public interface GenericBaseBO {
  /**
   * The method for getting all entries. Must be implemented by the concrete
   * class.
   * 
   * @return a list of all entries
   * @throws Exception if an error occurs
   */
  public abstract List<?> getAllEntries() throws Exception;

  /**
   * The method for getting an entry by ID. Must be implemented by the concrete
   * class.
   * 
   * @param dsId the ID of the entry
   * @return the entry object
   * @throws Exception if an error occurs
   */
  public default Object getEntryById(Object dsId) throws Exception {
    return null;
  }

  /**
   * The method for getting entries by ID. Must be implemented by the concrete
   * class.
   * 
   * @param dsIds the IDs list of the entries
   * @return a list of entry objects
   * @throws Exception if an error occurs
   */
  public default List<?> getEntriesById(List<Object> dsIds) throws Exception {
    return null;
  }

  /**
   * The method for removing all entries. Must be implemented by the concrete
   * class.
   * 
   * @return the number of entries removed
   * @throws Exception if an error occurs
   */
  public default int removeAllEntries() throws Exception {
    return 0;
  }

  /**
   * The method for removing an entry by ID. Must be implemented by the concrete
   * class.
   * 
   * @param dsId the ID of the entry
   * @return the number of entries removed
   * @throws Exception if an error occurs
   */
  public default int removeEntriesById(List<Object> dsIds) throws Exception {
    return 0;
  }
}