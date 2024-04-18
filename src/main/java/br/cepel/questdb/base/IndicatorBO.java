package br.cepel.questdb.base;

import java.util.List;

/**
 * The interface for the business object
 * 
 * @since 2024-02-29
 * @version 1.0
 */
public interface IndicatorBO {
        /**
         * The method for get the count of records. Must be implemented by the
         * concrete class.
         * 
         * @param dsId             the ID of data source
         * @param initialTimestamp the initial time stamp
         * @param endTimestamp     the end time stamp
         * @return the count of records
         * @throws Exception if an error occurs
         */
        public Long getCountForIndicator(Object dsId, Long initialTimestamp, Long endTimestamp)
                        throws Exception;

        /**
         * The method for get the max date of entries. Must be implemented by the
         * concrete class.
         * 
         * @param dsId             the ID of data source
         * @param initialTimestamp the initial time stamp
         * @param endTimestamp     the end time stamp
         * @return the max date of entries
         * @throws Exception if an error occurs
         */
        public Long getMaxDateForIndicator(Object dsId, Long initialTimestamp, Long endTimestamp)
                        throws Exception;

        /**
         * The method for get the min date of entries. Must be implemented by the
         * concrete class.
         * 
         * @param dsId             the ID of data source
         * @param initialTimestamp the initial time stamp
         * @param endTimestamp     the end time stamp
         * @return the min date of entries
         * @throws Exception if an error occurs
         */
        public Long getMinDateForIndicator(Object dsId, Long initialTimestamp, Long endTimestamp)
                        throws Exception;

        /**
         * The method for get the statistics of entries. Must be implemented by the
         * concrete class.
         * 
         * @param dsId             the ID of data source
         * @param initialTimestamp the initial time stamp
         * @param endTimestamp     the end time stamp
         * @return the sum of entries
         * @throws Exception if an error occurs
         */
        public Number[] getValueStatsForIndicator(Object dsId, Long initialTimestamp, Long endTimestamp)
                        throws Exception;

        /**
         * The method for get the entries by id and time. Must be implemented by the
         * concrete class.
         * 
         * @param dsId             the ID of data source
         * @param initialTimestamp the initial time stamp
         * @param endTimestamp     the end time stamp
         * @return the list of entries
         * @throws Exception if an error occurs
         */
        public List<Object[]> getByIndicatorAndTime(Object dsId, Long initialTimestamp, Long endTimestamp)
                        throws Exception;

        /**
         * The method for get the entries after time. Must be implemented by the
         * concrete class.
         * 
         * @param dsId             the ID of data source
         * @param initialTimestamp the initial time stamp
         * @param pageSize         the page size
         * @return the list of entries
         * @throws Exception if an error occurs
         */
        public List<Object[]> getByIndicatorAfterTime(Object dsId, Long initialTimestamp,
                        Integer pageSize) throws Exception;

        /**
         * The method for get the entries before time. Must be implemented by the
         * concrete class.
         * 
         * @param dsId      the ID of data source
         * @param timeStamp the time stamp
         * @param pageSize  the page size
         * @return the list of entries
         * @throws Exception if an error occurs
         */
        public List<Object[]> getByIndicatorBeforeTime(Object dsId, Long timeStamp, Integer pageSize)
                        throws Exception;

        /**
         * The method for get the total count of entries. Must be implemented by the
         * concrete class.
         * 
         * @param dsId the ID of data source
         * @return the total count of entries
         * @throws Exception if an error occurs
         */
        public Long getTotalCountForIndicator(Object dsId) throws Exception;

        /**
         * The method for get the entries by id and size. Must be implemented by the
         * concrete class.
         * 
         * @param dsId     the ID of data source
         * @param quantity the quantity of entries
         * @return the list of entries
         * @throws Exception if an error occurs
         */
        public List<Object[]> getByIndicatorAndSize(Object dsId, Integer quantity) throws Exception;
}
