/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.iterator;

import java.util.Arrays;
import java.util.NoSuchElementException;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Iterator to browse multiple clusters forward and backward. Once browsed in a direction, the iterator cannot change it. This
 * iterator with "live updates" set is able to catch updates to the cluster sizes while browsing. This is the case when concurrent
 * clients/threads insert and remove item in any cluster the iterator is browsing. If the cluster are hot removed by from the
 * database the iterator could be invalid and throw exception of cluster not found.
 * 
 * @author Luca Garulli
 * 
 */
public class ORecordIteratorClusters<REC extends ORecordInternal<?>> extends OIdentifiableIterator<REC> {
  protected int[]      clusterIds;
  protected int        currentClusterIdx;
  protected ORecord<?> currentRecord;

  protected ORID       beginRange;
  protected ORID       endRange;

  public ORecordIteratorClusters(final ODatabaseRecord iDatabase, final ODatabaseRecordAbstract iLowLevelDatabase,
      final int[] iClusterIds) {
    super(iDatabase, iLowLevelDatabase);
    clusterIds = iClusterIds;
    config();
  }

  protected ORecordIteratorClusters(final ODatabaseRecord iDatabase, final ODatabaseRecordAbstract iLowLevelDatabase) {
    super(iDatabase, iLowLevelDatabase);
  }

  public ORecordIteratorClusters<REC> setRange(final ORID iBegin, final ORID iEnd) {
    beginRange = iBegin;
    endRange = iEnd;
    return this;
  }

  @Override
  public boolean hasPrevious() {
    checkDirection(false);

    if (currentRecord != null)
      return true;

    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return false;

    if (browsedRecords >= totalAvailableRecords)
      return false;

    if (liveUpdated)
      updateClusterRange();

    ORecordInternal<?> record = getRecord();

    // ITERATE UNTIL THE PREVIOUS GOOD RECORD
    while (currentClusterIdx > -1) {
      while (currentEntry >= firstClusterEntry) {
        if (!nextPosition(+1)) {
          currentRecord = null;
          break;
        }

        currentRecord = readCurrentRecord(record, 0);

        if (currentRecord != null)
          if (include(currentRecord))
            // FOUND
            return true;
      }

      // CLUSTER EXHAUSTED, TRY WITH THE PREVIOUS ONE
      currentClusterIdx--;

      updateClusterRange();

      currentEntry = lastClusterEntry;
      currentPositionIndex = currentPositions.length;
      currentPositions = dbStorage.getClusterPositionsForEntry(current.clusterId, currentEntry);
    }

    if (txEntries != null && txEntries.size() - (currentTxEntryPosition + 1) > 0)
      return true;

    currentRecord = null;
    return false;
  }

  public boolean hasNext() {
    checkDirection(true);

    if (currentRecord != null)
      return true;

    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return false;

    if (browsedRecords >= totalAvailableRecords)
      return false;

    // COMPUTE THE NUMBER OF RECORDS TO BROWSE
    if (liveUpdated)
      updateClusterRange();

    ORecordInternal<?> record = getRecord();

    // ITERATE UNTIL THE NEXT GOOD RECORD
    while (currentClusterIdx < clusterIds.length) {
      boolean thereAreRecordsToBrowse;
      if (current.clusterPosition <= -2)
        thereAreRecordsToBrowse = false;
      else if (currentEntry == lastClusterEntry
          && (currentPositions.length == 0 || currentPositionIndex >= currentPositions.length))
        thereAreRecordsToBrowse = false;
      else
        thereAreRecordsToBrowse = true;

      while (thereAreRecordsToBrowse) {
        if (!nextPosition(+1)) {
          currentRecord = null;
          break;
        }

        final long currentPosition = currentPosition();
        if (outsideOfTheRange(currentPosition))
          continue;

        currentRecord = readCurrentRecord(record, 0);

        if (currentRecord != null)
          if (include(currentRecord))
            // FOUND
            return true;
      }

      // CLUSTER EXHAUSTED, TRY WITH THE NEXT ONE
      currentClusterIdx++;
      if (currentClusterIdx >= clusterIds.length)
        break;

      updateClusterRange();

      currentEntry = firstClusterEntry;
      currentPositionIndex = -1;
      currentPositions = dbStorage.getClusterPositionsForEntry(current.clusterId, currentEntry);
    }

    // CHECK IN TX IF ANY
    if (txEntries != null && txEntries.size() - (currentTxEntryPosition + 1) > 0)
      return true;

    currentRecord = null;
    return false;
  }

  private boolean outsideOfTheRange(long currentPosition) {
    if (beginRange != null && currentPosition < beginRange.getClusterPosition())
      return true;

    if (endRange != null && currentPosition > endRange.getClusterPosition())
      return true;

    return false;
  }

  /**
   * Return the element at the current position and move forward the cursor to the next position available.
   * 
   * @return the next record found, otherwise the NoSuchElementException exception is thrown when no more records are found.
   */
  @SuppressWarnings("unchecked")
  public REC next() {
    checkDirection(true);

    if (currentRecord != null)
      try {
        // RETURN LAST LOADED RECORD
        return (REC) currentRecord;
      } finally {
        currentRecord = null;
      }

    ORecordInternal<?> record;

    // MOVE FORWARD IN THE CURRENT CLUSTER
    while (hasNext()) {
      if (currentRecord != null)
        try {
          // RETURN LAST LOADED RECORD
          return (REC) currentRecord;
        } finally {
          currentRecord = null;
        }

      record = getTransactionEntry();
      if (record == null)
        record = readCurrentRecord(null, +1);

      if (record != null)
        // FOUND
        if (include(record))
          return (REC) record;
    }

    record = getTransactionEntry();
    if (record != null)
      return (REC) record;

    throw new NoSuchElementException("Direction: forward, last position was: " + current + ", range: " + beginRange + "-"
        + endRange);
  }

  /**
   * Return the element at the current position and move backward the cursor to the previous position available.
   * 
   * @return the previous record found, otherwise the NoSuchElementException exception is thrown when no more records are found.
   */
  @SuppressWarnings("unchecked")
  @Override
  public REC previous() {
    checkDirection(false);

    if (currentRecord != null)
      try {
        // RETURN LAST LOADED RECORD
        return (REC) currentRecord;
      } finally {
        currentRecord = null;
      }

    ORecordInternal<?> record = getRecord();

    // MOVE BACKWARD IN THE CURRENT CLUSTER
    while (hasPrevious()) {
      if (currentRecord != null)
        try {
          // RETURN LAST LOADED RECORD
          return (REC) currentRecord;
        } finally {
          currentRecord = null;
        }

      if (record == null)
        record = readCurrentRecord(null, -1);

      if (record != null)
        // FOUND
        if (include(record))
          return (REC) record;
    }

    record = getTransactionEntry();
    if (record != null)
      return (REC) record;

    return null;
  }

  protected boolean include(final ORecord<?> iRecord) {
    return true;
  }

  /**
   * Move the iterator to the begin of the range. If no range was specified move to the first record of the cluster.
   * 
   * @return The object itself
   */
  @Override
  public ORecordIteratorClusters<REC> begin() {
    currentClusterIdx = 0;
    current.clusterId = clusterIds[currentClusterIdx];

    if (liveUpdated)
      updateClusterRange();

    currentEntry = firstClusterEntry;
    currentPositions = dbStorage.getClusterPositionsForEntry(current.clusterId, currentEntry);

    currentPositionIndex = -1;
    return this;
  }

  /**
   * Move the iterator to the end of the range. If no range was specified move to the last record of the cluster.
   * 
   * @return The object itself
   */
  @Override
  public ORecordIteratorClusters<REC> last() {
    currentClusterIdx = clusterIds.length - 1;
    if (liveUpdated)
      updateClusterRange();

    current.clusterId = currentClusterIdx;

    currentEntry = lastClusterEntry;
    currentPositions = dbStorage.getClusterPositionsForEntry(current.clusterId, currentEntry);

    currentPositionIndex = currentPositions.length;

    return this;
  }

  /**
   * Tell to the iterator that the upper limit must be checked at every cycle. Useful when concurrent deletes or additions change
   * the size of the cluster while you're browsing it. Default is false.
   * 
   * @param iLiveUpdated
   *          True to activate it, otherwise false (default)
   * @see #isLiveUpdated()
   */
  @Override
  public ORecordIteratorClusters<REC> setLiveUpdated(boolean iLiveUpdated) {
    super.setLiveUpdated(iLiveUpdated);

    if (iLiveUpdated) {
      firstClusterEntry = -1;
      lastClusterEntry = -1;
    } else {
      updateClusterRange();
    }

    return this;
  }

  protected void updateClusterRange() {
    current.clusterId = clusterIds[currentClusterIdx];
    final long[] range = database.getStorage().getClusterDataRange(current.clusterId);

    firstClusterEntry = range[0];
    lastClusterEntry = range[1];
  }

  protected void config() {
    if( clusterIds.length == 0 )
      return;
    
    currentClusterIdx = 0; // START FROM THE FIRST CLUSTER

    updateClusterRange();

    totalAvailableRecords = database.countClusterElements(clusterIds);

    txEntries = database.getTransaction().getRecordEntriesByClusterIds(clusterIds);

    if (txEntries != null)
      // ADJUST TOTAL ELEMENT BASED ON CURRENT TRANSACTION'S ENTRIES
      for (ORecordOperation entry : txEntries) {
        if (entry.getRecord().getIdentity().isTemporary() && entry.type != ORecordOperation.DELETED)
          totalAvailableRecords++;
        else if (entry.type == ORecordOperation.DELETED)
          totalAvailableRecords--;
      }

    begin();
  }

  @Override
  public String toString() {
    return String.format("ORecordIteratorCluster.clusters(%s).currentRecord(%s).range(%s-%s)", Arrays.toString(clusterIds),
        currentRecord, beginRange, endRange);
  }
}
