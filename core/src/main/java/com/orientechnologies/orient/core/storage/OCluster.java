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
package com.orientechnologies.orient.core.storage;

import java.io.IOException;

import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;

/**
 * Handle the table to resolve logical address to physical address.<br/>
 * <br/>
 * Record structure:<br/>
 * <br/>
 * +---------------------------------------------+<br/>
 * | DATA SEGMENT........ | DATA OFFSET......... |<br/>
 * | 2 bytes = max 2^15-1 | 4 bytes = max 2^31-1 |<br/>
 * +---------------------------------------------+<br/>
 * = 6 bytes<br/>
 */
public interface OCluster {

  public static enum ATTRIBUTES {
    NAME, DATASEGMENT
  }

  public void configure(OStorage iStorage, int iId, String iClusterName, final String iLocation, int iDataSegmentId,
      Object... iParameters) throws IOException;

  public void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException;

  public void create(int iStartSize) throws IOException;

  public void open() throws IOException;

  public void close() throws IOException;

  public void delete() throws IOException;

  public void set(ATTRIBUTES iAttribute, Object iValue) throws IOException;

  /**
   * Truncates the cluster content. All the entries will be removed.
   * 
   * @throws IOException
   */
  public void truncate() throws IOException;

  public String getType();

  public int getDataSegmentId();

  /**
   * Adds a new entry.
   */
  public boolean addPhysicalPosition(OPhysicalPosition iPPosition) throws IOException;

  /**
   * Fills and return the PhysicalPosition object received as parameter with the physical position of logical record iPosition
   * 
   * @throws IOException
   */
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException;

  /**
   * Updates position in data segment (usually on defrag).
   */

  public void updateDataSegmentPosition(long iPosition, int iDataSegmentId, long iDataPosition) throws IOException;

  /**
   * Removes the Logical Position entry.
   */
  public void removePhysicalPosition(long iPosition) throws IOException;

  public void updateRecordType(long iPosition, final byte iRecordType) throws IOException;

  public void updateVersion(long iPosition, int iVersion) throws IOException;

  public long getEntries();

  public long getFirstEntryPosition();

  public long getLastEntryPosition();

  /**
   * Lets to an external actor to lock the cluster in shared mode. Useful for range queries to avoid atomic locking.
   * 
   * @see #unlock();
   */
  public void lock();

  /**
   * Lets to an external actor to unlock the shared mode lock acquired by the lock().
   * 
   * @see #lock();
   */
  public void unlock();

  public int getId();

  public void synch() throws IOException;

  public void setSoftlyClosed(boolean softlyClosed) throws IOException;

  public String getName();

  /**
   * Returns the size of the records contained in the cluster in bytes.
   * 
   * @return
   */
  public long getRecordsSize();

  public boolean generatePositionBeforeCreation();

  public OClusterEntryIterator absoluteIterator();

  public OPhysicalPosition[] getPositionsByEntryPos(long entryPosition) throws IOException;
}
