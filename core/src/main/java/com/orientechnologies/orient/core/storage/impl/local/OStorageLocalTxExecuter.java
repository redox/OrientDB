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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageTxConfiguration;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTxListener;

public class OStorageLocalTxExecuter {
  private final OStorageLocal storage;
  private final OTxSegment    txSegment;
  private OTransaction        currentTransaction;

  public OStorageLocalTxExecuter(final OStorageLocal iStorage, final OStorageTxConfiguration iConfig) throws IOException {
    storage = iStorage;

    iConfig.path = OStorageVariableParser.DB_PATH_VARIABLE + "/txlog.otx";

    txSegment = new OTxSegment(storage, iStorage.getConfiguration().txSegment);
  }

  public void open() throws IOException {
    try {
      
      txSegment.open();
      
    } catch (FileNotFoundException e) {
      OLogManager.instance().warn(this, "Creating new txlog file '%s'", txSegment.getFile());
      create();
    } catch (Exception e) {
      OLogManager.instance().warn(this, "Error on opening the txlog file '%s', reset it", e, txSegment.getFile());
      create();
    }
  }

  public void create() throws IOException {
    txSegment.create(0);
  }

  public void close() throws IOException {
    txSegment.close();
  }

  protected OPhysicalPosition createRecord(final int iTxId, final ODataLocal iDataSegment, final OCluster iClusterSegment,
      final ORecordId iRid, final byte[] iContent, final int iRecordVersion, final byte iRecordType, int dataSegmentId) {

    try {
      final OPhysicalPosition ppos = storage.createRecord(iDataSegment, iClusterSegment, iContent, iRecordType, iRid,
          iRecordVersion);

      // SAVE INTO THE LOG THE POSITION OF THE RECORD JUST CREATED. IF TX FAILS AT THIS POINT A GHOST RECORD IS CREATED UNTIL DEFRAG
      txSegment.addLog(OTxSegment.OPERATION_CREATE, iTxId, iRid.clusterId, iRid.clusterPosition, iRecordType, 0, null,
          dataSegmentId);

      return ppos;
    } catch (IOException e) {

      OLogManager.instance().error(this, "Error on creating entry in log segment: " + iClusterSegment, e,
          OTransactionException.class);
      return null;
    }
  }

  /**
   * Stores the new content in a new position, then saves in the log the coords of the new position. At free time the
   * 
   * @param iTxId
   * @param iClusterSegment
   * @param iRid
   * @param iContent
   * @param iVersion
   * @param iRecordType
   * @return
   */

  protected int updateRecord(final int iTxId, final OCluster iClusterSegment, final ORecordId iRid, final byte[] iContent,
      final int iVersion, final byte iRecordType) {
    try {
      // READ CURRENT RECORD CONTENT
      final ORawBuffer buffer = storage.readRecord(iClusterSegment, iRid, true);

      if (buffer == null)
        throw new ORecordNotFoundException("The record with id " + iRid + " was not found");

      // SAVE INTO THE LOG THE POSITION OF THE OLD RECORD JUST DELETED. IF TX FAILS AT THIS POINT AS ABOVE
      txSegment.addLog(OTxSegment.OPERATION_UPDATE, iTxId, iRid.clusterId, iRid.clusterPosition, iRecordType, buffer.version,
          buffer.buffer, -1);

      final OPhysicalPosition ppos = storage.updateRecord(iClusterSegment, iRid, iContent, iVersion, iRecordType);
      if (ppos != null)
        return ppos.recordVersion;

      return -1;

    } catch (IOException e) {

      OLogManager.instance().error(this, "Error on updating entry #" + iRid + " in log segment: " + iClusterSegment, e,
          OTransactionException.class);
    }
    return -1;
  }

  protected boolean deleteRecord(final int iTxId, final OCluster iClusterSegment, final long iPosition, final int iVersion) {
    try {
      final ORecordId rid = new ORecordId(iClusterSegment.getId(), iPosition);

      // READ CURRENT RECORD CONTENT
      final ORawBuffer buffer = storage.readRecord(iClusterSegment, rid, true);

      if (buffer != null) {
        // SAVE INTO THE LOG THE OLD RECORD
        final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(new OPhysicalPosition(iPosition));
        txSegment.addLog(OTxSegment.OPERATION_DELETE, iTxId, iClusterSegment.getId(), iPosition, buffer.recordType, buffer.version,
            buffer.buffer, ppos.dataSegmentId);

        return storage.deleteRecord(iClusterSegment, rid, iVersion) != null;
      }

    } catch (IOException e) {

      OLogManager.instance().error(this, "Error on deleting entry #" + iPosition + " in log segment: " + iClusterSegment, e,
          OTransactionException.class);
    }
    return false;
  }

  public OTxSegment getTxSegment() {
    return txSegment;
  }

  public void commitAllPendingRecords(final OTransaction iTx) throws IOException {
    currentTransaction = iTx;
    try {
      // COPY ALL THE ENTRIES IN SEPARATE COLLECTION SINCE DURING THE COMMIT PHASE SOME NEW ENTRIES COULD BE CREATED AND
      // CONCURRENT-EXCEPTION MAY OCCURS
      final List<ORecordOperation> tmpEntries = new ArrayList<ORecordOperation>();

      while (iTx.getCurrentRecordEntries().iterator().hasNext()) {
        for (ORecordOperation txEntry : iTx.getCurrentRecordEntries())
          tmpEntries.add(txEntry);

        iTx.clearRecordEntries();

        if (!tmpEntries.isEmpty()) {
          for (ORecordOperation txEntry : tmpEntries)
            // COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
            commitEntry(iTx, txEntry, iTx.isUsingLog());
        }
      }

      // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT
      OTransactionAbstract.updateCacheFromEntries(storage, iTx, iTx.getAllRecordEntries(), true);
    } finally {
      currentTransaction = null;
    }
  }

  public void clearLogEntries(final OTransaction iTx) throws IOException {
    // CLEAR ALL TEMPORARY RECORDS
    txSegment.clearLogEntries(iTx.getId());
  }

  private void commitEntry(final OTransaction iTx, final ORecordOperation txEntry, final boolean iUseLog) throws IOException {

    if (txEntry.type != ORecordOperation.DELETED && !txEntry.getRecord().isDirty())
      return;

    final ORecordId rid = (ORecordId) txEntry.getRecord().getIdentity();

    if (rid.clusterId == ORID.CLUSTER_ID_INVALID && txEntry.getRecord() instanceof ODocument
        && ((ODocument) txEntry.getRecord()).getSchemaClass() != null) {
      // TRY TO FIX CLUSTER ID TO THE DEFAULT CLUSTER ID DEFINED IN SCHEMA CLASS
      rid.clusterId = ((ODocument) txEntry.getRecord()).getSchemaClass().getDefaultClusterId();
    }

    final OCluster cluster = storage.getClusterById(rid.clusterId);
    final ODataLocal dataSegment = storage.getDataSegmentById(txEntry.dataSegmentId);

    if (cluster.getName().equals(OMetadata.CLUSTER_INDEX_NAME) || cluster.getName().equals(OMetadata.CLUSTER_MANUAL_INDEX_NAME))
      // AVOID TO COMMIT INDEX STUFF
      return;

    if (!(cluster instanceof OClusterLocal || cluster instanceof OClusterLocalLHPEPS))
      // ONLY LOCAL CLUSTER ARE INVOLVED IN TX
      return;

    if (txEntry.getRecord() instanceof OTxListener)
      ((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.BEFORE_COMMIT);

    switch (txEntry.type) {
    case ORecordOperation.LOADED:
      break;

    case ORecordOperation.CREATED: {
      // CHECK 2 TIMES TO ASSURE THAT IT'S A CREATE OR AN UPDATE BASED ON RECURSIVE TO-STREAM METHOD
      byte[] stream = txEntry.getRecord().toStream();

      final ORID oldRid = rid.copy();

      if (rid.isNew()) {
        txEntry.getRecord().onBeforeIdentityChanged(rid);
        rid.clusterId = cluster.getId();
      }

      if (rid.isNew()) {
        final OPhysicalPosition ppos;
        if (iUseLog)
          ppos = createRecord(iTx.getId(), dataSegment, cluster, rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord()
              .getRecordType(), txEntry.dataSegmentId);
        else
          ppos = iTx.getDatabase().getStorage()
              .createRecord(txEntry.dataSegmentId, rid, stream, 0, txEntry.getRecord().getRecordType(), (byte) 0, null);

        rid.clusterPosition = ppos.clusterPosition;
        txEntry.getRecord().setVersion(ppos.recordVersion);

        txEntry.getRecord().onAfterIdentityChanged(txEntry.getRecord());
        iTx.updateIndexIdentityAfterCommit(oldRid, rid);
      } else {
        if (iUseLog)
          txEntry.getRecord()
              .setVersion(
                  updateRecord(iTx.getId(), cluster, rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord()
                      .getRecordType()));
        else
          txEntry.getRecord()
              .setVersion(
                  iTx.getDatabase()
                      .getStorage()
                      .updateRecord(rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType(), (byte) 0,
                          null));
      }
      break;
    }

    case ORecordOperation.UPDATED: {
      byte[] stream = txEntry.getRecord().toStream();

      if (iUseLog)
        txEntry.getRecord().setVersion(
            updateRecord(iTx.getId(), cluster, rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType()));
      else
        txEntry.getRecord().setVersion(
            iTx.getDatabase().getStorage()
                .updateRecord(rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType(), (byte) 0, null));
      break;
    }

    case ORecordOperation.DELETED: {
      if (iUseLog)
        deleteRecord(iTx.getId(), cluster, rid.clusterPosition, txEntry.getRecord().getVersion());
      else
        iTx.getDatabase().getStorage().deleteRecord(rid, txEntry.getRecord().getVersion(), (byte) 0, null);
    }
      break;
    }

    txEntry.getRecord().unsetDirty();

    if (txEntry.getRecord() instanceof OTxListener)
      ((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.AFTER_COMMIT);
  }

  public boolean isCommitting() {
    return currentTransaction != null;
  }

  public OTransaction getCurrentTransaction() {
    return currentTransaction;
  }
}
