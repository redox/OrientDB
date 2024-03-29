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
package com.orientechnologies.orient.client.remote;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequestAsynch;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ODataSegment;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.enterprise.channel.binary.OAsynchChannelServiceThread;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

/**
 * This object is bound to each remote ODatabase instances.
 */
public class OStorageRemote extends OStorageAbstract implements OStorageProxy {
  private static final String              DEFAULT_HOST         = "localhost";
  private static final int                 DEFAULT_PORT         = 2424;
  private static final String              ADDRESS_SEPARATOR    = ";";

  public static final String               PARAM_MIN_POOL       = "minpool";
  public static final String               PARAM_MAX_POOL       = "maxpool";
  public static final String               PARAM_DB_TYPE        = "dbtype";

  private static final String              DRIVER_NAME          = "OrientDB Java";

  private final ExecutorService            asynchExecutor;
  private OAsynchChannelServiceThread      serviceThread;
  private OContextConfiguration            clientConfiguration;
  private int                              connectionRetry;
  private int                              connectionRetryDelay;

  private final List<OChannelBinaryClient> networkPool          = new ArrayList<OChannelBinaryClient>();
  private int                              networkPoolCursor    = 0;

  protected final List<String>             serverURLs           = new ArrayList<String>();
  private OCluster[]                       clusters             = new OCluster[0];
  protected final Map<String, OCluster>    clusterMap           = new ConcurrentHashMap<String, OCluster>();
  private int                              defaultClusterId;
  private int                              minPool;
  private int                              maxPool;
  private final boolean                    debug                = false;
  private ODocument                        clusterConfiguration = new ODocument();
  private ORemoteServerEventListener       asynchEventListener;
  private String                           connectionDbType;
  private String                           connectionUserName;
  private String                           connectionUserPassword;
  private Map<String, Object>              connectionOptions;
  private final String                     clientId;

  private final int                        maxReadQueue;

  public OStorageRemote(final String iClientId, final String iURL, final String iMode) throws IOException {
    super(iURL, iURL, iMode);
    clientId = iClientId;
    configuration = null;

    clientConfiguration = new OContextConfiguration();
    connectionRetry = clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY);
    connectionRetryDelay = clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY_DELAY);
    asynchEventListener = new OStorageRemoteAsynchEventListener(this);
    parseServerURLs();

    asynchExecutor = Executors.newSingleThreadScheduledExecutor();

    maxReadQueue = Runtime.getRuntime().availableProcessors() - 1;
  }

  public int getSessionId() {
    return OStorageRemoteThreadLocal.INSTANCE.get().sessionId.intValue();
  }

  public void setSessionId(final int iSessionId) {
    OStorageRemoteThreadLocal.INSTANCE.get().sessionId = iSessionId;
  }

  public ORemoteServerEventListener getAsynchEventListener() {
    return asynchEventListener;
  }

  public void setAsynchEventListener(final ORemoteServerEventListener iListener) {
    asynchEventListener = iListener;
  }

  public void removeRemoteServerEventListener() {
    asynchEventListener = null;
  }

  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iOptions) {
    addUser();

    lock.acquireExclusiveLock();
    try {

      connectionUserName = iUserName;
      connectionUserPassword = iUserPassword;
      connectionOptions = iOptions != null ? new HashMap<String, Object>(iOptions) : null; // CREATE A COPY TO AVOID USER
                                                                                           // MANIPULATION
                                                                                           // POST OPEN
      openRemoteDatabase();

      configuration = new OStorageConfiguration(this);
      configuration.load();

    } catch (Exception e) {
      if (!OGlobalConfiguration.STORAGE_KEEP_OPEN.getValueAsBoolean())
        close();

      if (e instanceof RuntimeException)
        // PASS THROUGH
        throw (RuntimeException) e;
      else
        throw new OStorageException("Cannot open the remote storage: " + name, e);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void reload() {
    checkConnection();

    lock.acquireExclusiveLock();
    try {

      do {
        try {

          OChannelBinaryClient network = null;
          try {
            network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_RELOAD);
          } finally {
            endRequest(network);
          }

          try {
            beginResponse(network);

            readDatabaseInformation(network);
            break;

          } finally {
            endResponse(network);
          }

        } catch (Exception e) {
          handleException("Error on reloading database information", e);

        }
      } while (true);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void create(final Map<String, Object> iOptions) {
    throw new UnsupportedOperationException(
        "Cannot create a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public boolean exists() {
    throw new UnsupportedOperationException(
        "Cannot check the existance of a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public void close(final boolean iForce) {
    OChannelBinaryClient network = null;

    lock.acquireExclusiveLock();
    try {

      synchronized (networkPool) {
        if (networkPool.size() > 0) {
          try {
            network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_CLOSE);
          } finally {
            endRequest(network);
          }
        }
      }

      setSessionId(-1);

      if (!checkForClose(iForce))
        return;

      // CLOSE THE CHANNEL
      if (serviceThread != null) {
        serviceThread.sendShutdown();
      }

      synchronized (networkPool) {
        for (OChannelBinaryClient n : networkPool)
          n.close();
        networkPool.clear();
      }

      level2Cache.shutdown();
      super.close(iForce);
      status = STATUS.CLOSED;

      Orient.instance().unregisterStorage(this);

    } catch (Exception e) {
      OLogManager.instance().debug(this, "Error on closing remote connection: %s", network);
      closeChannel(network);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void delete() {
    throw new UnsupportedOperationException(
        "Cannot delete a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public Set<String> getClusterNames() {
    lock.acquireSharedLock();
    try {

      return new HashSet<String>(clusterMap.keySet());

    } finally {
      lock.releaseSharedLock();
    }
  }

  public OPhysicalPosition createRecord(final int iDataSegmentId, final ORecordId iRid, final byte[] iContent, int iRecordVersion,
      final byte iRecordType, int iMode, final ORecordCallback<Long> iCallback) {
    checkConnection();

    if (iMode == 1 && iCallback == null)
      // ASYNCHRONOUS MODE NO ANSWER
      iMode = 2;

    final OPhysicalPosition ppos = new OPhysicalPosition(iDataSegmentId, -1, iRecordType);

    do {
      try {
        final OChannelBinaryClient network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_CREATE);
        try {
          if (network.getSrvProtocolVersion() >= 10)
            // SEND THE DATA SEGMENT ID
            network.writeInt(iDataSegmentId);
          network.writeShort((short) iRid.clusterId);
          network.writeBytes(iContent);
          network.writeByte(iRecordType);
          network.writeByte((byte) iMode);

        } finally {
          endRequest(network);
        }

        switch (iMode) {
        case 0:
          // SYNCHRONOUS
          try {
            beginResponse(network);
            iRid.clusterPosition = network.readLong();
            ppos.clusterPosition = iRid.clusterPosition;
            if (network.getSrvProtocolVersion() >= 11)
              ppos.recordVersion = network.readInt();
            else
              ppos.recordVersion = 0;
            return ppos;
          } finally {
            endResponse(network);
          }

        case 1:
          // ASYNCHRONOUS
          if (iCallback != null) {
            final int sessionId = getSessionId();
            Callable<Object> response = new Callable<Object>() {
              public Object call() throws Exception {
                final Long result;

                try {
                  OStorageRemoteThreadLocal.INSTANCE.get().sessionId = sessionId;
                  System.out.println("BEGIN ASYNCH READ " + OStorageRemoteThreadLocal.INSTANCE.get().sessionId);
                  beginResponse(network);
                  result = network.readLong();
                  if (network.getSrvProtocolVersion() >= 11)
                    network.readInt();
                } finally {
                  endResponse(network);
                  System.out.println("END   ASYNCH READ " + OStorageRemoteThreadLocal.INSTANCE.get().sessionId);
                  OStorageRemoteThreadLocal.INSTANCE.get().sessionId = -1;
                }
                iCallback.call(iRid, result);
                return null;
              }

            };
            asynchExecutor.submit(new FutureTask<Object>(response));
          }
        }
        return ppos;

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException("Error on create record in cluster: " + iRid.clusterId, e);

      }
    } while (true);
  }

  public ORawBuffer readRecord(final ORecordId iRid, final String iFetchPlan, final boolean iIgnoreCache,
      final ORecordCallback<ORawBuffer> iCallback) {
    checkConnection();

    if (OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return null;

    do {
      try {

        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_LOAD);
          network.writeRID(iRid);
          network.writeString(iFetchPlan != null ? iFetchPlan : "");
          if (network.getSrvProtocolVersion() >= 9)
            network.writeByte((byte) (iIgnoreCache ? 1 : 0));

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          if (network.readByte() == 0)
            return null;

          final ORawBuffer buffer = new ORawBuffer(network.readBytes(), network.readInt(), network.readByte());

          final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
          ORecordInternal<?> record;
          while (network.readByte() == 2) {
            record = (ORecordInternal<?>) OChannelBinaryProtocol.readIdentifiable(network);

            if (database != null)
              // PUT IN THE CLIENT LOCAL CACHE
              database.getLevel1Cache().updateRecord(record);
          }
          return buffer;

        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException("Error on read record " + iRid, e);

      }
    } while (true);
  }

  public int updateRecord(final ORecordId iRid, final byte[] iContent, final int iVersion, final byte iRecordType, int iMode,
      final ORecordCallback<Integer> iCallback) {
    checkConnection();

    if (iMode == 1 && iCallback == null)
      // ASYNCHRONOUS MODE NO ANSWER
      iMode = 2;

    do {
      try {
        final OChannelBinaryClient network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_UPDATE);
        try {
          network.writeRID(iRid);
          network.writeBytes(iContent);
          network.writeInt(iVersion);
          network.writeByte(iRecordType);
          network.writeByte((byte) iMode);

        } finally {
          endRequest(network);
        }

        switch (iMode) {
        case 0:
          // SYNCHRONOUS
          try {
            beginResponse(network);
            return network.readInt();
          } finally {
            endResponse(network);
          }

        case 1:
          // ASYNCHRONOUS
          if (iCallback != null) {
            final int sessionId = getSessionId();
            Callable<Object> response = new Callable<Object>() {
              public Object call() throws Exception {
                int result;

                try {
                  OStorageRemoteThreadLocal.INSTANCE.get().sessionId = sessionId;
                  beginResponse(network);
                  result = network.readInt();
                } finally {
                  endResponse(network);
                  OStorageRemoteThreadLocal.INSTANCE.get().sessionId = -1;
                }

                iCallback.call(iRid, result);
                return null;
              }

            };
            asynchExecutor.submit(new FutureTask<Object>(response));
          }
        }
        return iVersion;

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException("Error on update record " + iRid, e);

      }
    } while (true);
  }

  public boolean deleteRecord(final ORecordId iRid, final int iVersion, int iMode, final ORecordCallback<Boolean> iCallback) {
    checkConnection();

    if (iMode == 1 && iCallback == null)
      // ASYNCHRONOUS MODE NO ANSWER
      iMode = 2;

    do {
      try {
        final OChannelBinaryClient network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_DELETE);
        try {

          network.writeRID(iRid);
          network.writeInt(iVersion);
          network.writeByte((byte) iMode);

        } finally {
          endRequest(network);
        }

        switch (iMode) {
        case 0:
          // SYNCHRONOUS
          try {
            beginResponse(network);
            return network.readByte() == 1;
          } finally {
            endResponse(network);
          }

        case 1:
          // ASYNCHRONOUS
          if (iCallback != null) {
            final int sessionId = getSessionId();
            Callable<Object> response = new Callable<Object>() {
              public Object call() throws Exception {
                Boolean result;

                try {
                  OStorageRemoteThreadLocal.INSTANCE.get().sessionId = sessionId;
                  beginResponse(network);
                  result = network.readByte() == 1;
                } finally {
                  endResponse(network);
                  OStorageRemoteThreadLocal.INSTANCE.get().sessionId = -1;
                }

                iCallback.call(iRid, result);
                return null;
              }
            };
            asynchExecutor.submit(new FutureTask<Object>(response));
          }
        }
        return false;
      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException("Error on delete record " + iRid, e);

      }
    } while (true);
  }

  public long count(final int iClusterId) {
    return count(new int[] { iClusterId });
  }

  public long[] getClusterDataRange(final int iClusterId) {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_DATARANGE);

          network.writeShort((short) iClusterId);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return new long[] { network.readLong(), network.readLong() };
        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException("Error on getting last entry position count in cluster: " + iClusterId, e);

      }
    } while (true);
  }

  @Override
  public long[] getClusterPositionsForEntry(int currentClusterId, long entry) {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_POSITIONS);

          network.writeShort((short) currentClusterId);
          network.writeLong(entry);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          final int length = network.readInt();
          final long[] result = new long[length];

          for (int i = 0; i < length; i++)
            result[i] = network.readLong();

          return result;
        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException("Error on getting positions list from cluster : " + currentClusterId + " and entry : " + entry, e);
      }
    } while (true);

  }

  public long getSize() {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {

          network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_SIZE);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException("Error on read database size", e);

      }
    } while (true);
  }

  @Override
  public long countRecords() {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {

          network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException("Error on read database record count", e);

      }
    } while (true);
  }

  public long count(final int[] iClusterIds) {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_COUNT);

          network.writeShort((short) iClusterIds.length);
          for (int i = 0; i < iClusterIds.length; ++i)
            network.writeShort((short) iClusterIds[i]);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException("Error on read record count in clusters: " + Arrays.toString(iClusterIds), e);

      }
    } while (true);
  }

  @Override
  public void changeRecordIdentity(ORID originalId, ORID newId) {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_CHANGE_IDENTITY);

          network.writeShort((short) originalId.getClusterId());
          network.writeLong(originalId.getClusterPosition());

          network.writeShort((short) newId.getClusterId());
          network.writeLong(newId.getClusterPosition());

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return;
        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException("Error during changing identity of  " + originalId + " record to " + newId, e);
      }
    } while (true);
  }

  @Override
  public boolean isLHClustersAreUsed() {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_LH_CLUSTER_IS_USED);
        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          final boolean isLHClustersAreUsed = network.readByte() > 0;
          return isLHClustersAreUsed;
        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException("Error during requesting of cluster persistence mode", e);
      }
    } while (true);

  }

  /**
   * Execute the command remotely and get the results back.
   */
  public Object command(final OCommandRequestText iCommand) {
    checkConnection();

    if (!(iCommand instanceof OSerializableStream))
      throw new OCommandExecutionException("Cannot serialize the command to be executed to the server side.");

    OSerializableStream command = iCommand;
    Object result = null;

    final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.get();

    do {

      OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = true;
      try {

        final OCommandRequestText aquery = iCommand;

        final boolean asynch = iCommand instanceof OCommandRequestAsynch;

        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_COMMAND);

          network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
          network.writeBytes(OStreamSerializerAnyStreamable.INSTANCE.toStream(command));

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          if (asynch) {
            byte status;

            // ASYNCH: READ ONE RECORD AT TIME
            while ((status = network.readByte()) > 0) {
              final ORecordInternal<?> record = (ORecordInternal<?>) OChannelBinaryProtocol.readIdentifiable(network);
              if (record == null)
                break;

              switch (status) {
              case 1:
                // PUT AS PART OF THE RESULT SET. INVOKE THE LISTENER
                try {
                  if (!aquery.getResultListener().result(record)) {
                    // EMPTY THE INPUT CHANNEL
                    while (network.in.available() > 0)
                      network.in.read();

                    break;
                  }
                } catch (Throwable t) {
                  // ABSORBE ALL THE USER EXCEPTIONS
                  t.printStackTrace();
                }
                database.getLevel1Cache().updateRecord(record);
                break;

              case 2:
                // PUT IN THE CLIENT LOCAL CACHE
                database.getLevel1Cache().updateRecord(record);
              }
            }
          } else {
            final byte type = network.readByte();
            switch (type) {
            case 'n':
              result = null;
              break;

            case 'r':
              result = OChannelBinaryProtocol.readIdentifiable(network);
              if (result instanceof ORecord<?>)
                database.getLevel1Cache().updateRecord((ORecordInternal<?>) result);
              break;

            case 'l':
              final int tot = network.readInt();
              final Collection<OIdentifiable> list = new ArrayList<OIdentifiable>();
              for (int i = 0; i < tot; ++i) {
                final OIdentifiable resultItem = OChannelBinaryProtocol.readIdentifiable(network);
                if (resultItem instanceof ORecord<?>)
                  database.getLevel1Cache().updateRecord((ORecordInternal<?>) resultItem);
                list.add(resultItem);
              }
              result = list;
              break;

            case 'a':
              final String value = new String(network.readBytes());
              result = ORecordSerializerStringAbstract.fieldTypeFromStream(null, ORecordSerializerStringAbstract.getType(value),
                  value);
              break;
            }
          }
          break;
        } finally {
          endResponse(network);
        }

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException("Error on executing command: " + iCommand, e);

      } finally {
        OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = false;
      }
    } while (true);

    return result;
  }

  public void commit(final OTransaction iTx) {
    checkConnection();

    final List<ORecordOperation> committedEntries = new ArrayList<ORecordOperation>();
    do {
      try {
        OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = true;

        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_TX_COMMIT);

          network.writeInt(iTx.getId());
          network.writeByte((byte) (iTx.isUsingLog() ? 1 : 0));

          final List<ORecordOperation> tmpEntries = new ArrayList<ORecordOperation>();

          if (iTx.getCurrentRecordEntries().iterator().hasNext()) {
            while (iTx.getCurrentRecordEntries().iterator().hasNext()) {
              for (ORecordOperation txEntry : iTx.getCurrentRecordEntries())
                tmpEntries.add(txEntry);

              iTx.clearRecordEntries();

              if (tmpEntries.size() > 0) {
                for (ORecordOperation txEntry : tmpEntries) {
                  commitEntry(network, txEntry);
                  committedEntries.add(txEntry);
                }
                tmpEntries.clear();
              }
            }
          } else if (committedEntries.size() > 0) {
            for (ORecordOperation txEntry : committedEntries)
              commitEntry(network, txEntry);
          }

          // END OF RECORD ENTRIES
          network.writeByte((byte) 0);

          // SEND INDEX ENTRIES
          network.writeBytes(iTx.getIndexChanges().toStream());
        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          final int createdRecords = network.readInt();
          ORecordId currentRid;
          ORecordId createdRid;
          for (int i = 0; i < createdRecords; i++) {
            currentRid = network.readRID();
            createdRid = network.readRID();
            for (ORecordOperation txEntry : iTx.getAllRecordEntries()) {
              if (txEntry.getRecord().getIdentity().equals(currentRid)) {
                txEntry.getRecord().setIdentity(createdRid);
                break;
              }
            }
          }
          final int updatedRecords = network.readInt();
          ORecordId rid;
          for (int i = 0; i < updatedRecords; ++i) {
            rid = network.readRID();

            // SEARCH THE RECORD WITH THAT ID TO UPDATE THE VERSION
            for (ORecordOperation txEntry : iTx.getAllRecordEntries()) {
              if (txEntry.getRecord().getIdentity().equals(rid)) {
                txEntry.getRecord().setVersion(network.readInt());
                break;
              }
            }
          }

          committedEntries.clear();
        } finally {
          endResponse(network);
        }

        // SET ALL THE RECORDS AS UNDIRTY
        for (ORecordOperation txEntry : iTx.getAllRecordEntries())
          txEntry.getRecord().unload();

        // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT. USE THE STRATEGY TO ALWAYS REMOVE ALL THE RECORDS SINCE THEY COULD BE
        // CHANGED AS CONTENT IN CASE OF TREE AND GRAPH DUE TO CROSS REFERENCES
        OTransactionAbstract.updateCacheFromEntries(this, iTx, iTx.getAllRecordEntries(), false);

        break;

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException("Error on commit", e);

      } finally {
        OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = false;

      }
    } while (true);
  }

  public void rollback(OTransaction iTx) {
  }

  public int getClusterIdByName(final String iClusterName) {
    checkConnection();

    if (iClusterName == null)
      return -1;

    if (Character.isDigit(iClusterName.charAt(0)))
      return Integer.parseInt(iClusterName);

    final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());
    if (cluster == null)
      return -1;

    return cluster.getId();
  }

  public String getClusterTypeByName(final String iClusterName) {
    checkConnection();

    if (iClusterName == null)
      return null;

    final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());
    if (cluster == null)
      return null;

    return cluster.getType();
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public int addCluster(final String iClusterType, final String iClusterName, final String iLocation,
      final String iDataSegmentName, final Object... iArguments) {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD);

          network.writeString(iClusterType.toString());
          network.writeString(iClusterName);
          if (network.getSrvProtocolVersion() >= 10 || iClusterType.equalsIgnoreCase("PHYSICAL"))
            network.writeString(iLocation);
          if (network.getSrvProtocolVersion() >= 10)
            network.writeString(iDataSegmentName);
          else
            network.writeInt(-1);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          final int clusterId = network.readShort();

          final OClusterRemote cluster = new OClusterRemote();

          cluster.setType(iClusterType);
          cluster.configure(this, clusterId, iClusterName.toLowerCase(), null, 0);

          if (clusters.length <= clusterId)
            clusters = Arrays.copyOf(clusters, clusterId + 1);
          clusters[cluster.getId()] = cluster;
          clusterMap.put(cluster.getName().toLowerCase(), cluster);

          return clusterId;
        } finally {
          endResponse(network);
        }
      } catch (OModificationOperationProhibitedException mphe) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException("Error on add new cluster", e);
      }
    } while (true);
  }

  public boolean dropCluster(final int iClusterId) {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_DROP);

          network.writeShort((short) iClusterId);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          if (network.readByte() == 1) {
            // REMOVE THE CLUSTER LOCALLY
            final OCluster cluster = clusters[iClusterId];
            clusters[iClusterId] = null;
            clusterMap.remove(cluster.getName());
            if (configuration.clusters.size() > iClusterId)
              configuration.dropCluster(iClusterId);

            getLevel2Cache().freeCluster(iClusterId);
            return true;
          }
          return false;
        } finally {
          endResponse(network);
        }

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException("Error on removing of cluster", e);

      }
    } while (true);
  }

  public int addDataSegment(final String iDataSegmentName) {
    return addDataSegment(iDataSegmentName, null);
  }

  public int addDataSegment(final String iSegmentName, final String iLocation) {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATASEGMENT_ADD);

          network.writeString(iSegmentName).writeString(iLocation);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readInt();
        } finally {
          endResponse(network);
        }

      } catch (OModificationOperationProhibitedException mphe) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException("Error on add new data segment", e);
      }
    } while (true);
  }

  public boolean dropDataSegment(final String iSegmentName) {
    checkConnection();

    do {
      try {
        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATASEGMENT_DROP);

          network.writeString(iSegmentName);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readByte() == 1;
        } finally {
          endResponse(network);
        }

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException("Error on remove data segment", e);
      }
    } while (true);
  }

  public void synch() {
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    lock.acquireSharedLock();
    try {

      if (iClusterId >= clusters.length)
        return null;

      final OCluster cluster = clusters[iClusterId];
      return cluster != null ? cluster.getName() : null;

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getClusterMap() {
    return clusterMap.size();
  }

  public Collection<OCluster> getClusterInstances() {
    lock.acquireSharedLock();
    try {

      return Arrays.asList(clusters);

    } finally {
      lock.releaseSharedLock();
    }
  }

  public OCluster getClusterById(int iClusterId) {
    lock.acquireSharedLock();
    try {

      if (iClusterId == ORID.CLUSTER_ID_INVALID)
        // GET THE DEFAULT CLUSTER
        iClusterId = defaultClusterId;

      return clusters[iClusterId];

    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public long getVersion() {
    throw new UnsupportedOperationException("getVersion");
  }

  public ODocument getClusterConfiguration() {
    return clusterConfiguration;
  }

  /**
   * Handles exceptions. In case of IO errors retries to reconnect until the configured retry times has reached.
   * 
   * @param iMessage
   * @param iException
   */
  protected void handleException(final String iMessage, final Exception iException) {
    if (iException instanceof OTimeoutException) {
      // DO NOTHING
    } else if (iException instanceof OException)
      // RE-THROW IT
      throw (OException) iException;
    else if (!(iException instanceof IOException))
      throw new OStorageException(iMessage, iException);

    if (status != STATUS.OPEN)
      // STORAGE CLOSED: DON'T HANDLE RECONNECTION
      return;

    final long lostConnectionTime = System.currentTimeMillis();

    final int currentMaxRetry;
    final int currentRetryDelay;
    synchronized (clusterConfiguration) {
      if (!clusterConfiguration.isEmpty()) {
        // IN CLUSTER: NO RETRY AND 0 SLEEP TIME BETWEEN NODES
        currentMaxRetry = 1;
        currentRetryDelay = 0;
      } else {
        currentMaxRetry = connectionRetry;
        currentRetryDelay = connectionRetryDelay;
      }
    }

    for (int retry = 0; retry < currentMaxRetry; ++retry) {
      // WAIT THE DELAY BEFORE TO RETRY
      if (currentRetryDelay > 0)
        try {
          Thread.sleep(currentRetryDelay);
        } catch (InterruptedException e) {
          // THREAD INTERRUPTED: RETURN EXCEPTION
          Thread.currentThread().interrupt();
          break;
        }

      try {
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance()
              .debug(this, "Retrying to connect to remote server #" + (retry + 1) + "/" + currentMaxRetry + "...");

        openRemoteDatabase();

        OLogManager.instance().warn(this,
            "Connection re-acquired transparently after %dms and %d retries: no errors will be thrown at application level",
            System.currentTimeMillis() - lostConnectionTime, retry + 1);

        // RECONNECTED!
        return;

      } catch (Throwable t) {
        // DO NOTHING BUT CONTINUE IN THE LOOP
      }
    }

    // RECONNECTION FAILED: THROW+LOG THE ORIGINAL EXCEPTION
    throw new OStorageException(iMessage, iException);
  }

  protected void openRemoteDatabase() throws IOException {
    minPool = OGlobalConfiguration.CLIENT_CHANNEL_MIN_POOL.getValueAsInteger();
    maxPool = OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL.getValueAsInteger();
    connectionDbType = ODatabaseDocument.TYPE;

    if (connectionOptions != null && connectionOptions.size() > 0) {
      if (connectionOptions.containsKey(PARAM_MIN_POOL))
        minPool = Integer.parseInt(connectionOptions.get(PARAM_MIN_POOL).toString());
      if (connectionOptions.containsKey(PARAM_MAX_POOL))
        maxPool = Integer.parseInt(connectionOptions.get(PARAM_MAX_POOL).toString());
      if (connectionOptions.containsKey(PARAM_DB_TYPE))
        connectionDbType = connectionOptions.get(PARAM_DB_TYPE).toString();
    }

    setSessionId(-1);
    createConnectionPool();

    boolean availableConnections;
    synchronized (networkPool) {
      availableConnections = !networkPool.isEmpty();
    }

    while (availableConnections) {
      try {

        OChannelBinaryClient network = null;
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_OPEN);

          // @SINCE 1.0rc8
          sendClientInfo(network);

          network.writeString(name);

          if (network.getSrvProtocolVersion() >= 8)
            network.writeString(connectionDbType);

          network.writeString(connectionUserName);
          network.writeString(connectionUserPassword);

        } finally {
          endRequest(network);
        }

        final int sessionId;

        try {
          beginResponse(network);
          sessionId = network.readInt();
          setSessionId(sessionId);

          OLogManager.instance().debug(this, "Client connected with session id: " + sessionId);

          readDatabaseInformation(network);

          // READ CLUSTER CONFIGURATION
          updateClusterConfiguration(network.readBytes());
          status = STATUS.OPEN;
          return;

        } finally {
          endResponse(network);
        }
      } catch (IOException e) {
        OLogManager.instance().debug(this, "Error while reading response on creation of connection ", e);
      } catch (OTimeoutException e) {
        OLogManager.instance().debug(this, "Error while reading response on creation of connection ", e);
      } catch (Exception e) {
        handleException("Cannot create a connection to remote server address(es): " + serverURLs, e);
      }

      // CHECK AGAIN IF THERE ARE FREE CHANNELS
      synchronized (networkPool) {
        availableConnections = !networkPool.isEmpty();
      }
    }

    throw new OStorageException("Cannot create a connection to remote server address(es): " + serverURLs);

  }

  protected void sendClientInfo(OChannelBinaryClient network) throws IOException {
    if (network.getSrvProtocolVersion() >= 7) {
      // @COMPATIBILITY 1.0rc8
      network.writeString(DRIVER_NAME).writeString(OConstants.ORIENT_VERSION)
          .writeShort((short) OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION).writeString(clientId);
    }
  }

  /**
   * Parse the URL in the following formats:<br/>
   */
  protected void parseServerURLs() {
    int dbPos = url.indexOf('/');
    if (dbPos == -1) {
      // SHORT FORM
      addHost(url);
      name = url;
    } else {
      name = url.substring(dbPos + 1);
      for (String host : url.substring(0, dbPos).split(ADDRESS_SEPARATOR))
        host = addHost(host);
    }

    if (serverURLs.size() == 1 && OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_ENABLED.getValueAsBoolean()) {
      // LOOK FOR LOAD BALANCING DNS TXT RECORD
      final String primaryServer = serverURLs.get(0);

      try {
        final Hashtable<String, String> env = new Hashtable<String, String>();
        env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
        env.put("com.sun.jndi.ldap.connect.timeout",
            OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT.getValueAsString());
        final DirContext ictx = new InitialDirContext(env);
        final String hostName = primaryServer.indexOf(":") == -1 ? primaryServer : primaryServer.substring(0,
            primaryServer.indexOf(":"));
        final Attributes attrs = ictx.getAttributes(hostName, new String[] { "TXT" });
        final Attribute attr = attrs.get("TXT");
        if (attr != null) {
          String configuration = (String) attr.get();
          if (configuration.startsWith(""))
            configuration = configuration.substring(1, configuration.length() - 1);
          if (configuration != null) {
            final String[] parts = configuration.split(" ");
            for (String part : parts) {
              if (part.startsWith("s=")) {
                addHost(part.substring("s=".length()));
              }
            }
          }
        }
      } catch (NamingException e) {
      }
    }
  }

  /**
   * Registers the remote server with port.
   */
  protected String addHost(String host) {
    if (host.startsWith("localhost"))
      host = "127.0.0.1" + host.substring("localhost".length());

    // REGISTER THE REMOTE SERVER+PORT
    if (host.indexOf(":") == -1)
      host += ":" + getDefaultPort();

    if (!serverURLs.contains(host))
      serverURLs.add(host);

    return host;
  }

  protected String getDefaultHost() {
    return DEFAULT_HOST;
  }

  protected int getDefaultPort() {
    return DEFAULT_PORT;
  }

  protected OChannelBinaryClient createNetworkConnection() throws IOException, UnknownHostException {
    for (String server : serverURLs) {
      OLogManager.instance().debug(this, "Trying to connect to the remote host %s...", server);

      final int sepPos = server.indexOf(":");
      final String remoteHost = server.substring(0, sepPos);
      final int remotePort = Integer.parseInt(server.substring(sepPos + 1));

      try {
        return new OChannelBinaryClient(remoteHost, remotePort, clientConfiguration,
            OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);
      } catch (Exception e) {
        // GET THE NEXT ONE IF ANY
      }
    }

    final StringBuilder buffer = new StringBuilder();
    for (String server : serverURLs) {
      if (buffer.length() > 0)
        buffer.append(',');
      buffer.append(server);
    }

    throw new OIOException("Cannot connect to any configured remote nodes: " + buffer);
  }

  protected void checkConnection() {
    lock.acquireSharedLock();

    try {
      synchronized (networkPool) {

        if (networkPool.size() == 0)
          throw new ODatabaseException("Connection is closed");
      }

    } finally {
      lock.releaseSharedLock();
    }
  }

  /**
   * Acquire a network channel from the pool. Don't lock the write stream since the connection usage is exclusive.
   * 
   * @param iCommand
   * @return
   * @throws IOException
   */
  protected OChannelBinaryClient beginRequest(final byte iCommand) throws IOException {
    OChannelBinaryClient network = null;

    if (debug)
      System.out.println("-> req: " + getSessionId());

    // FIND THE FIRST FREE CHANNEL AVAILABLE
    synchronized (networkPool) {
      final int beginCursor = networkPoolCursor;

      while (network == null) {
        if (networkPool.size() == 0)
          openRemoteDatabase();

        if (networkPool.size() == 0)
          throw new ONetworkProtocolException("Connection pool closed");

        if (networkPoolCursor >= networkPool.size())
          // RESTART FROM THE FIRST ONE
          networkPoolCursor = 0;

        network = networkPool.get(networkPoolCursor);

        networkPoolCursor++;
        if (network.getLockWrite().tryLock())
          break;

        network = null;

        if (networkPoolCursor == beginCursor) {
          // COMPLETE ROUND AND NOT FREE CONNECTIONS FOUND

          if (networkPool.size() < maxPool) {
            // CREATE NEW CONNECTION
            network = createNetworkConnection();
            network.getLockWrite().lock();
            networkPool.add(network);

            if (debug)
              System.out.println("Created new connection " + networkPool.size());
          } else {
            if (debug)
              System.out.println("-> req (waiting) : " + getSessionId());

            final long startToWait = System.currentTimeMillis();
            try {
              networkPool.wait(5000);
            } catch (InterruptedException e) {
              // THREAD INTERRUPTED: RETURN EXCEPTION
              Thread.currentThread().interrupt();
              throw new OStorageException("Cannot acquire a connection because the thread has been interrupted");
            }

            final long elapsed = Orient.instance().getProfiler()
                .stopChrono("system.network.connectionPool.waitingTime", startToWait);

            if (debug)
              System.out.println("Waiting for connection = elapsed: " + elapsed);
          }
        }
      }
    }

    network.writeByte(iCommand);
    network.writeInt(getSessionId());

    return network;
  }

  /**
   * Ends the request and unlock the write lock
   */
  public void endRequest(final OChannelBinaryClient iNetwork) throws IOException {
    if (iNetwork == null)
      return;

    try {
      iNetwork.flush();
    } catch (IOException e) {
      try {
        iNetwork.close();
      } catch (Exception e2) {
      } finally {
        synchronized (networkPool) {
          networkPool.remove(iNetwork);
        }
      }
      throw e;
    } finally {

      iNetwork.getLockWrite().unlock();

      if (debug)
        System.out.println("<- req: " + getSessionId());

      synchronized (networkPool) {
        networkPool.notifyAll();
      }
    }
  }

  /**
   * Closes the channel and remove it from the pool.
   * 
   * @param iNetwork
   *          Channel to close and remove
   */
  protected void closeChannel(final OChannelBinaryClient iNetwork) {
    iNetwork.close();
    synchronized (networkPool) {
      networkPool.remove(iNetwork);
    }
  }

  /**
   * Starts listening the response.
   */
  protected void beginResponse(final OChannelBinaryClient iNetwork) throws IOException {
    iNetwork.beginResponse(getSessionId());

    if (iNetwork.getLockRead().getQueueLength() + 1 >= maxReadQueue)
      synchronized (networkPool) {
        if (networkPool.size() < maxPool) {
          // CREATE NEW CONNECTION
          final OChannelBinaryClient network = createNetworkConnection();
          networkPool.add(network);
        }
      }

    if (debug)
      System.out.println("-> res: " + getSessionId());
  }

  /**
   * End response reached: release the channel in the pool to being reused
   */
  public void endResponse(final OChannelBinaryClient iNetwork) {
    iNetwork.endResponse();

    if (debug)
      System.out.println("<- res: " + getSessionId());
  }

  public boolean isPermanentRequester() {
    return false;
  }

  protected void getResponse(final OChannelBinaryClient iNetwork) throws IOException {
    try {
      beginResponse(iNetwork);
    } finally {
      endResponse(iNetwork);
    }
  }

  @SuppressWarnings("unchecked")
  public void updateClusterConfiguration(final byte[] obj) {
    if (obj == null)
      return;

    // UPDATE IT
    synchronized (clusterConfiguration) {
      clusterConfiguration.fromStream(obj);

      final List<ODocument> members = clusterConfiguration.field("members");
      if (members != null) {
        // serverURLs.clear();

        for (ODocument m : members)
          if (m != null && !serverURLs.contains((String) m.field("id"))) {
            for (Map<String, Object> listener : ((Collection<Map<String, Object>>) m.field("listeners"))) {
              if (((String) listener.get("protocol")).equals("ONetworkProtocolBinary")) {
                String url = (String) listener.get("listen");
                if (!serverURLs.contains(url))
                  addHost(url);
              }
            }
          }
      }
    }
  }

  private void commitEntry(final OChannelBinaryClient iNetwork, final ORecordOperation txEntry) throws IOException {
    if (txEntry.type == ORecordOperation.LOADED)
      // JUMP LOADED OBJECTS
      return;

    // SERIALIZE THE RECORD IF NEEDED. THIS IS DONE HERE TO CATCH EXCEPTION AND SEND A -1 AS ERROR TO THE SERVER TO SIGNAL THE ABORT
    // OF TX COMMIT
    byte[] stream = null;
    try {
      switch (txEntry.type) {
      case ORecordOperation.CREATED:
      case ORecordOperation.UPDATED:
        stream = txEntry.getRecord().toStream();
        break;
      }
    } catch (Exception e) {
      // ABORT TX COMMIT
      iNetwork.writeByte((byte) -1);
      throw new OTransactionException("Error on transaction commit", e);
    }

    iNetwork.writeByte((byte) 1);
    iNetwork.writeByte(txEntry.type);
    iNetwork.writeShort((short) txEntry.getRecord().getIdentity().getClusterId());
    iNetwork.writeLong(txEntry.getRecord().getIdentity().getClusterPosition());
    iNetwork.writeByte(txEntry.getRecord().getRecordType());

    switch (txEntry.type) {
    case ORecordOperation.CREATED:
      iNetwork.writeBytes(stream);
      break;

    case ORecordOperation.UPDATED:
      iNetwork.writeInt(txEntry.getRecord().getVersion());
      iNetwork.writeBytes(stream);
      break;

    case ORecordOperation.DELETED:
      iNetwork.writeInt(txEntry.getRecord().getVersion());
      break;
    }
  }

  protected void createConnectionPool() throws IOException, UnknownHostException {
    synchronized (networkPool) {
      if (!networkPool.isEmpty()) {
        // CHECK EXISTENT NETWORK CONNECTIONS
        final List<OChannelBinaryClient> editableList = new ArrayList<OChannelBinaryClient>(networkPool);
        for (OChannelBinaryClient net : editableList) {
          if (!net.isConnected())
            // CLOSE IT AND REMOVE FROM THE LIST
            closeChannel(net);
        }
      }

      // CREATE THE CHANNEL POOL
      if (networkPool.size() == 0) {
        // ALWAYS CREATE AT LEAST ONE CONNECTION
        final OChannelBinaryClient firstChannel = createNetworkConnection();
        networkPool.add(firstChannel);
        serviceThread = new OAsynchChannelServiceThread(asynchEventListener, firstChannel, "OrientDB <- Asynch Client ("
            + firstChannel.socket.getRemoteSocketAddress() + ")");
      }

      // CREATE THE MINIMUM POOL
      for (int i = networkPool.size(); i < minPool; ++i)
        networkPool.add(createNetworkConnection());
    }
  }

  private boolean handleDBFreeze() {
    boolean retry;
    OLogManager.instance().warn(this,
        "DB is frozen will wait for " + OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT.getValue() + " ms. and then retry.");
    retry = true;
    try {
      Thread.sleep(OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT.getValueAsInteger());
    } catch (InterruptedException ie) {
      retry = false;

      Thread.currentThread().interrupt();
    }
    return retry;
  }

  private void readDatabaseInformation(final OChannelBinaryClient network) throws IOException {
    // @COMPATIBILITY 1.0rc8
    final int tot = network.getSrvProtocolVersion() >= 7 ? network.readShort() : network.readInt();

    clusters = new OCluster[tot];
    clusterMap.clear();

    for (int i = 0; i < tot; ++i) {
      final OClusterRemote cluster = new OClusterRemote();
      String clusterName = network.readString();
      if (clusterName != null)
        clusterName = clusterName.toLowerCase();
      final int clusterId = network.readShort();
      final String clusterType = network.readString();
      final int dataSegmentId = network.getSrvProtocolVersion() >= 12 ? (int) network.readShort() : 0;

      cluster.setType(clusterType);
      cluster.configure(this, clusterId, clusterName, null, dataSegmentId);

      if (clusterId >= clusters.length)
        clusters = Arrays.copyOf(clusters, clusterId + 1);
      clusters[clusterId] = cluster;
      clusterMap.put(clusterName, cluster);
    }

    defaultClusterId = clusterMap.get(CLUSTER_DEFAULT_NAME).getId();
  }

  @Override
  public String getURL() {
    return OEngineRemote.NAME + ":" + url;
  }

  public String getClientId() {
    return clientId;
  }

  public int getDataSegmentIdByName(final String iName) {
    if (iName == null)
      return 0;

    throw new UnsupportedOperationException("getDataSegmentIdByName()");
  }

  public ODataSegment getDataSegmentById(final int iDataSegmentId) {
    throw new UnsupportedOperationException("getDataSegmentById()");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OStorage#getClusters()
   */
  public int getClusters() {
    return clusterMap.size();
  }

  public void setDefaultClusterId(int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }
}
