package com.orientechnologies.orient.server.hazelcast.sharding.distributed;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedThreadLocal;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public class OLocalDHTNode implements ODHTNode {
  public static final String                    REPLICATOR_USER   = "replicator";

  private AtomicLong                            predecessor       = new AtomicLong(-1);

  private final long                            id;
  private final AtomicLongArray                 fingerPoints      = new AtomicLongArray(63);

  private volatile long                         migrationId       = -1;
  private volatile ODHTNodeLookup               nodeLookup;
  private volatile ODHTConfiguration            dhtConfiguration;
  private final AtomicInteger                   next              = new AtomicInteger(1);

  private final OLockManager<ORID, Runnable>    lockManager       = new OLockManager<ORID, Runnable>(true, 500);

  private final ExecutorService                 executorService   = Executors.newCachedThreadPool();
  private final Queue<Long>                     notificationQueue = new ConcurrentLinkedQueue<Long>();

  private volatile NodeState                    state;

  private boolean                               inheritedDatabase;
  private static final OServerUserConfiguration replicatorUser    = OServerMain.server().getUser(REPLICATOR_USER);

  public OLocalDHTNode(long id) {
    this.id = id;
    for (int i = 0; i < fingerPoints.length(); i++)
      fingerPoints.set(i, -1);
  }

  public ODHTNodeLookup getNodeLookup() {
    return nodeLookup;
  }

  public void setNodeLookup(ODHTNodeLookup nodeLookup) {
    this.nodeLookup = nodeLookup;
  }

  public void setDhtConfiguration(ODHTConfiguration dhtConfiguration) {
    this.dhtConfiguration = dhtConfiguration;
  }

  public void create() {
    log("New ring creation was started");

    predecessor.set(-1);
    fingerPoints.set(0, id);
    state = NodeState.STABLE;

    log("New ring was created");
  }

  public long getNodeId() {
    return id;
  }

  public boolean join(long joinNodeId) {
    try {
      log("Join is started using node with id " + joinNodeId);

      final ODHTNode node = nodeLookup.findById(joinNodeId);
      if (node == null) {
        log("Node with id " + joinNodeId + " is absent.");
        return false;
      }

      state = NodeState.JOIN;
      predecessor.set(-1);
      fingerPoints.set(0, node.findSuccessor(id));

      log("Join completed, successor is " + fingerPoints.get(0));

      ODHTNode successor = nodeLookup.findById(fingerPoints.get(0));
      if (successor == null) {
        log("Node with id " + fingerPoints.get(0) + " is absent .");
        return false;
      }

      successor.notify(id);

      log("Join was finished");

      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public long findSuccessor(long keyId) {
    final long successorId = fingerPoints.get(0);

    if (insideInterval(id, successorId, keyId, true))
      return successorId;

    long nodeId = findClosestPrecedingFinger(keyId);
    ODHTNode node = nodeLookup.findById(nodeId);

    return node.findSuccessor(keyId);
  }

  private long findClosestPrecedingFinger(long keyId) {
    for (int i = fingerPoints.length() - 1; i >= 0; i--) {
      final long fingerPoint = fingerPoints.get(i);
      if (fingerPoint > -1 && insideInterval(this.id, keyId, fingerPoint, false)) {
        return fingerPoint;
      }
    }

    return this.id;
  }

  public long getSuccessor() {
    return fingerPoints.get(0);
  }

  public Long getPredecessor() {
    return predecessor.get();
  }

  public void requestMigration(long requesterId) {
    executorService.submit(new MergeCallable(requesterId));
    log("Data migration was started for node " + requesterId);
  }

  @Override
  public OPhysicalPosition createRecord(String storageName, ORecordId iRecordId, byte[] iContent, int iRecordVersion,
      byte iRecordType) {
    while (state == NodeState.JOIN) {
      log("Wait till node will be joined.");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Interrupted", e);
      }
    }

    return executeCreateRecord(storageName, iRecordId, iContent, iRecordVersion, iRecordType);
  }

  private OPhysicalPosition executeCreateRecord(String storageName, ORecordId iRecordId, byte[] iContent, int iRecordVersion,
      byte iRecordType) {
    ODistributedThreadLocal.INSTANCE.distributedExecution = true;
    try {
      final ORecordInternal<?> record = Orient.instance().getRecordFactoryManager().newInstance(iRecordType);

      final ODatabaseDocumentTx database = openDatabase(storageName);

      try {
        record.fill(iRecordId, iRecordVersion, iContent, true);
        if (iRecordId.getClusterId() == -1)
          record.save(true);
        else
          record.save(database.getClusterNameById(iRecordId.getClusterId()), true);

        return new OPhysicalPosition(record.getIdentity().getClusterPosition(), record.getVersion());
      } finally {
        closeDatabase(database);
      }
    } finally {
      ODistributedThreadLocal.INSTANCE.distributedExecution = false;
    }
  }

  @Override
  public ORawBuffer readRecord(String storageName, ORID iRid) {
    while (state == NodeState.JOIN) {
      log("Wait till node will be joined.");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (state == NodeState.MERGING) {
      ORawBuffer data = executeReadRecord(storageName, iRid);

      if (data == null) {
        final ODHTNode successorNode = nodeLookup.findById(fingerPoints.get(0));
        data = successorNode.readRecord(storageName, iRid);
        if (data == null && successorNode.getNodeId() != id)
          return executeReadRecord(storageName, iRid);
        else
          return data;
      } else
        return data;
    }

    return executeReadRecord(storageName, iRid);
  }

  private ORawBuffer executeReadRecord(String storageName, ORID iRid) {
    lockManager.acquireLock(Thread.currentThread(), iRid, OLockManager.LOCK.EXCLUSIVE);
    try {
      final ODatabaseDocumentTx database = openDatabase(storageName);
      try {

        return new ORawBuffer(database.load(iRid));

      } finally {
        closeDatabase(database);
      }
    } finally {
      lockManager.releaseLock(Thread.currentThread(), iRid, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  @Override
  public int updateRecord(String storageName, ORecordId iRecordId, byte[] iContent, int iVersion, byte iRecordType) {
    final ORecordInternal<?> record = Orient.instance().getRecordFactoryManager().newInstance(iRecordType);

    lockManager.acquireLock(Thread.currentThread(), iRecordId, OLockManager.LOCK.EXCLUSIVE);
    try {
      final ODatabaseDocumentTx database = openDatabase(storageName);
      try {
        record.fill(iRecordId, iVersion, iContent, true);
        if (iRecordId.getClusterId() == -1)
          record.save();
        else
          record.save(database.getClusterNameById(iRecordId.getClusterId()));

        return record.getVersion();
      } finally {
        closeDatabase(database);
      }
    } finally {
      lockManager.releaseLock(Thread.currentThread(), iRecordId, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  @Override
  public boolean deleteRecord(String storageName, ORecordId iRecordId, int iVersion) {
    boolean result = false;

    while (state == NodeState.JOIN) {
      log("Wait till node will be joined.");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (state == NodeState.MERGING) {
      final ODHTNode successorNode = nodeLookup.findById(fingerPoints.get(0));
      result = successorNode.deleteRecord(storageName, iRecordId, iVersion);
    }

    result = result | executeDeleteRecord(storageName, iRecordId, iVersion);

    return result;
  }

  @Override
  public boolean isLocal() {
    return true;
  }

  private boolean executeDeleteRecord(String storageName, final ORecordId iRecordId, int iVersion) {
    lockManager.acquireLock(Thread.currentThread(), iRecordId, OLockManager.LOCK.EXCLUSIVE);
    try {
      final ODatabaseDocumentTx database = openDatabase(storageName);
      try {
        final ORecordInternal record = database.load(iRecordId);
        if (record != null) {
          record.setVersion(iVersion);
          record.delete();
          return true;
        }
        return false;
      } finally {
        closeDatabase(database);
      }
    } finally {
      lockManager.releaseLock(Thread.currentThread(), iRecordId, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  protected ODatabaseDocumentTx openDatabase(String databaseName) {
    inheritedDatabase = true;

    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && db.getName().equals(databaseName) && !db.isClosed()) {
      if (db instanceof ODatabaseDocumentTx)
        return (ODatabaseDocumentTx) db;
      else if (db.getDatabaseOwner() instanceof ODatabaseDocumentTx)
        return (ODatabaseDocumentTx) db.getDatabaseOwner();
    }

    inheritedDatabase = false;
    return (ODatabaseDocumentTx) OServerMain.server().openDatabase("document", databaseName, replicatorUser.name,
        replicatorUser.password);
  }

  protected void closeDatabase(final ODatabaseDocumentTx iDatabase) {
    if (!inheritedDatabase)
      iDatabase.close();
  }

  public void stabilize() {
    // log("Stabilization is started");
    boolean result = false;

    ODHTNode successor = null;

    while (!result) {
      long successorId = fingerPoints.get(0);

      successor = nodeLookup.findById(successorId);

      Long predecessor = successor.getPredecessor();

      if (predecessor > -1 && insideInterval(this.id, successorId, predecessor, false)) {
        log("Successor was " + successorId + " is going to be changed to " + predecessor);

        result = fingerPoints.compareAndSet(0, successorId, predecessor);

        if (result)
          log("Successor was successfully changed");
        else
          log("Successor change was failed");

        if (result)
          successor = nodeLookup.findById(predecessor);

        drawRing();
      } else
        result = true;
    }

    if (successor.getNodeId() != id)
      successor.notify(id);

    // drawRing();
    // log("Stabilization is finished");
  }

  public void fixFingers() {
    int nextValue = next.intValue();

    // log("Fix of fingers is started for interval " + ((id + 1 << nextValue) & Long.MAX_VALUE));

    fingerPoints.set(nextValue, findSuccessor((id + 1 << nextValue) & Long.MAX_VALUE));

    next.compareAndSet(nextValue, nextValue + 1);

    while (next.intValue() > 62) {
      nextValue = next.intValue();
      if (nextValue > 62)
        next.compareAndSet(nextValue, 1);

      // log("Next value is changed to 1");
    }

    // log("Fix of fingers was finished.");
  }

  public void fixPredecessor() {
    // log("Fix of predecessor is started");

    boolean result = false;

    while (!result) {
      long predecessorId = predecessor.longValue();

      if (predecessorId > -1 && nodeLookup.findById(predecessorId) == null) {
        result = predecessor.compareAndSet(predecessorId, -1);

        // log("Predecessor " + predecessorId + " left the cluster");
      } else
        result = true;
    }

    // log("Fix of predecessor is finished");
  }

  public void notify(long nodeId) {
    // log("Node " + nodeId + " thinks it can be our parent");

    boolean result = false;

    while (!result) {
      long predecessorId = predecessor.longValue();

      if (predecessorId < 0 || (insideInterval(predecessorId, this.id, nodeId, false))) {
        result = predecessor.compareAndSet(predecessorId, nodeId);
        if (result)
          log("New predecessor is " + nodeId);
        else
          log("Predecessor setup was failed.");

        if (result && predecessorId < 0 && state == NodeState.JOIN) {

          migrationId = fingerPoints.get(0);
          final ODHTNode mergeNode = nodeLookup.findById(migrationId);
          mergeNode.requestMigration(id);

          state = NodeState.MERGING;
          log("Status was changed to " + state);
        }

        drawRing();
      } else
        result = true;
    }

    // log("Parent check is finished.");

  }

  public void notifyMigrationEnd(long nodeId) {
    log("Migration completion notification from " + nodeId);

    while (state == NodeState.JOIN) {
      log("Wait till node will be joined.");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (nodeId == migrationId) {
      state = NodeState.STABLE;
      log("State was changed to " + state);

      Long nodeToNotifyId = notificationQueue.poll();
      while (nodeToNotifyId != null) {
        final ODHTNode node = nodeLookup.findById(nodeToNotifyId);
        node.notifyMigrationEnd(id);
        nodeToNotifyId = notificationQueue.poll();
      }
    }
  }

  private boolean insideInterval(long from, long to, long value, boolean rightIsIncluded) {
    if (to > from) {
      if (rightIsIncluded)
        return from < value && to >= value;
      else
        return from < value && to > value;
    } else {
      if (rightIsIncluded)
        return !(value > to && value <= from);
      else
        return !(value >= to && value <= from);
    }
  }

  private void log(String message) {
    DateFormat dateFormat = DateFormat.getDateTimeInstance();

    System.out.println(state + " : " + Thread.currentThread().getName() + " : " + id + " : " + dateFormat.format(new Date())
        + " : " + message);
  }

  private void drawRing() {
    StringBuilder builder = new StringBuilder();

    builder.append("Ring : ");

    builder.append(id);
    ODHTNode node = this;

    Set<Long> processedIds = new HashSet<Long>();
    processedIds.add(id);

    long successor = node.getSuccessor();
    while (!processedIds.contains(successor)) {
      builder.append("-").append(successor);
      processedIds.add(successor);

      node = nodeLookup.findById(successor);
      successor = node.getSuccessor();
    }

    builder.append(".");

    log(builder.toString());
  }

  private enum NodeState {
    JOIN, MERGING, STABLE
  }

  private final class MergeCallable implements Callable<Void> {
    private final long requesterNode;

    private MergeCallable(long requesterNode) {
      this.requesterNode = requesterNode;
    }

    public Void call() throws Exception {
      for (String storageName : dhtConfiguration.getDistributedStorageNames()) {

        final ODatabaseDocumentTx db = openDatabase(storageName);

        final Set<String> clusterNames = db.getStorage().getClusterNames();
        for (String clusterName : clusterNames) {
          if (dhtConfiguration.getUndistributableClusters().contains(clusterName.toLowerCase())) {
            continue;
          }

          final ORecordIteratorCluster<? extends ORecordInternal<?>> it = db.browseCluster(clusterName);
          while (it.hasNext()) {
            final ORecordInternal<?> rec = it.next();
            lockManager.acquireLock(Thread.currentThread(), rec.getIdentity(), OLockManager.LOCK.EXCLUSIVE);
            try {
              final long successorId = findSuccessor(rec.getIdentity().getClusterPosition());
              if (successorId != id) {
                final ODHTNode node = nodeLookup.findById(successorId);

                node.createRecord(storageName, (ORecordId) rec.getIdentity(), rec.toStream(), rec.getVersion(), rec.getRecordType());

                ODistributedThreadLocal.INSTANCE.distributedExecution = true;
                try {
                  rec.delete();
                } finally {
                  ODistributedThreadLocal.INSTANCE.distributedExecution = false;
                }
              }
            } finally {
              lockManager.releaseLock(Thread.currentThread(), rec.getIdentity(), OLockManager.LOCK.EXCLUSIVE);
            }
          }
        }
      }

      if (state == NodeState.STABLE) {
        final ODHTNode node = nodeLookup.findById(requesterNode);
        node.notifyMigrationEnd(id);
      } else {
        notificationQueue.add(requesterNode);
        if (state == NodeState.STABLE) {
          Long nodeToNotifyId = notificationQueue.poll();
          while (nodeToNotifyId != null) {
            final ODHTNode node = nodeLookup.findById(nodeToNotifyId);
            node.notifyMigrationEnd(id);
            nodeToNotifyId = notificationQueue.poll();
          }
        }
      }

      log("Migration was successfully finished for node " + requesterNode);
      return null;
    }
  }
}
