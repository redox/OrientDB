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
package com.orientechnologies.orient.core.index;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog.Listener;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.profiler.OJVMProfiler;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeDatabaseLazySave;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeProviderAbstract;

/**
 * Handles indexing when records change.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexMVRBTreeAbstract<T> extends OSharedResourceAdaptiveExternal implements OIndexInternal<T> {
  protected final OModificationLock              modificationLock = new OModificationLock();

  protected static final String                  CONFIG_MAP_RID   = "mapRid";
  protected static final String                  CONFIG_CLUSTERS  = "clusters";
  protected String                               name;
  protected String                               type;
  protected OMVRBTreeDatabaseLazySave<Object, T> map;
  protected Set<String>                          clustersToIndex  = new LinkedHashSet<String>();
  protected OIndexDefinition                     indexDefinition;
  protected final String                         databaseName;
  protected int                                  maxUpdatesBeforeSave;

  @ODocumentInstance
  protected ODocument                            configuration;
  private final Listener                         watchDog;

  public OIndexMVRBTreeAbstract(final String iType) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(), OGlobalConfiguration.MVRBTREE_TIMEOUT
        .getValueAsInteger(), true);

    databaseName = ODatabaseRecordThreadLocal.INSTANCE.get().getName();

    type = iType;
    watchDog = new Listener() {
      public void memoryUsageLow(final long iFreeMemory, final long iFreeMemoryPercentage) {
        map.setOptimization(iFreeMemoryPercentage < 10 ? 2 : 1);
      }
    };
  }

  public void flush() {
    lazySave();
  }

  /**
   * Creates the index.
   * 
   * @param iDatabase
   *          Current Database instance
   * @param iClusterIndexName
   *          Cluster name where to place the TreeMap
   * @param iProgressListener
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public OIndexInternal<?> create(final String iName, final OIndexDefinition iIndexDefinition, final ODatabaseRecord iDatabase,
      final String iClusterIndexName, final int[] iClusterIdsToIndex, final OProgressListener iProgressListener,
      final OStreamSerializer iValueSerializer) {
    acquireExclusiveLock();
    try {

      name = iName;
      configuration = new ODocument();

      indexDefinition = iIndexDefinition;
      maxUpdatesBeforeSave = lazyUpdates();

      if (iClusterIdsToIndex != null)
        for (final int id : iClusterIdsToIndex)
          clustersToIndex.add(iDatabase.getClusterNameById(id));

      if (indexDefinition != null) {
        if (indexDefinition instanceof ORuntimeKeyIndexDefinition) {
          map = new OMVRBTreeDatabaseLazySave<Object, T>(iClusterIndexName,
              ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer(), iValueSerializer, 1, maxUpdatesBeforeSave);
        } else {
          final OBinarySerializer<?> keySerializer;
          if (indexDefinition.getTypes().length > 1) {
            keySerializer = OCompositeKeySerializer.INSTANCE;
          } else {
            keySerializer = OBinarySerializerFactory.INSTANCE.getObjectSerializer(indexDefinition.getTypes()[0]);
          }
          map = new OMVRBTreeDatabaseLazySave<Object, T>(iClusterIndexName, (OBinarySerializer<Object>) keySerializer,
              iValueSerializer, indexDefinition.getTypes().length, maxUpdatesBeforeSave);
        }
      } else
        map = new OMVRBTreeDatabaseLazySave<Object, T>(iClusterIndexName, new OSimpleKeySerializer(), iValueSerializer, 1, maxUpdatesBeforeSave);

      installHooks(iDatabase);

      rebuild(iProgressListener);
      updateConfiguration();
    } catch (Exception e) {
      if (map != null)
        map.delete();
      if (e instanceof OIndexException)
        throw (OIndexException) e;

      throw new OIndexException("Cannot create the index '" + iName + "'", e);

    } finally {
      releaseExclusiveLock();
    }
    return this;
  }

  public OIndexInternal<T> loadFromConfiguration(final ODocument iConfig) {
    acquireExclusiveLock();
    try {

      final ORID rid = (ORID) iConfig.field(CONFIG_MAP_RID, ORID.class);
      if (rid == null)
        throw new OIndexException("Error during deserialization of index definition: '" + CONFIG_MAP_RID + "' attribute is null");

      configuration = iConfig;
      name = configuration.field(OIndexInternal.CONFIG_NAME);

      final ODocument indexDefinitionDoc = configuration.field(OIndexInternal.INDEX_DEFINITION);
      if (indexDefinitionDoc != null) {
        try {
          final String indexDefClassName = configuration.field(OIndexInternal.INDEX_DEFINITION_CLASS);
          final Class<?> indexDefClass = Class.forName(indexDefClassName);
          indexDefinition = (OIndexDefinition) indexDefClass.getDeclaredConstructor().newInstance();
          indexDefinition.fromStream(indexDefinitionDoc);

        } catch (final ClassNotFoundException e) {
          throw new OIndexException("Error during deserialization of index definition", e);
        } catch (final NoSuchMethodException e) {
          throw new OIndexException("Error during deserialization of index definition", e);
        } catch (final InvocationTargetException e) {
          throw new OIndexException("Error during deserialization of index definition", e);
        } catch (final InstantiationException e) {
          throw new OIndexException("Error during deserialization of index definition", e);
        } catch (final IllegalAccessException e) {
          throw new OIndexException("Error during deserialization of index definition", e);
        }
      } else {
        // @COMPATIBILITY 1.0rc6 new index model was implemented
        final Boolean isAutomatic = configuration.field(OIndexInternal.CONFIG_AUTOMATIC);
        if (Boolean.TRUE.equals(isAutomatic)) {
          final int pos = name.lastIndexOf('.');
          if (pos < 0)
            throw new OIndexException("Can not convert from old index model to new one. "
                + "Invalid index name. Dot (.) separator should be present.");
          final String className = name.substring(0, pos);
          final String propertyName = name.substring(pos + 1);

          final String keyTypeStr = configuration.field(OIndexInternal.CONFIG_KEYTYPE);
          if (keyTypeStr == null)
            throw new OIndexException("Can not convert from old index model to new one. " + "Index key type is absent.");
          final OType keyType = OType.valueOf(keyTypeStr.toUpperCase(Locale.ENGLISH));
          indexDefinition = new OPropertyIndexDefinition(className, propertyName, keyType);

          configuration.removeField(OIndexInternal.CONFIG_AUTOMATIC);
          configuration.removeField(OIndexInternal.CONFIG_KEYTYPE);
        } else if (configuration.field(OIndexInternal.CONFIG_KEYTYPE) != null) {
          final String keyTypeStr = configuration.field(OIndexInternal.CONFIG_KEYTYPE);
          final OType keyType = OType.valueOf(keyTypeStr.toUpperCase(Locale.ENGLISH));

          indexDefinition = new OSimpleKeyIndexDefinition(keyType);

          configuration.removeField(OIndexInternal.CONFIG_KEYTYPE);
        }
      }

      clustersToIndex.clear();
      maxUpdatesBeforeSave = lazyUpdates();

      final Collection<? extends String> clusters = configuration.field(CONFIG_CLUSTERS);
      if (clusters != null)
        clustersToIndex.addAll(clusters);

      map = new OMVRBTreeDatabaseLazySave<Object, T>(getDatabase(), rid, maxUpdatesBeforeSave);
      try {
        map.load();
      } catch (Exception e) {
        if (onCorruptionRepairDatabase(null, "load", "Index will be rebuilt")) {
          if (isAutomatic())
            // AUTOMATIC REBUILD IT
            OLogManager.instance().warn(this, "Cannot load index '%s' from storage (rid=%s): rebuilt it from scratch", getName(),
                rid);
          rebuild();
        }
      }

      installHooks(iConfig.getDatabase());

      return this;

    } finally {
      releaseExclusiveLock();
    }
  }

  public boolean contains(final Object iKey) {

    acquireExclusiveLock();
    try {

      return map.containsKey(iKey);

    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Returns a set of records with key between the range passed as parameter. Range bounds are included.
   * <p/>
   * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used as values boundaries.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iRangeTo
   *          Ending range
   * @return a set of records with key between the range passed as parameter. Range bounds are included.
   * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
   * @see #getValuesBetween(Object, boolean, Object, boolean)
   */
  public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final Object iRangeTo) {
    return getValuesBetween(iRangeFrom, true, iRangeTo, true);
  }

  /**
   * Returns a set of documents with key between the range passed as parameter. Range bounds are included.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iRangeTo
   *          Ending range
   * @see #getEntriesBetween(Object, Object, boolean)
   * @return
   */
  public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo) {
    return getEntriesBetween(iRangeFrom, iRangeTo, true);
  }

  public Collection<OIdentifiable> getValuesMajor(final Object fromKey, final boolean isInclusive) {
    return getValuesMajor(fromKey, isInclusive, -1);
  }

  public Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive) {
    return getValuesMinor(toKey, isInclusive, -1);
  }

  public Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive) {
    return getEntriesMajor(fromKey, isInclusive, -1);
  }

  public Collection<ODocument> getEntriesMinor(final Object toKey, final boolean isInclusive) {
    return getEntriesMinor(toKey, isInclusive, -1);
  }

  /**
   * Returns a set of records with key between the range passed as parameter.
   * <p/>
   * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used as values boundaries.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iFromInclusive
   *          Indicates whether start range boundary is included in result.
   * @param iRangeTo
   *          Ending range
   * @param iToInclusive
   *          Indicates whether end range boundary is included in result.
   * @return Returns a set of records with key between the range passed as parameter.
   * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
   */
  public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
      final boolean iToInclusive) {
    return getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive, -1);
  }

  public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive) {
    return getEntriesBetween(iRangeFrom, iRangeTo, iInclusive, -1);
  }

  public Collection<OIdentifiable> getValues(final Collection<?> iKeys) {
    return getValues(iKeys, -1);
  }

  public Collection<ODocument> getEntries(final Collection<?> iKeys) {
    return getEntries(iKeys, -1);
  }

  public ORID getIdentity() {
    return ((OMVRBTreeProviderAbstract<Object, ?>) map.getProvider()).getRecord().getIdentity();
  }

  public long rebuild() {
    return rebuild(null);
  }

  /**
   * Populates the index with all the existent records. Uses the massive insert intent to speed up and keep the consumed memory low.
   */
  public long rebuild(final OProgressListener iProgressListener) {
    long documentIndexed = 0;

    final boolean intentInstalled = getDatabase().declareIntent(new OIntentMassiveInsert());

    acquireExclusiveLock();
    try {
      try {
        map.clear();
      } catch (Exception e) {
        // IGNORE EXCEPTION: IF THE REBUILD WAS LAUNCHED IN CASE OF RID INVALID CLEAR ALWAYS GOES IN ERROR
      }

      int documentNum = 0;
      long documentTotal = 0;

      for (final String cluster : clustersToIndex)
        documentTotal += getDatabase().countClusterElements(cluster);

      if (iProgressListener != null)
        iProgressListener.onBegin(this, documentTotal);

      for (final String clusterName : clustersToIndex)
        try {
          for (final ORecord<?> record : getDatabase().browseCluster(clusterName)) {
            if (record instanceof ODocument) {
              final ODocument doc = (ODocument) record;

              if (indexDefinition == null)
                throw new OConfigurationException("Index '" + name + "' cannot be rebuilt because has no a valid definition ("
                    + indexDefinition + ")");

              final Object fieldValue = indexDefinition.getDocumentValueToIndex(doc);

              if (fieldValue != null) {
                if (fieldValue instanceof Collection) {
                  for (final Object fieldValueItem : (Collection<?>) fieldValue) {
                    put(fieldValueItem, doc);
                  }
                } else
                  put(fieldValue, doc);

                ++documentIndexed;
              }
            }
            documentNum++;

            if (iProgressListener != null)
              iProgressListener.onProgress(this, documentNum, documentNum * 100f / documentTotal);
          }
        } catch (NoSuchElementException e) {
          // END OF CLUSTER REACHED, IGNORE IT
        }

      lazySave();

      if (iProgressListener != null)
        iProgressListener.onCompletition(this, true);

    } catch (final Exception e) {
      if (iProgressListener != null)
        iProgressListener.onCompletition(this, false);

      try {
        map.clear();
      } catch (Exception e2) {
        // IGNORE EXCEPTION: IF THE REBUILD WAS LAUNCHED IN CASE OF RID INVALID CLEAR ALWAYS GOES IN ERROR
      }

      throw new OIndexException("Error on rebuilding the index for clusters: " + clustersToIndex, e);

    } finally {
      if (intentInstalled)
        getDatabase().declareIntent(null);

      releaseExclusiveLock();
    }

    return documentIndexed;
  }

  public boolean remove(final Object iKey, final OIdentifiable iValue) {
    modificationLock.requestModificationLock();
    try {
      return remove(iKey);
    } finally {
      modificationLock.releaseModificationLock();
    }

  }

  public boolean remove(final Object key) {
    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();
      try {

        return map.remove(key) != null;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public OIndex<T> clear() {
    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();
      try {

        map.clear();
        return this;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public OIndexInternal<T> delete() {
    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();

      try {
        map.delete();
        return this;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public OIndexInternal<T> lazySave() {

    acquireExclusiveLock();
    try {

      map.lazySave();
      return this;

    } finally {
      releaseExclusiveLock();
    }
  }

  public ORecord<?> getRecord() {
    return ((OMVRBTreeProviderAbstract<Object, ?>) map.getProvider()).getRecord();
  }

  public Iterator<Entry<Object, T>> iterator() {

    acquireExclusiveLock();
    try {

      return map.entrySet().iterator();

    } finally {
      releaseExclusiveLock();
    }
  }

  public Iterable<Object> keys() {

    acquireExclusiveLock();
    try {

      return map.keySet();

    } finally {
      releaseExclusiveLock();
    }
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  @Override
  public String toString() {
    return name;
  }

  public OIndexInternal<T> getInternal() {
    return this;
  }

  public Set<String> getClusters() {

    acquireSharedLock();
    try {

      return Collections.unmodifiableSet(clustersToIndex);

    } finally {
      releaseSharedLock();
    }
  }

  public OIndexMVRBTreeAbstract<T> addCluster(final String iClusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.add(iClusterName))
        updateConfiguration();
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndexMVRBTreeAbstract<T> removeCluster(String iClusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.remove(iClusterName))
        updateConfiguration();
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  public void checkEntry(final OIdentifiable iRecord, final Object iKey) {
  }

  public void unload() {

    acquireExclusiveLock();
    try {

      map.unload();

    } finally {
      releaseExclusiveLock();
    }
  }

  public ODocument updateConfiguration() {

    acquireExclusiveLock();
    try {

      configuration.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

      try {
        configuration.field(OIndexInternal.CONFIG_TYPE, type);
        configuration.field(OIndexInternal.CONFIG_NAME, name);

        if (indexDefinition != null) {
          final ODocument indexDefDocument = indexDefinition.toStream();
          if (!indexDefDocument.hasOwners())
            indexDefDocument.addOwner(configuration);

          configuration.field(OIndexInternal.INDEX_DEFINITION, indexDefDocument, OType.EMBEDDED);
          configuration.field(OIndexInternal.INDEX_DEFINITION_CLASS, indexDefinition.getClass().getName());
        } else {
          configuration.removeField(OIndexInternal.INDEX_DEFINITION);
          configuration.removeField(OIndexInternal.INDEX_DEFINITION_CLASS);
        }

        configuration.field(CONFIG_CLUSTERS, clustersToIndex, OType.EMBEDDEDSET);
        configuration.field(CONFIG_MAP_RID, ((OMVRBTreeProviderAbstract<Object, ?>) map.getProvider()).getRecord().getIdentity());

      } finally {
        configuration.setInternalStatus(ORecordElement.STATUS.LOADED);
      }

    } finally {
      releaseExclusiveLock();
    }
    return configuration;
  }

  @SuppressWarnings("unchecked")
  public void commit(final ODocument iDocument) {
    if (iDocument == null)
      return;

    acquireExclusiveLock();
    try {
      map.setRunningTransaction(true);

      final Boolean clearAll = (Boolean) iDocument.field("clear");
      if (clearAll != null && clearAll)
        clear();

      final Collection<ODocument> entries = iDocument.field("entries");

      for (final ODocument entry : entries) {
        final String serializedKey = OStringSerializerHelper.decode((String) entry.field("k"));

        final Object key;

        try {
          if (serializedKey.equals("*"))
            key = "*";
          else {
            final ODocument keyContainer = new ODocument();
            keyContainer.setLazyLoad(false);

            keyContainer.fromString(serializedKey);

            final Object storedKey = keyContainer.field("key");
            if (storedKey instanceof List)
              key = new OCompositeKey((List<? extends Comparable<?>>) storedKey);
            else if (Boolean.TRUE.equals(keyContainer.field("binary"))) {
              key = OStreamSerializerAnyStreamable.INSTANCE.fromStream((byte[]) storedKey);
            } else
              key = storedKey;
          }
        } catch (IOException ioe) {
          throw new OTransactionException("Error during index changes deserialization. ", ioe);
        }

        final List<ODocument> operations = (List<ODocument>) entry.field("ops");
        if (operations != null) {
          for (final ODocument op : operations) {
            final int operation = (Integer) op.rawField("o");
            final OIdentifiable value = op.field("v", OType.LINK);

            if (operation == OPERATION.PUT.ordinal())
              put(key, value);
            else if (operation == OPERATION.REMOVE.ordinal()) {
              if (key.equals("*"))
                remove(value);
              else if (value == null)
                remove(key);
              else
                remove(key, value);
            }
          }
        }
      }

    } finally {
      releaseExclusiveLock();
      map.setRunningTransaction(false);
    }
  }

  public ODocument getConfiguration() {
    return configuration;
  }

  public boolean isAutomatic() {
    return indexDefinition != null && indexDefinition.getClassName() != null;
  }

  protected void installHooks(final ODatabaseRecord iDatabase) {
    final OJVMProfiler profiler = Orient.instance().getProfiler();
    final String profilerPrefix = profiler.getDatabaseMetric(iDatabase.getName(), "index." + name + '.');

    profiler.registerHookValue(profilerPrefix + "items", new OProfilerHookValue() {
      public Object getValue() {
        acquireSharedLock();
        try {
          return map != null ? map.size() : "-";
        } finally {
          releaseSharedLock();
        }
      }
    });

    profiler.registerHookValue(profilerPrefix + "entryPointSize", new OProfilerHookValue() {
      public Object getValue() {
        return map != null ? map.getEntryPointSize() : "-";
      }
    });

    profiler.registerHookValue(profilerPrefix + "maxUpdateBeforeSave", new OProfilerHookValue() {
      public Object getValue() {
        return map != null ? map.getMaxUpdatesBeforeSave() : "-";
      }
    });

    profiler.registerHookValue(profilerPrefix + "optimizationThreshold", new OProfilerHookValue() {
      public Object getValue() {
        return map != null ? map.getOptimizeThreshold() : "-";
      }
    });

    Orient.instance().getMemoryWatchDog().addListener(watchDog);
    iDatabase.registerListener(this);
  }

  protected void uninstallHooks(final ODatabaseRecord iDatabase) {
    Orient.instance().getProfiler().unregisterHookValue("index." + name + ".items");
    Orient.instance().getProfiler().unregisterHookValue("index." + name + ".entryPointSize");
    Orient.instance().getProfiler().unregisterHookValue("index." + name + ".maxUpdateBeforeSave");
    Orient.instance().getProfiler().unregisterHookValue("index." + name + ".optimizationThreshold");
    Orient.instance().getMemoryWatchDog().removeListener(watchDog);
    iDatabase.unregisterListener(this);
  }

  public void onCreate(final ODatabase iDatabase) {
  }

  public void onDelete(final ODatabase iDatabase) {
  }

  public void onOpen(final ODatabase iDatabase) {
  }

  public void onBeforeTxBegin(final ODatabase iDatabase) {
    acquireExclusiveLock();
    try {

      map.commitChanges(true);

    } finally {
      releaseExclusiveLock();
    }
  }

  public void onBeforeTxRollback(final ODatabase iDatabase) {
  }

  public boolean onCorruptionRepairDatabase(final ODatabase iDatabase, final String iReason, String iWhatWillbeFixed) {
    if (iReason.equals("load"))
      return true;
    return false;
  }

  public void onAfterTxRollback(final ODatabase iDatabase) {

    acquireExclusiveLock();
    try {

      map.unload();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void onBeforeTxCommit(final ODatabase iDatabase) {
  }

  public void onAfterTxCommit(final ODatabase iDatabase) {
    acquireExclusiveLock();
    try {

      map.onAfterTxCommit();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void onClose(final ODatabase iDatabase) {
    acquireExclusiveLock();
    try {

      map.commitChanges();
      Orient.instance().getMemoryWatchDog().removeListener(watchDog);

    } finally {
      releaseExclusiveLock();
    }
  }

  protected void optimize(final boolean iHardMode) {
    if (map == null)
      return;

    acquireExclusiveLock();
    try {

      OLogManager.instance().debug(this,
          "Forcing " + (iHardMode ? "hard" : "soft") + " optimization of Index %s (%d items). Found %d entries in memory...", name,
          map.size(), map.getNumberOfNodesInCache());

      map.setOptimization(iHardMode ? 2 : 1);
      final int freed = map.optimize(iHardMode);

      OLogManager.instance().debug(this, "Completed! Freed %d entries and now %d entries reside in memory", freed,
          map.getNumberOfNodesInCache());

    } finally {
      releaseExclusiveLock();
    }
  }

  protected void checkForKeyType(final Object iKey) {
    if (indexDefinition == null) {
      // RECOGNIZE THE KEY TYPE AT RUN-TIME

      final OType type = OType.getTypeByClass(iKey.getClass());
      if (type == null)
        return;

      indexDefinition = new OSimpleKeyIndexDefinition(type);
      maxUpdatesBeforeSave = lazyUpdates();
      updateConfiguration();
    }
  }

  protected ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public OType[] getKeyTypes() {
    if (indexDefinition == null)
      return null;

    return indexDefinition.getTypes();
  }

  public OIndexDefinition getDefinition() {
    return indexDefinition;
  }

  public void freeze(boolean throwException) {
    modificationLock.prohibitModifications(throwException);
  }

  public void release() {
    modificationLock.allowModifications();
  }

  public void acquireModificationLock() {
    modificationLock.requestModificationLock();
  }

  public void releaseModificationLock() {
    modificationLock.releaseModificationLock();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OIndexMVRBTreeAbstract<?> that = (OIndexMVRBTreeAbstract<?>) o;

    if (!name.equals(that.name))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  private int lazyUpdates() {
    return isAutomatic() ?
        OGlobalConfiguration.INDEX_AUTO_LAZY_UPDATES.getValueAsInteger() :
        OGlobalConfiguration.INDEX_MANUAL_LAZY_UPDATES.getValueAsInteger();
  }
}
