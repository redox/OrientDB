/*
 * Copyright 2012 Sylvain Utard (sylvain.utard--at--gmail.com)
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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Lucene-based index for full-text searches.
 * 
 * @author Sylvain Utard
 * 
 */
public class OIndexLuceneFullText implements OIndexInternal<OIdentifiable> {

    public static final String TYPE_ID = OClass.INDEX_TYPE.LUCENE_FULLTEXT.toString();
    
    private String name;

    public OIndexLuceneFullText() {
    }

    @Override
    public OIndex<OIdentifiable> create(String iName, OIndexDefinition iIndexDefinition, ODatabaseRecord iDatabase,
            String iClusterIndexName, int[] iClusterIdsToIndex, OProgressListener iProgressListener) {
        this.name = iName;
        throw new Error("Not implemented");
    }

    @Override
    public void unload() {
        throw new Error("Not implemented");
    }

    @Override
    public String getDatabaseName() {
        throw new Error("Not implemented");
    }

    @Override
    public OType[] getKeyTypes() {
        throw new Error("Not implemented");
    }

    @Override
    public Iterator<Entry<Object, OIdentifiable>> iterator() {
        throw new Error("Not implemented");
    }

    @Override
    public OIdentifiable get(Object iKey) {
        throw new Error("Not implemented");
    }

    @Override
    public long count(Object iKey) {
        throw new Error("Not implemented");
    }

    @Override
    public boolean contains(Object iKey) {
        throw new Error("Not implemented");
    }

    @Override
    public OIndex<OIdentifiable> put(Object iKey, OIdentifiable iValue) {
        throw new Error("Not implemented");
    }

    @Override
    public boolean remove(Object iKey) {
        throw new Error("Not implemented");
    }

    @Override
    public boolean remove(Object iKey, OIdentifiable iRID) {
        throw new Error("Not implemented");
    }

    @Override
    public int remove(OIdentifiable iRID) {
        throw new Error("Not implemented");
    }

    @Override
    public OIndex<OIdentifiable> clear() {
        throw new Error("Not implemented");
    }

    @Override
    public Iterable<Object> keys() {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, Object iRangeTo) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo,
            boolean iToInclusive) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo,
            boolean iToInclusive, int maxValuesToFetch) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive, int maxValuesToFetch) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive, int maxValuesToFetch) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive, int maxEntriesToFetch) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, int maxEntriesToFetch) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, int maxEntriesToFetch) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo) {
        throw new Error("Not implemented");
    }

    @Override
    public long getSize() {
        throw new Error("Not implemented");
    }

    @Override
    public long getKeySize() {
        throw new Error("Not implemented");
    }

    @Override
    public void checkEntry(OIdentifiable iRecord, Object iKey) {
        throw new Error("Not implemented");
    }

    @Override
    public OIndex<OIdentifiable> lazySave() {
        throw new Error("Not implemented");
    }

    @Override
    public OIndex<OIdentifiable> delete() {
        throw new Error("Not implemented");
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return TYPE_ID;
    }

    @Override
    public boolean isAutomatic() {
        throw new Error("Not implemented");
    }

    @Override
    public long rebuild() {
        throw new Error("Not implemented");
    }

    @Override
    public long rebuild(OProgressListener iProgressListener) {
        throw new Error("Not implemented");
    }

    @Override
    public ODocument getConfiguration() {
        throw new Error("Not implemented");
    }

    @Override
    public ORID getIdentity() {
        throw new Error("Not implemented");
    }

    @Override
    public void commit(ODocument iDocument) {
        throw new Error("Not implemented");
    }

    @Override
    public OIndexInternal<OIdentifiable> getInternal() {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<OIdentifiable> getValues(Collection<?> iKeys) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<OIdentifiable> getValues(Collection<?> iKeys, int maxValuesToFetch) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<ODocument> getEntries(Collection<?> iKeys) {
        throw new Error("Not implemented");
    }

    @Override
    public Collection<ODocument> getEntries(Collection<?> iKeys, int maxEntriesToFetch) {
        throw new Error("Not implemented");
    }

    @Override
    public OIndexDefinition getDefinition() {
        throw new Error("Not implemented");
    }

    @Override
    public Set<String> getClusters() {
        throw new Error("Not implemented");
    }

    @Override
    public void onCreate(ODatabase iDatabase) {
        throw new Error("Not implemented");
    }

    @Override
    public void onDelete(ODatabase iDatabase) {
        throw new Error("Not implemented");
    }

    @Override
    public void onOpen(ODatabase iDatabase) {
        throw new Error("Not implemented");
    }

    @Override
    public void onBeforeTxBegin(ODatabase iDatabase) {
        throw new Error("Not implemented");
    }

    @Override
    public void onBeforeTxRollback(ODatabase iDatabase) {
        throw new Error("Not implemented");
    }

    @Override
    public void onAfterTxRollback(ODatabase iDatabase) {
        throw new Error("Not implemented");
    }

    @Override
    public void onBeforeTxCommit(ODatabase iDatabase) {
        throw new Error("Not implemented");
    }

    @Override
    public void onAfterTxCommit(ODatabase iDatabase) {
        throw new Error("Not implemented");
    }

    @Override
    public void onClose(ODatabase iDatabase) {
        throw new Error("Not implemented");
    }

    @Override
    public boolean onCorruptionRepairDatabase(ODatabase iDatabase, String iReason, String iWhatWillbeFixed) {
        throw new Error("Not implemented");
    }

    @Override
    public void flush() {
        throw new Error("Not implemented");
    }

    @Override
    public int count(OIdentifiable iRecord) {
        throw new Error("Not implemented");
    }

    @Override
    public OIndexInternal<OIdentifiable> loadFromConfiguration(ODocument iConfig) {
        throw new Error("Not implemented");
    }

    @Override
    public ODocument updateConfiguration() {
        throw new Error("Not implemented");
    }

    @Override
    public OIndex<OIdentifiable> addCluster(String iClusterName) {
        throw new Error("Not implemented");
    }

    @Override
    public OIndex<OIdentifiable> removeCluster(String iClusterName) {
        throw new Error("Not implemented");
    }

    @Override
    public boolean canBeUsedInEqualityOperators() {
        throw new Error("Not implemented");
    }

    @Override
    public void freeze(boolean throwException) {
        throw new Error("Not implemented");
    }

    @Override
    public void release() {
        throw new Error("Not implemented");
    }

    @Override
    public void acquireModificationLock() {
        throw new Error("Not implemented");
    }

    @Override
    public void releaseModificationLock() {
        throw new Error("Not implemented");
    }

}
