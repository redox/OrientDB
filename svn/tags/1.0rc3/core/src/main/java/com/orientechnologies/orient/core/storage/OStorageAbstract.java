/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.common.concur.resource.OSharedContainerImpl;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.orient.core.cache.OLevel2RecordCache;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public abstract class OStorageAbstract extends OSharedContainerImpl implements OStorage {
	protected final String										url;
	protected final String										mode;
	protected OStorageConfiguration						configuration;
	protected String													name;
	protected AtomicLong											version	= new AtomicLong();
	protected OLevel2RecordCache							level2Cache;

	protected volatile boolean								open		= false;
	protected OSharedResourceAdaptiveExternal	lock		= new OSharedResourceAdaptiveExternal(
																												OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());

	public OStorageAbstract(final String iName, final String iFilePath, final String iMode) {
		if (OStringSerializerHelper.contains(iName, '/'))
			name = iName.substring(iName.lastIndexOf("/") + 1);
		else
			name = iName;

		if (OStringSerializerHelper.contains(iName, ','))
			throw new IllegalArgumentException("Invalid character in storage name: " + name);

		level2Cache = new OLevel2RecordCache(this);
		level2Cache.startup();

		url = iFilePath;
		mode = iMode;
	}

	public OStorageConfiguration getConfiguration() {
		return configuration;
	}

	public boolean isClosed() {
		return !open;
	}

	public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
		return ppos != null && ppos.version != -1;
	}

	public String getName() {
		return name;
	}

	/**
	 * Returns the configured local Level-2 cache component. Cache component is always created even if not used.
	 * 
	 * @return
	 */
	public OLevel2RecordCache getLevel2Cache() {
		return level2Cache;
	}

	public String getURL() {
		return url;
	}

	public void close() {
		close(false);
	}

	/**
	 * Returns current storage's version as serial.
	 */
	public long getVersion() {
		return version.get();
	}

	/**
	 * Update the storage's version
	 */
	protected void incrementVersion() {
		version.incrementAndGet();
	}

	public boolean dropCluster(final String iClusterName) {
		return dropCluster(getClusterIdByName(iClusterName));
	}

	protected boolean checkForClose(final boolean iForce) {
		if (!open)
			return false;

		final int remainingUsers = getUsers() > 0 ? removeUser() : 0;

		return iForce || (!OGlobalConfiguration.STORAGE_KEEP_OPEN.getValueAsBoolean() && remainingUsers == 0);
	}

	public int getUsers() {
		return lock.getUsers();
	}

	public int addUser() {
		return lock.addUser();
	}

	public int removeUser() {
		return lock.removeUser();
	}

	public OSharedResourceAdaptive getLock() {
		return lock;
	}

	public long countRecords() {
		long tot = 0;

		for (OCluster c : getClusters())
			if (c != null)
				tot += c.getEntries();

		return tot;
	}

	@Override
	public String toString() {
		return url != null ? url : "?";
	}
}
