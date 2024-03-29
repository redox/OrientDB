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
package com.orientechnologies.orient.server.hazelcast;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.DataSerializable;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

/**
 * Serialization-optimized implementation to transfer only the needed information on the network for the distributed create
 * operation.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPhysicalPosition extends OPhysicalPosition implements DataSerializable {
  private static final long serialVersionUID = 1L;

  public OHazelcastPhysicalPosition() {
  }

  public OHazelcastPhysicalPosition(final OPhysicalPosition iFrom) {
    clusterPosition = iFrom.clusterPosition;
    recordVersion = iFrom.recordVersion;
  }

  @Override
  public void readData(final DataInput in) throws IOException {
    clusterPosition = in.readLong();
    recordVersion = in.readInt();
  }

  @Override
  public void writeData(final DataOutput out) throws IOException {
    out.writeLong(clusterPosition);
    out.writeInt(recordVersion);
  }
}
