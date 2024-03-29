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
package com.orientechnologies.orient.core.hook;

import com.orientechnologies.orient.core.record.ORecord;

/**
 * Hook interface to catch all events regarding records.
 * 
 * @author Luca Garulli
 * @see ORecordHookAbstract
 * 
 */
public interface ORecordHook {
  public enum TYPE {
    ANY, BEFORE_CREATE, BEFORE_READ, BEFORE_UPDATE, BEFORE_DELETE, AFTER_CREATE, AFTER_READ, AFTER_UPDATE, AFTER_DELETE, UPDATE_FAILED, CREATE_FAILED, DELETE_FAILED
  };

  public enum RESULT {
    RECORD_NOT_CHANGED, RECORD_CHANGED, SKIP
  }

  public RESULT onTrigger(TYPE iType, ORecord<?> iRecord);
}
