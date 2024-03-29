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
package com.orientechnologies.orient.core.record.impl;

import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement.STATUS;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordTrackedList;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Helper class to manage documents.
 * 
 * @author Luca Garulli
 */
public class ODocumentHelper {
  public static final String ATTRIBUTE_THIS    = "@this";
  public static final String ATTRIBUTE_RID     = "@rid";
  public static final String ATTRIBUTE_VERSION = "@version";
  public static final String ATTRIBUTE_CLASS   = "@class";
  public static final String ATTRIBUTE_TYPE    = "@type";
  public static final String ATTRIBUTE_SIZE    = "@size";
  public static final String ATTRIBUTE_FIELDS  = "@fields";
  public static final String ATTRIBUTE_RAW     = "@raw";

  public static void sort(List<OIdentifiable> ioResultSet, List<OPair<String, String>> iOrderCriteria) {
    if (ioResultSet != null)
      Collections.sort(ioResultSet, new ODocumentComparator(iOrderCriteria));
  }

  @SuppressWarnings("unchecked")
  public static <RET> RET convertField(final ODocument iDocument, final String iFieldName, final Class<?> iFieldType, Object iValue) {
    if (iFieldType == null)
      return (RET) iValue;

    if (ORID.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof ORID) {
        return (RET) iValue;
      } else if (iValue instanceof String) {
        return (RET) new ORecordId((String) iValue);
      } else if (iValue instanceof ORecord<?>) {
        return (RET) ((ORecord<?>) iValue).getIdentity();
      }
    } else if (ORecord.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof ORID || iValue instanceof ORecord<?>) {
        return (RET) iValue;
      } else if (iValue instanceof String) {
        return (RET) new ORecordId((String) iValue);
      }
    } else if (Set.class.isAssignableFrom(iFieldType)) {
      if (!(iValue instanceof Set)) {
        // CONVERT IT TO SET
        final Collection<?> newValue;

        if (iValue instanceof ORecordLazyList || iValue instanceof ORecordLazyMap)
          newValue = new OMVRBTreeRIDSet(iDocument);
        else
          newValue = new OTrackedSet<Object>(iDocument);

        if (iValue instanceof Collection<?>) {
          ((Collection<Object>) newValue).addAll((Collection<Object>) iValue);
          return (RET) newValue;
        } else if (iValue instanceof Map) {
          ((Collection<Object>) newValue).addAll(((Map<String, Object>) iValue).values());
          return (RET) newValue;
        } else if (iValue instanceof String) {
          final String stringValue = (String) iValue;

          if (stringValue != null && !stringValue.isEmpty()) {
            final String[] items = stringValue.split(",");
            for (String s : items) {
              ((Collection<Object>) newValue).add(s);
            }
          }
          return (RET) newValue;
        }
      } else {
        return (RET) iValue;
      }
    } else if (List.class.isAssignableFrom(iFieldType)) {
      if (!(iValue instanceof List)) {
        // CONVERT IT TO LIST
        final Collection<?> newValue;

        if (iValue instanceof OMVRBTreeRIDSet || iValue instanceof ORecordLazyMap)
          newValue = new ORecordLazyList(iDocument);
        else
          newValue = new OTrackedList<Object>(iDocument);

        if (iValue instanceof Collection) {
          ((Collection<Object>) newValue).addAll((Collection<Object>) iValue);
          return (RET) newValue;
        } else if (iValue instanceof Map) {
          ((Collection<Object>) newValue).addAll(((Map<String, Object>) iValue).values());
          return (RET) newValue;
        } else if (iValue instanceof String) {
          final String stringValue = (String) iValue;

          if (stringValue != null && !stringValue.isEmpty()) {
            final String[] items = stringValue.split(",");
            for (String s : items) {
              ((Collection<Object>) newValue).add(s);
            }
          }
          return (RET) newValue;
        }
      } else {
        return (RET) iValue;
      }
    } else if (iValue instanceof Enum) {
      // ENUM
      if (Number.class.isAssignableFrom(iFieldType))
        iValue = ((Enum<?>) iValue).ordinal();
      else
        iValue = iValue.toString();
      if (!(iValue instanceof String) && !iFieldType.isAssignableFrom(iValue.getClass()))
        throw new IllegalArgumentException("Property '" + iFieldName + "' of type '" + iFieldType
            + "' cannot accept value of type: " + iValue.getClass());
    } else if (Date.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof String && ODatabaseRecordThreadLocal.INSTANCE.isDefined()) {
        final OStorageConfiguration config = iDocument.getDatabase().getStorage().getConfiguration();

        DateFormat formatter = config.getDateFormatInstance();

        if (((String) iValue).length() > config.dateFormat.length()) {
          // ASSUMES YOU'RE USING THE DATE-TIME FORMATTE
          formatter = config.getDateTimeFormatInstance();
        }

        try {
          Date newValue = formatter.parse((String) iValue);
          // _fieldValues.put(iFieldName, newValue);
          return (RET) newValue;
        } catch (ParseException pe) {
          final String dateFormat = ((String) iValue).length() > config.dateFormat.length() ? config.dateTimeFormat
              : config.dateFormat;
          throw new OQueryParsingException("Error on conversion of date '" + iValue + "' using the format: " + dateFormat);
        }
      }
    }

    iValue = OType.convert(iValue, iFieldType);

    return (RET) iValue;
  }

  @SuppressWarnings("unchecked")
  public static <RET> RET getFieldValue(Object value, final String iFieldName) {
    if (value == null)
      return null;

    if (value instanceof Collection || value.getClass().isArray()) {
      final List<Object> tempResult = new ArrayList<Object>();
      for (Object o : OMultiValue.getMultiValueIterable(value)) {
        final Object result = ODocumentHelper.getFieldValue(o, iFieldName);
        if (result != null) {
          if (OMultiValue.isMultiValue(result)) {
            // MULTI-VALUE: FLATTEN THE COLLECTION
            for (Object item : OMultiValue.getMultiValueIterable(result))
              tempResult.add(item);

          } else
            tempResult.add(result);
        }
      }
      return (RET) tempResult;
    } else
      return (RET) getSingleFieldValue(value, iFieldName);
  }

  @SuppressWarnings("unchecked")
  protected static <RET> RET getSingleFieldValue(Object value, final String iFieldName) {
    if (value == null)
      return null;

    final int fieldNameLength = iFieldName.length();
    if (fieldNameLength == 0)
      return (RET) value;

    OIdentifiable currentRecord = value instanceof OIdentifiable ? (OIdentifiable) value : null;

    int beginPos = iFieldName.charAt(0) == '.' ? 1 : 0;
    int nextSeparatorPos = iFieldName.charAt(0) == '.' ? 1 : 0;
    do {
      char nextSeparator = ' ';
      for (; nextSeparatorPos < fieldNameLength; ++nextSeparatorPos) {
        nextSeparator = iFieldName.charAt(nextSeparatorPos);
        if (nextSeparator == '.' || nextSeparator == '[')
          break;
      }

      final String fieldName;
      if (nextSeparatorPos < fieldNameLength)
        fieldName = iFieldName.substring(beginPos, nextSeparatorPos);
      else {
        nextSeparator = ' ';
        if (beginPos > 0)
          fieldName = iFieldName.substring(beginPos);
        else
          fieldName = iFieldName;
      }

      if (nextSeparator == '[') {
        if (fieldName != null && fieldName.length() > 0) {
          if (currentRecord != null)
            value = getIdentifiableValue(currentRecord, fieldName);
          else if (value instanceof Map<?, ?>)
            value = getMapEntry((Map<String, ?>) value, fieldName);
        }

        if (value == null)
          return null;
        else if (value instanceof OIdentifiable)
          currentRecord = (OIdentifiable) value;

        final int end = iFieldName.indexOf(']', nextSeparatorPos);
        if (end == -1)
          throw new IllegalArgumentException("Missed closed ']'");

        final String index = OStringSerializerHelper.getStringContent(iFieldName.substring(nextSeparatorPos + 1, end));
        nextSeparatorPos = end;

        if (value instanceof OIdentifiable) {
          final ORecord<?> record = currentRecord != null && currentRecord instanceof OIdentifiable ? ((OIdentifiable) currentRecord)
              .getRecord() : null;

          final List<String> indexParts = OStringSerializerHelper.smartSplit(index, ',');
          final List<String> indexCondition = OStringSerializerHelper.smartSplit(index, '=', ' ');
          if (indexParts.size() == 1 && indexCondition.size() == 1)
            // SINGLE VALUE
            value = ((ODocument) record).field(index);
          else if (indexParts.size() > 1) {
            // MULTI VALUE
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i) {
              values[i] = ((ODocument) record).field(indexParts.get(i));
            }
            value = values;
          } else if (!indexCondition.isEmpty()) {
            // CONDITION
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue = ORecordSerializerStringAbstract.getTypeValue(indexCondition.get(1));

            if (conditionFieldValue instanceof String)
              conditionFieldValue = OStringSerializerHelper.getStringContent(conditionFieldValue);

            final Object fieldValue = getFieldValue(currentRecord, conditionFieldName);
            if (fieldValue == null && !conditionFieldValue.equals("null") || fieldValue != null
                & !fieldValue.equals(conditionFieldValue))
              value = null;
          }
        } else if (value instanceof Map<?, ?>) {
          final List<String> indexParts = OStringSerializerHelper.smartSplit(index, ',');
          if (indexParts.size() == 1)
            // SINGLE VALUE
            value = ((Map<?, ?>) value).get(index);
          else {
            // MULTI VALUE
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i) {
              values[i] = ((Map<?, ?>) value).get(indexParts.get(i));
            }
            value = values;
          }
        } else if (value instanceof Collection<?> || value.getClass().isArray()) {
          // MULTI VALUE
          final List<String> indexParts = OStringSerializerHelper.smartSplit(index, ',');
          final List<String> indexRanges = OStringSerializerHelper.smartSplit(index, '-');
          final List<String> indexCondition = OStringSerializerHelper.smartSplit(index, '=', ' ');

          if (indexParts.size() == 1 && indexRanges.size() == 1 && indexCondition.size() == 1) {
            // SINGLE VALUE
            if (value instanceof Map<?, ?>)
              value = getMapEntry((Map<String, ?>) value, index);
            else
              value = OMultiValue.getValue(value, Integer.parseInt(index));

          } else if (indexParts.size() > 1) {

            // MULTI VALUES
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i)
              values[i] = OMultiValue.getValue(value, Integer.parseInt(indexParts.get(i)));
            value = values;

          } else if (indexRanges.size() > 1) {

            // MULTI VALUES RANGE
            final int rangeFrom = Integer.parseInt(indexRanges.get(0));
            final int rangeTo = Math.min(Integer.parseInt(indexRanges.get(1)), OMultiValue.getSize(value) - 1);

            final Object[] values = new Object[rangeTo - rangeFrom + 1];
            for (int i = rangeFrom; i <= rangeTo; ++i)
              values[i - rangeFrom] = OMultiValue.getValue(value, i);
            value = values;

          } else if (!indexCondition.isEmpty()) {
            // CONDITION
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue = ORecordSerializerStringAbstract.getTypeValue(indexCondition.get(1));

            if (conditionFieldValue instanceof String)
              conditionFieldValue = OStringSerializerHelper.getStringContent(conditionFieldValue);

            final List<Object> values = new ArrayList<Object>();
            for (Object v : OMultiValue.getMultiValueIterable(value)) {
              Object filtered = filterItem(conditionFieldName, conditionFieldValue, v);
              if (filtered != null)
                values.add(filtered);
            }

            if (values.isEmpty())
              // RETURNS NULL
              value = null;
            else if (values.size() == 1)
              // RETURNS THE SINGLE ODOCUMENT
              value = values.get(0);
            else
              // RETURNS THE FILTERED COLLECTION
              value = values;
          }
        }
      } else {
        if (fieldName.contains("("))
          value = evaluateFunction(value, fieldName);
        else {
          final List<String> indexCondition = OStringSerializerHelper.smartSplit(fieldName, '=', ' ');

          if (indexCondition.size() == 2) {
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue = ORecordSerializerStringAbstract.getTypeValue(indexCondition.get(1));

            if (conditionFieldValue instanceof String)
              conditionFieldValue = OStringSerializerHelper.getStringContent(conditionFieldValue);

            value = filterItem(conditionFieldName, conditionFieldValue, value);

          } else if (currentRecord != null) {
            // GET THE LINKED OBJECT IF ANY
            value = getIdentifiableValue(currentRecord, fieldName);
            if (value != null && value instanceof ORecord<?> && ((ORecord<?>) value).getInternalStatus() == STATUS.NOT_LOADED)
              // RELOAD IT
              ((ORecord<?>) value).reload();
          } else if (value instanceof Map<?, ?>)
            value = getMapEntry((Map<String, ?>) value, fieldName);

        }
      }

      if (value instanceof OIdentifiable)
        currentRecord = (OIdentifiable) value;

      beginPos = ++nextSeparatorPos;
    } while (nextSeparatorPos < fieldNameLength && value != null);

    return (RET) value;
  }

  @SuppressWarnings("unchecked")
  protected static Object filterItem(final String iConditionFieldName, final Object iConditionFieldValue, final Object iValue) {
    if (iValue instanceof ODocument) {
      final ODocument doc = (ODocument) iValue;
      Object fieldValue = doc.field(iConditionFieldName);

      fieldValue = OType.convert(fieldValue, iConditionFieldValue.getClass());
      if (fieldValue != null && fieldValue.equals(iConditionFieldValue))
        return doc;
    } else if (iValue instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iValue;
      Object fieldValue = getMapEntry(map, iConditionFieldName);

      fieldValue = OType.convert(fieldValue, iConditionFieldValue.getClass());
      if (fieldValue != null && fieldValue.equals(iConditionFieldValue))
        return map;
    }
    return null;
  }

  /**
   * Retrieves the value crossing the map with the dotted notation
   * 
   * @param iName
   *          Field(s) to retrieve. If are multiple fields, then the dot must be used as separator
   * @return
   */
  @SuppressWarnings("unchecked")
  public static Object getMapEntry(final Map<String, ?> iMap, final String iName) {
    if (iMap == null || iName == null)
      return null;

    String fieldName;
    int pos = iName.indexOf('.');
    if (pos > -1)
      fieldName = iName.substring(0, pos);
    else
      fieldName = iName;

    final Object value = iMap.get(fieldName);
    if (value == null)
      return null;

    if (pos > -1) {
      final String restFieldName = iName.substring(pos + 1);
      if (value instanceof ODocument)
        return getFieldValue(value, restFieldName);
      else if (value instanceof Map<?, ?>)
        return getMapEntry((Map<String, ?>) value, restFieldName);
    }

    return value;
  }

  public static Object getIdentifiableValue(final OIdentifiable iCurrent, final String iFieldName) {
    if (iFieldName == null || iFieldName.length() == 0)
      return null;

    final char begin = iFieldName.charAt(0);
    if (begin == '@') {
      // RETURN AN ATTRIBUTE
      if (iFieldName.equalsIgnoreCase(ATTRIBUTE_THIS))
        return iCurrent.getRecord();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID))
        return iCurrent.getIdentity();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_VERSION))
        return ((ODocument) iCurrent.getRecord()).getVersion();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_CLASS))
        return ((ODocument) iCurrent.getRecord()).getClassName();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_TYPE))
        return Orient.instance().getRecordFactoryManager()
            .getRecordTypeName(((ORecordInternal<?>) iCurrent.getRecord()).getRecordType());
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_SIZE)) {
        final byte[] stream = ((ORecordInternal<?>) iCurrent.getRecord()).toStream();
        return stream != null ? stream.length : 0;
      } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_FIELDS))
        return ((ODocument) iCurrent.getRecord()).fieldNames();
      else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RAW))
        return new String(((ORecordInternal<?>) iCurrent.getRecord()).toStream());
    }

    final ODocument doc = ((ODocument) iCurrent.getRecord());
    doc.checkForFields(iFieldName);
    return doc._fieldValues.get(iFieldName);
  }

  public static Object evaluateFunction(final Object currentValue, String iFunction) {
    if (currentValue == null)
      return null;

    Object result = null;

    iFunction = iFunction.toUpperCase();

    if (iFunction.startsWith("SIZE("))
      result = currentValue instanceof ORecord<?> ? 1 : OMultiValue.getSize(currentValue);
    else if (iFunction.startsWith("LENGTH("))
      result = currentValue.toString().length();
    else if (iFunction.startsWith("TOUPPERCASE("))
      result = currentValue.toString().toUpperCase();
    else if (iFunction.startsWith("TOLOWERCASE("))
      result = currentValue.toString().toLowerCase();
    else if (iFunction.startsWith("TRIM("))
      result = currentValue.toString().trim();
    else if (iFunction.startsWith("TOJSON("))
      result = currentValue instanceof ODocument ? ((ODocument) currentValue).toJSON() : null;
    else if (iFunction.startsWith("KEYS("))
      result = currentValue instanceof Map<?, ?> ? ((Map<?, ?>) currentValue).keySet() : null;
    else if (iFunction.startsWith("VALUES("))
      result = currentValue instanceof Map<?, ?> ? ((Map<?, ?>) currentValue).values() : null;
    else if (iFunction.startsWith("ASSTRING("))
      result = currentValue.toString();
    else if (iFunction.startsWith("ASINTEGER("))
      result = new Integer(currentValue.toString());
    else if (iFunction.startsWith("ASFLOAT("))
      result = new Float(currentValue.toString());
    else if (iFunction.startsWith("ASBOOLEAN(")) {
      if (currentValue instanceof String)
        result = new Boolean((String) currentValue);
      else if (currentValue instanceof Number) {
        final int bValue = ((Number) currentValue).intValue();
        if (bValue == 0)
          result = Boolean.FALSE;
        else if (bValue == 1)
          result = Boolean.TRUE;
      }
    } else if (iFunction.startsWith("ASDATE("))
      if (currentValue instanceof Long)
        result = new Date((Long) currentValue);
      else
        try {
          result = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateFormatInstance()
              .parse(currentValue.toString());
        } catch (ParseException e) {
        }
    else if (iFunction.startsWith("ASDATETIME("))
      if (currentValue instanceof Long)
        result = new Date((Long) currentValue);
      else
        try {
          result = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateTimeFormatInstance()
              .parse(currentValue.toString());
        } catch (ParseException e) {
        }
    else {
      // EXTRACT ARGUMENTS
      final List<String> args = OStringSerializerHelper.getParameters(iFunction.substring(iFunction.indexOf('(')));

      if (iFunction.startsWith("CHARAT("))
        result = currentValue.toString().charAt(Integer.parseInt(args.get(0)));
      else if (iFunction.startsWith("INDEXOF("))
        if (args.size() == 1)
          result = currentValue.toString().indexOf(OStringSerializerHelper.getStringContent(args.get(0)));
        else
          result = currentValue.toString().indexOf(OStringSerializerHelper.getStringContent(args.get(0)),
              Integer.parseInt(args.get(1)));
      else if (iFunction.startsWith("SUBSTRING("))
        if (args.size() == 1)
          result = currentValue.toString().substring(Integer.parseInt(args.get(0)));
        else
          result = currentValue.toString().substring(Integer.parseInt(args.get(0)), Integer.parseInt(args.get(1)));
      else if (iFunction.startsWith("APPEND("))
        result = currentValue.toString() + OStringSerializerHelper.getStringContent(args.get(0));
      else if (iFunction.startsWith("PREFIX("))
        result = OStringSerializerHelper.getStringContent(args.get(0)) + currentValue.toString();
      else if (iFunction.startsWith("FORMAT("))
        result = String.format(OStringSerializerHelper.getStringContent(args.get(0)), currentValue.toString());
      else if (iFunction.startsWith("LEFT(")) {
        final int len = Integer.parseInt(args.get(0));
        final String stringValue = currentValue.toString();
        result = stringValue.substring(0, len <= stringValue.length() ? len : stringValue.length());
      } else if (iFunction.startsWith("RIGHT(")) {
        final int offset = Integer.parseInt(args.get(0));
        final String stringValue = currentValue.toString();
        result = stringValue.substring(offset < stringValue.length() ? stringValue.length() - offset : 0);
      }
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  public static void copyFieldValue(final ODocument iCloned, final Entry<String, Object> iEntry) {
    final Object fieldValue = iEntry.getValue();

    if (fieldValue != null) {
      // LISTS
      if (fieldValue instanceof ORecordLazyList) {
        iCloned._fieldValues.put(iEntry.getKey(), ((ORecordLazyList) fieldValue).copy(iCloned));

      } else if (fieldValue instanceof ORecordTrackedList) {
        final ORecordTrackedList newList = new ORecordTrackedList(iCloned);
        newList.addAll((ORecordTrackedList) fieldValue);
        iCloned._fieldValues.put(iEntry.getKey(), newList);

      } else if (fieldValue instanceof OTrackedList<?>) {
        final OTrackedList<Object> newList = new OTrackedList<Object>(iCloned);
        newList.addAll((OTrackedList<Object>) fieldValue);
        iCloned._fieldValues.put(iEntry.getKey(), newList);

      } else if (fieldValue instanceof List<?>) {
        iCloned._fieldValues.put(iEntry.getKey(), new ArrayList<Object>((List<Object>) fieldValue));

        // SETS
      } else if (fieldValue instanceof OMVRBTreeRIDSet) {
        iCloned._fieldValues.put(iEntry.getKey(), ((OMVRBTreeRIDSet) fieldValue).copy(iCloned));

      } else if (fieldValue instanceof ORecordTrackedSet) {
        final ORecordTrackedSet newList = new ORecordTrackedSet(iCloned);
        newList.addAll((ORecordTrackedSet) fieldValue);
        iCloned._fieldValues.put(iEntry.getKey(), newList);

      } else if (fieldValue instanceof OTrackedSet<?>) {
        final OTrackedSet<Object> newList = new OTrackedSet<Object>(iCloned);
        newList.addAll((OTrackedSet<Object>) fieldValue);
        iCloned._fieldValues.put(iEntry.getKey(), newList);

      } else if (fieldValue instanceof Set<?>) {
        iCloned._fieldValues.put(iEntry.getKey(), new HashSet<Object>((Set<Object>) fieldValue));

        // MAPS
      } else if (fieldValue instanceof ORecordLazyMap) {
        final ORecordLazyMap newMap = new ORecordLazyMap(iCloned, ((ORecordLazyMap) fieldValue).getRecordType());
        newMap.putAll((ORecordLazyMap) fieldValue);
        iCloned._fieldValues.put(iEntry.getKey(), newMap);

      } else if (fieldValue instanceof OTrackedMap) {
        final OTrackedMap<Object> newMap = new OTrackedMap<Object>(iCloned);
        newMap.putAll((OTrackedMap<Object>) fieldValue);
        iCloned._fieldValues.put(iEntry.getKey(), newMap);

      } else if (fieldValue instanceof Map<?, ?>) {
        iCloned._fieldValues.put(iEntry.getKey(), new LinkedHashMap<String, Object>((Map<String, Object>) fieldValue));
      } else
        iCloned._fieldValues.put(iEntry.getKey(), fieldValue);
    } else if (iCloned.getSchemaClass() != null) {
      final OProperty prop = iCloned.getSchemaClass().getProperty(iEntry.getKey());
      if (prop != null && prop.isMandatory())
        iCloned._fieldValues.put(iEntry.getKey(), fieldValue);
    }
  }

  public static boolean hasSameContentItem(final Object iCurrent, ODatabaseRecord iMyDb, final Object iOther,
      final ODatabaseRecord iOtherDb) {
    if (iCurrent instanceof ODocument) {
      final ODocument current = (ODocument) iCurrent;
      if (iOther instanceof ORID) {
        if (!current.isDirty()) {
          if (!current.getIdentity().equals(iOther))
            return false;
        } else {
          final ODocument otherDoc = iOtherDb.load((ORID) iOther);
          if (!ODocumentHelper.hasSameContentOf(current, iMyDb, otherDoc, iOtherDb))
            return false;
        }
      } else if (!ODocumentHelper.hasSameContentOf(current, iMyDb, (ODocument) iOther, iOtherDb))
        return false;
    } else if (!compareScalarValues(iCurrent, iOther))
      return false;
    return true;
  }

  /**
   * Makes a deep comparison field by field to check if the passed ODocument instance is identical in the content to the current
   * one. Instead equals() just checks if the RID are the same.
   * 
   * @param iOther
   *          ODocument instance
   * @return true if the two document are identical, otherwise false
   * @see #equals(Object);
   */
  @SuppressWarnings("unchecked")
  public static boolean hasSameContentOf(final ODocument iCurrent, final ODatabaseRecord iMyDb, final ODocument iOther,
      final ODatabaseRecord iOtherDb) {
    if (iOther == null)
      return false;

    if (!iCurrent.equals(iOther) && iCurrent.getIdentity().isValid())
      return false;

    makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
      public Object call() {
        if (iCurrent.getInternalStatus() == STATUS.NOT_LOADED)
          iCurrent.reload();
        return null;
      }
    });

    makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
      public Object call() {
        if (iOther.getInternalStatus() == STATUS.NOT_LOADED)
          iOther.reload();
        return null;
      }
    });

    makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
      public Object call() {
        iCurrent.checkForFields();
        return null;
      }
    });

    makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
      public Object call() {
        iOther.checkForFields();
        return null;
      }
    });

    if (iCurrent._fieldValues.size() != iOther._fieldValues.size())
      return false;

    // CHECK FIELD-BY-FIELD
    Object myFieldValue;
    Object otherFieldValue;
    for (Entry<String, Object> f : iCurrent._fieldValues.entrySet()) {
      myFieldValue = f.getValue();
      otherFieldValue = iOther._fieldValues.get(f.getKey());

      // CHECK FOR NULLS
      if (myFieldValue == null) {
        if (otherFieldValue != null)
          return false;
      } else if (otherFieldValue == null)
        return false;

      if (myFieldValue != null)
        if (myFieldValue instanceof Set && otherFieldValue instanceof Set) {
          if (!compareSets(iMyDb, (Set<?>) myFieldValue, iOtherDb, (Set<?>) otherFieldValue))
            return false;
        } else if (myFieldValue instanceof Collection && otherFieldValue instanceof Collection) {
          if (!compareCollections(iMyDb, (Collection<?>) myFieldValue, iOtherDb, (Collection<?>) otherFieldValue))
            return false;
        } else if (myFieldValue instanceof Map && otherFieldValue instanceof Map) {
          if (!compareMaps(iMyDb, (Map<Object, Object>) myFieldValue, iOtherDb, (Map<Object, Object>) otherFieldValue))
            return false;
        } else if (myFieldValue instanceof ODocument && otherFieldValue instanceof ODocument) {
          return hasSameContentOf((ODocument) myFieldValue, iMyDb, (ODocument) otherFieldValue, iOtherDb);
        } else {
          if (!compareScalarValues(myFieldValue, otherFieldValue))
            return false;
        }
    }

    return true;
  }

  public static boolean compareMaps(ODatabaseRecord iMyDb, Map<Object, Object> myFieldValue, ODatabaseRecord iOtherDb,
      Map<Object, Object> otherFieldValue) {
    // CHECK IF THE ORDER IS RESPECTED
    final Map<Object, Object> myMap = myFieldValue;
    final Map<Object, Object> otherMap = otherFieldValue;

    if (myMap.size() != otherMap.size())
      return false;

    boolean oldMyAutoConvert = false;
    boolean oldOtherAutoConvert = false;

    if (myMap instanceof ORecordLazyMultiValue) {
      oldMyAutoConvert = ((ORecordLazyMultiValue) myMap).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) myMap).setAutoConvertToRecord(false);
    }

    if (otherMap instanceof ORecordLazyMultiValue) {
      oldOtherAutoConvert = ((ORecordLazyMultiValue) otherMap).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) otherMap).setAutoConvertToRecord(false);
    }

    try {
      final Iterator<Entry<Object, Object>> myEntryIterator = makeDbCall(iMyDb,
          new ODbRelatedCall<Iterator<Entry<Object, Object>>>() {
            public Iterator<Entry<Object, Object>> call() {
              return myMap.entrySet().iterator();
            }
          });

      while (makeDbCall(iMyDb, new ODbRelatedCall<Boolean>() {
        public Boolean call() {
          return myEntryIterator.hasNext();
        }
      })) {
        final Entry<Object, Object> myEntry = makeDbCall(iMyDb, new ODbRelatedCall<Entry<Object, Object>>() {
          public Entry<Object, Object> call() {
            return myEntryIterator.next();
          }
        });
        final Object myKey = makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
          public Object call() {
            return myEntry.getKey();
          }
        });

        if (makeDbCall(iOtherDb, new ODbRelatedCall<Boolean>() {
          public Boolean call() {
            return !otherMap.containsKey(myKey);
          }
        }))
          return false;

        if (myEntry.getValue() instanceof ODocument) {
          if (!hasSameContentOf(makeDbCall(iMyDb, new ODbRelatedCall<ODocument>() {
            public ODocument call() {
              return (ODocument) myEntry.getValue();
            }
          }), iMyDb, makeDbCall(iOtherDb, new ODbRelatedCall<ODocument>() {
            public ODocument call() {
              return (ODocument) otherMap.get(myEntry.getKey());
            }
          }), iOtherDb))
            return false;
        } else {
          final Object myValue = makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
            public Object call() {
              return myEntry.getValue();
            }
          });

          final Object otherValue = makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
            public Object call() {
              return otherMap.get(myEntry.getKey());
            }
          });

          if (!compareScalarValues(myValue, otherValue))
            return false;
        }
      }
      return true;
    } finally {
      if (myMap instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) myMap).setAutoConvertToRecord(oldMyAutoConvert);

      if (otherMap instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) otherMap).setAutoConvertToRecord(oldOtherAutoConvert);
    }
  }

  public static boolean compareCollections(ODatabaseRecord iMyDb, Collection<?> myFieldValue, ODatabaseRecord iOtherDb,
      Collection<?> otherFieldValue) {
    final Collection<?> myCollection = myFieldValue;
    final Collection<?> otherCollection = otherFieldValue;

    if (myCollection.size() != otherCollection.size())
      return false;

    boolean oldMyAutoConvert = false;
    boolean oldOtherAutoConvert = false;

    if (myCollection instanceof ORecordLazyMultiValue) {
      oldMyAutoConvert = ((ORecordLazyMultiValue) myCollection).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) myCollection).setAutoConvertToRecord(false);
    }

    if (otherCollection instanceof ORecordLazyMultiValue) {
      oldOtherAutoConvert = ((ORecordLazyMultiValue) otherCollection).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) otherCollection).setAutoConvertToRecord(false);
    }

    try {
      final Iterator<?> myIterator = makeDbCall(iMyDb, new ODbRelatedCall<Iterator<?>>() {
        public Iterator<?> call() {
          return myCollection.iterator();
        }
      });

      final Iterator<?> otherIterator = makeDbCall(iOtherDb, new ODbRelatedCall<Iterator<?>>() {
        public Iterator<?> call() {
          return otherCollection.iterator();
        }
      });

      while (makeDbCall(iMyDb, new ODbRelatedCall<Boolean>() {
        public Boolean call() {
          return myIterator.hasNext();
        }
      })) {
        final Object myNextVal = makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
          public Object call() {
            return myIterator.next();
          }
        });

        final Object otherNextVal = makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
          public Object call() {
            return otherIterator.next();
          }
        });

        if (!hasSameContentItem(myNextVal, iMyDb, otherNextVal, iOtherDb))
          return false;
      }
      return true;
    } finally {
      if (myCollection instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) myCollection).setAutoConvertToRecord(oldMyAutoConvert);

      if (otherCollection instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) otherCollection).setAutoConvertToRecord(oldOtherAutoConvert);
    }
  }

  public static boolean compareSets(ODatabaseRecord iMyDb, Set<?> myFieldValue, ODatabaseRecord iOtherDb, Set<?> otherFieldValue) {
    final Set<?> mySet = myFieldValue;
    final Set<?> otherSet = otherFieldValue;

    final int mySize = makeDbCall(iMyDb, new ODbRelatedCall<Integer>() {
      public Integer call() {
        return mySet.size();
      }
    });

    final int otherSize = makeDbCall(iOtherDb, new ODbRelatedCall<Integer>() {
      public Integer call() {
        return otherSet.size();
      }
    });

    if (mySize != otherSize)
      return false;

    boolean oldMyAutoConvert = false;
    boolean oldOtherAutoConvert = false;

    if (mySet instanceof ORecordLazyMultiValue) {
      oldMyAutoConvert = ((ORecordLazyMultiValue) mySet).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) mySet).setAutoConvertToRecord(false);
    }

    if (otherSet instanceof ORecordLazyMultiValue) {
      oldOtherAutoConvert = ((ORecordLazyMultiValue) otherSet).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) otherSet).setAutoConvertToRecord(false);
    }

    try {
      final Iterator<?> myIterator = makeDbCall(iMyDb, new ODbRelatedCall<Iterator<?>>() {
        public Iterator<?> call() {
          return mySet.iterator();
        }
      });

      while (makeDbCall(iMyDb, new ODbRelatedCall<Boolean>() {
        public Boolean call() {
          return myIterator.hasNext();
        }
      })) {

        final Iterator<?> otherIterator = makeDbCall(iOtherDb, new ODbRelatedCall<Iterator<?>>() {
          public Iterator<?> call() {
            return otherSet.iterator();
          }
        });

        final Object myNextVal = makeDbCall(iMyDb, new ODbRelatedCall<Object>() {
          public Object call() {
            return myIterator.next();
          }
        });

        boolean found = false;
        while (!found && makeDbCall(iOtherDb, new ODbRelatedCall<Boolean>() {
          public Boolean call() {
            return otherIterator.hasNext();
          }
        })) {
          final Object otherNextVal = makeDbCall(iOtherDb, new ODbRelatedCall<Object>() {
            public Object call() {
              return otherIterator.next();
            }
          });

          found = hasSameContentItem(myNextVal, iMyDb, otherNextVal, iOtherDb);
        }

        if (!found)
          return false;
      }
      return true;
    } finally {
      if (mySet instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) mySet).setAutoConvertToRecord(oldMyAutoConvert);

      if (otherSet instanceof ORecordLazyMultiValue)
        ((ORecordLazyMultiValue) otherSet).setAutoConvertToRecord(oldOtherAutoConvert);
    }
  }

  private static boolean compareScalarValues(Object myValue, Object otherValue) {
    if (myValue == null && otherValue != null || myValue != null && otherValue == null)
      return false;

    if (myValue == null)
      return true;

    if (myValue.getClass().isArray() && !otherValue.getClass().isArray() || !myValue.getClass().isArray()
        && otherValue.getClass().isArray())
      return false;

    if (myValue.getClass().isArray() && otherValue.getClass().isArray()) {
      final int myArraySize = Array.getLength(myValue);
      final int otherArraySize = Array.getLength(otherValue);

      if (myArraySize != otherArraySize)
        return false;

      for (int i = 0; i < myArraySize; i++)
        if (!Array.get(myValue, i).equals(Array.get(otherValue, i)))
          return false;

      return true;
    }

    if (myValue instanceof Number && otherValue instanceof Number) {
      final Number myNumberValue = (Number) myValue;
      final Number otherNumberValue = (Number) otherValue;

      if (isInteger(myNumberValue) && isInteger(otherNumberValue))
        return myNumberValue.longValue() == otherNumberValue.longValue();
      else if (isFloat(myNumberValue) && isFloat(otherNumberValue))
        return myNumberValue.doubleValue() == otherNumberValue.doubleValue();
    }

    return myValue.equals(otherValue);
  }

  private static boolean isInteger(Number value) {
    return value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;

  }

  private static boolean isFloat(Number value) {
    return value instanceof Float || value instanceof Double;
  }

  public static void deleteCrossRefs(final ORID iRid, final ODocument iContent) {
    for (String fieldName : iContent.fieldNames()) {
      final Object fieldValue = iContent.field(fieldName);
      if (fieldValue != null) {
        if (fieldValue.equals(iRid)) {
          // REMOVE THE LINK
          iContent.field(fieldName, (ORID) null);
          iContent.save();
        } else if (fieldValue instanceof ODocument && ((ODocument) fieldValue).isEmbedded()) {
          // EMBEDDED DOCUMENT: GO RECURSIVELY
          deleteCrossRefs(iRid, (ODocument) fieldValue);
        } else if (OMultiValue.isMultiValue(fieldValue)) {
          // MULTI-VALUE (COLLECTION, ARRAY OR MAP), CHECK THE CONTENT
          for (final Iterator<?> it = OMultiValue.getMultiValueIterator(fieldValue); it.hasNext();) {
            final Object item = it.next();

            if (fieldValue.equals(iRid)) {
              // DELETE ITEM
              it.remove();
            } else if (item instanceof ODocument && ((ODocument) item).isEmbedded()) {
              // EMBEDDED DOCUMENT: GO RECURSIVELY
              deleteCrossRefs(iRid, (ODocument) item);
            }
          }
        }
      }
    }
  }

  public static <T> T makeDbCall(final ODatabaseRecord databaseRecord, final ODbRelatedCall<T> function) {
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseRecord);
    return function.call();
  }

  public static interface ODbRelatedCall<T> {
    public T call();
  }
}
