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
package com.orientechnologies.orient.test.database.auto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * If some of the tests start to fail then check cluster number
 * in queries, e.g #7:1. It can be because the order of clusters
 * could be affected due to adding or removing cluster from storage.
 */
@Test(groups = "sql-update", sequential = true)
public class SQLUpdateTest {
  private ODatabaseDocument database;
  private int               updatedRecords;

  @Parameters(value = "url")
  public SQLUpdateTest(String iURL) {
    database = new ODatabaseDocumentTx(iURL);
  }

  @Test
  public void updateWithWhereOperator() {
    database.open("admin", "admin");

    List<Long> positions = getValidPositions(4);

    Integer records = (Integer) database.command(
        new OCommandSQL("update Profile set salary = 120.30, location = 4:" + positions.get(2)
            + ", salary_cloned = salary where surname = 'Obama'")).execute();

    Assert.assertEquals(records.intValue(), 3);

    database.close();
  }

  @Test
  public void updateWithWhereRid() {
    database.open("admin", "admin");

    List<ODocument> result = database.command(new OCommandSQL("select @rid as rid from Profile where surname = 'Obama'")).execute();

    Assert.assertEquals(result.size(), 3);

    Integer records = (Integer) database.command(new OCommandSQL("update Profile set salary = 133.00 where @rid = ?")).execute(
        result.get(0).field("rid"));

    Assert.assertEquals(records.intValue(), 1);

    database.close();
  }

  @Test(dependsOnMethods = "updateWithWhereOperator")
  public void updateCollectionsAddWithWhereOperator() {
    database.open("admin", "admin");

    updatedRecords = (Integer) database.command(new OCommandSQL("update Account add addresses = #13:0")).execute();

    database.close();
  }

  @Test(dependsOnMethods = "updateCollectionsAddWithWhereOperator")
  public void updateCollectionsRemoveWithWhereOperator() {
    database.open("admin", "admin");

    final int records = (Integer) database.command(new OCommandSQL("update Account remove addresses = #13:0")).execute();

    Assert.assertEquals(records, updatedRecords);

    database.close();
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateCollectionsWithSetOperator() {
    database.open("admin", "admin");

    List<ODocument> docs = database.query(new OSQLSynchQuery<ODocument>("select from Account"));

    List<Long> positions = getValidPositions(13);

    for (ODocument doc : docs) {

      final int records = (Integer) database.command(
          new OCommandSQL("update Account set addresses = [#13:" + positions.get(0) + ", #13:" + positions.get(1) + ",#13:"
              + positions.get(2) + "] where @rid = " + doc.getIdentity())).execute();

      Assert.assertEquals(records, 1);

      ODocument loadedDoc = database.load(doc.getIdentity(), "*:-1", true);
      Assert.assertEquals(((List<?>) loadedDoc.field("addresses")).size(), 3);
      Assert.assertEquals(((OIdentifiable) ((List<?>) loadedDoc.field("addresses")).get(0)).getIdentity().toString(), "#13:"
          + positions.get(0));
      loadedDoc.field("addresses", doc.field("addresses"));
      database.save(loadedDoc);
    }

    database.close();
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateMapsWithSetOperator() {
    database.open("admin", "admin");

    ODocument doc = (ODocument) database
        .command(
            new OCommandSQL(
                "insert into cluster:default (equaledges, name, properties) values ('no', 'circleUpdate', {'round':'eeee', 'blaaa':'zigzag'} )"))
        .execute();

    Integer records = (Integer) database.command(
        new OCommandSQL("update " + doc.getIdentity()
            + " set properties = {'roundOne':'ffff', 'bla':'zagzig','testTestTEST':'okOkOK'}")).execute();

    Assert.assertEquals(records.intValue(), 1);

    ODocument loadedDoc = database.load(doc.getIdentity(), "*:-1", true);

    Assert.assertTrue(loadedDoc.field("properties") instanceof Map);

    @SuppressWarnings("unchecked")
    Map<Object, Object> entries = ((Map<Object, Object>) loadedDoc.field("properties"));
    Assert.assertEquals(entries.size(), 3);

    Assert.assertNull(entries.get("round"));
    Assert.assertNull(entries.get("blaaa"));

    Assert.assertEquals(entries.get("roundOne"), "ffff");
    Assert.assertEquals(entries.get("bla"), "zagzig");
    Assert.assertEquals(entries.get("testTestTEST"), "okOkOK");

    database.close();
  }

    @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
    public void updateMapsWithPutOperatorAndWhere() {
      database.open("admin", "admin");

      ODocument doc = (ODocument) database
          .command(
              new OCommandSQL(
                  "insert into cluster:default (equaledges, name, properties) values ('no', 'updateMapsWithPutOperatorAndWhere', {} )"))
          .execute();

      Integer records = (Integer) database.command(
          new OCommandSQL("update " + doc.getIdentity()
              + " put properties = 'one', 'two' where name = 'updateMapsWithPutOperatorAndWhere'")).execute();

      Assert.assertEquals(records.intValue(), 1);

      ODocument loadedDoc = database.load(doc.getIdentity(), "*:-1", true);

      Assert.assertTrue(loadedDoc.field("properties") instanceof Map);

      @SuppressWarnings("unchecked")
      Map<Object, Object> entries = ((Map<Object, Object>) loadedDoc.field("properties"));
      Assert.assertEquals(entries.size(), 1);

      Assert.assertNull(entries.get("round"));
      Assert.assertNull(entries.get("blaaa"));

      Assert.assertEquals(entries.get("one"), "two");

      database.close();
    }


  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateAllOperator() {
    database.open("admin", "admin");

    Long total = database.countClass("Profile");

    Integer records = (Integer) database.command(new OCommandSQL("update Profile set sex = 'male'")).execute();

    Assert.assertEquals(records.intValue(), total.intValue());

    database.close();
  }

  @Test
  public void updateWithWildcards() {
    database.open("admin", "admin");

    int updated = (Integer) database.command(new OCommandSQL("update Profile set sex = ? where sex = 'male' limit 1")).execute(
        "male");

    Assert.assertEquals(updated, 1);

    database.close();
  }

  @Test
  public void updateWithWildcardsOnSetAndWhere() {

    database.open("admin", "admin");
    ODocument doc = new ODocument("Person");
    doc.field("name", "Raf");
    doc.field("city", "Torino");
    doc.field("gender", "fmale");
    doc.save();
    checkUpdatedDoc(database, "Raf", "Torino", "fmale");

    /* THESE COMMANDS ARE OK */
    OCommandSQL updatecommand = new OCommandSQL("update Person set gender = 'female' where name = 'Raf'");
    database.command(updatecommand).execute("Raf");
    checkUpdatedDoc(database, "Raf", "Torino", "female");

    updatecommand = new OCommandSQL("update Person set city = 'Turin' where name = ?");
    database.command(updatecommand).execute("Raf");
    checkUpdatedDoc(database, "Raf", "Turin", "female");

    updatecommand = new OCommandSQL("update Person set gender = ? where name = 'Raf'");
    database.command(updatecommand).execute("F");
    checkUpdatedDoc(database, "Raf", "Turin", "F");

    updatecommand = new OCommandSQL("update Person set gender = ?, city = ? where name = 'Raf'");
    database.command(updatecommand).execute("FEMALE", "TORINO");
    checkUpdatedDoc(database, "Raf", "TORINO", "FEMALE");

    updatecommand = new OCommandSQL("update Person set gender = ? where name = ?");
    database.command(updatecommand).execute("f", "Raf");
    checkUpdatedDoc(database, "Raf", "TORINO", "f");

    database.close();
  }

  public void updateIncrement() {
    database.open("admin", "admin");

    List<ODocument> result1 = database.command(new OCommandSQL("select salary from Account where salary is defined")).execute();
    Assert.assertFalse(result1.isEmpty());

    updatedRecords = (Integer) database.command(new OCommandSQL("update Account increment salary = 10 where salary is defined"))
        .execute();
    Assert.assertTrue(updatedRecords > 0);

    List<ODocument> result2 = database.command(new OCommandSQL("select salary from Account where salary is defined")).execute();
    Assert.assertFalse(result2.isEmpty());
    Assert.assertEquals(result2.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = (Float) result1.get(i).field("salary");
      float salary2 = (Float) result2.get(i).field("salary");
      Assert.assertEquals(salary2, salary1 + 10);
    }

    updatedRecords = (Integer) database.command(new OCommandSQL("update Account increment salary = -10 where salary is defined"))
        .execute();
    Assert.assertTrue(updatedRecords > 0);

    List<ODocument> result3 = database.command(new OCommandSQL("select salary from Account where salary is defined")).execute();
    Assert.assertFalse(result3.isEmpty());
    Assert.assertEquals(result3.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = (Float) result1.get(i).field("salary");
      float salary3 = (Float) result3.get(i).field("salary");
      Assert.assertEquals(salary3, salary1);
    }
    database.close();
  }

  public void updateSetMultipleFields() {
    database.open("admin", "admin");

    List<ODocument> result1 = database.command(new OCommandSQL("select salary from Account where salary is defined")).execute();
    Assert.assertFalse(result1.isEmpty());

    updatedRecords = (Integer) database.command(
        new OCommandSQL("update Account set salary2 = salary, checkpoint = true where salary is defined")).execute();
    Assert.assertTrue(updatedRecords > 0);

    List<ODocument> result2 = database.command(new OCommandSQL("select from Account where salary is defined")).execute();
    Assert.assertFalse(result2.isEmpty());
    Assert.assertEquals(result2.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = (Float) result1.get(i).field("salary");
      float salary2 = (Float) result2.get(i).field("salary2");
      Assert.assertEquals(salary2, salary1);
      Assert.assertEquals(result2.get(i).field("checkpoint"), true);
    }

    database.close();
  }

  public void updateAddMultipleFields() {
    database.open("admin", "admin");

    updatedRecords = (Integer) database.command(new OCommandSQL("update Account add myCollection = 1, myCollection = 2 limit 1"))
        .execute();
    Assert.assertTrue(updatedRecords > 0);

    List<ODocument> result2 = database.command(new OCommandSQL("select from Account where myCollection is defined")).execute();
    Assert.assertEquals(result2.size(), 1);

    Collection<Object> myCollection = result2.iterator().next().field("myCollection");

    Assert.assertTrue(myCollection.containsAll(Arrays.asList(new Integer[] { 1, 2 })));

    database.close();
  }

  private void checkUpdatedDoc(ODatabaseDocument database, String expectedName, String expectedCity, String expectedGender) {
    List<ODocument> result = database.query(new OSQLSynchQuery<Object>("select * from person"));
    ODocument oDoc = result.get(0);
    Assert.assertEquals(expectedName, oDoc.field("name"));
    Assert.assertEquals(expectedCity, oDoc.field("city"));
    Assert.assertEquals(expectedGender, oDoc.field("gender"));
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<Long>();

    final ORecordIteratorCluster<ODocument> iteratorCluster = database.browseCluster(database.getClusterNameById(clusterId));

    for (int i = 0; i < 7; i++) {
      if (!iteratorCluster.hasNext())
        break;
      ODocument doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }
}
