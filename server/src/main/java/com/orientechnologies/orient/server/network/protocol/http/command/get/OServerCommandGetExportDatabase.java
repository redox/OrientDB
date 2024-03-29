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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import java.io.IOException;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetExportDatabase extends OServerCommandAuthenticatedDbAbstract implements OCommandOutputListener {
  private static final String[] NAMES = { "GET|export/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception {
    String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: export/<database>/[<name>][?params*]");

    if (urlParts.length > 2) {
    } else {
      exportStandard(iRequest, iResponse);
    }
    return false;
  }

  protected void exportStandard(final OHttpRequest iRequest, final OHttpResponse iResponse) throws InterruptedException,
      IOException {
    iRequest.data.commandInfo = "Database export";
    ODatabaseRecord database = getProfiledDatabaseInstance(iRequest);
    iResponse.writeStatus(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION);
    iResponse.writeHeaders(OHttpUtils.CONTENT_GZIP);
    iResponse.writeLine("Content-Disposition: attachment; filename=" + database.getName() + ".gz");
    iResponse.writeLine("Date: " + new Date());
    iResponse.writeLine(null);
    ODatabaseExport export = new ODatabaseExport(database, new GZIPOutputStream(iResponse.getOutputStream()), this);
    export.exportDatabase();
    iResponse.flush();
  }

  @Override
  public void onMessage(String iText) {
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
