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
package com.orientechnologies.orient.server.network.protocol.http.command.delete;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandDeleteProperty extends OServerCommandAuthenticatedDbAbstract {
	private static final String[]	NAMES	= { "DELETE|property/*" };

	@Override
	public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
		String[] urlParts = checkSyntax(iRequest.url, 4, "Syntax error: property/<database>/<class-name>/<property-name>");

		iRequest.data.commandInfo = "Delete property";
		iRequest.data.commandDetail = urlParts[2] + "." + urlParts[3];

		ODatabaseDocumentTx db = null;

		try {
			db = getProfiledDatabaseInstance(iRequest);

			if (db.getMetadata().getSchema().getClass(urlParts[2]) == null)
				throw new IllegalArgumentException("Invalid class '" + urlParts[2] + "'");

			final OClass cls = db.getMetadata().getSchema().getClass(urlParts[2]);

			cls.dropProperty(urlParts[3]);

			iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);

		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
		}
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
