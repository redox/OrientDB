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
package com.orientechnologies.orient.server.network.protocol.http;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.util.Date;
import java.util.HashMap;
import java.util.IllegalFormatException;
import java.util.InputMismatchException;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.enterprise.channel.text.OChannelTextServer;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.multipart.OHttpMultipartBaseInputStream;

public abstract class ONetworkProtocolHttpAbstract extends ONetworkProtocol {
  private static final String               COMMAND_SEPARATOR = "|";
  private static int                        requestMaxContentLength;                                  // MAX = 10Kb
  private static int                        socketTimeout;

  protected OClientConnection               connection;
  protected OChannelTextServer              channel;
  protected OUser                           account;
  protected OHttpRequest                    request;
  protected OHttpResponse                   response;

  private final StringBuilder               requestContent    = new StringBuilder();
  private final Map<String, OServerCommand> exactCommands     = new HashMap<String, OServerCommand>();
  private final Map<String, OServerCommand> wildcardCommands  = new HashMap<String, OServerCommand>();
  private String                            responseCharSet;
  private String[]                          additionalResponseHeaders;
  private String                            listeningAddress  = "?";

  public ONetworkProtocolHttpAbstract() {
    super(Orient.getThreadGroup(), "IO-HTTP");
  }

  @Override
  public void config(final OServer iServer, final Socket iSocket, final OContextConfiguration iConfiguration) throws IOException {
    final String addHeaders = iConfiguration.getValueAsString("network.http.additionalResponseHeaders", null);
    if (addHeaders != null)
      additionalResponseHeaders = addHeaders.split(";");

    // CREATE THE CLIENT CONNECTION
    connection = OClientConnectionManager.instance().connect(iSocket, this);

    server = iServer;
    requestMaxContentLength = iConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_HTTP_MAX_CONTENT_LENGTH);
    socketTimeout = iConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT);
    responseCharSet = iConfiguration.getValueAsString(OGlobalConfiguration.NETWORK_HTTP_CONTENT_CHARSET);

    channel = new OChannelTextServer(iSocket, iConfiguration);
    channel.connected();

    request = new OHttpRequest(this, channel.inStream, connection.data, iConfiguration);

    connection.data.caller = channel.toString();

    listeningAddress = getListeningAddress();

    start();
  }

  public void service() throws ONetworkProtocolException, IOException {
    Orient.instance().getProfiler().updateCounter("server.http." + listeningAddress + ".requests", +1);

    ++connection.data.totalRequests;
    connection.data.commandInfo = null;
    connection.data.commandDetail = null;

    final String callbackF;
    if ((request.parameters != null) && request.parameters.containsKey(OHttpUtils.CALLBACK_PARAMETER_NAME))
      callbackF = request.parameters.get(OHttpUtils.CALLBACK_PARAMETER_NAME);
    else
      callbackF = null;

    response = new OHttpResponse(channel.outStream, request.httpVersion, additionalResponseHeaders, responseCharSet,
        connection.data.serverInfo, request.sessionId, callbackF);

    final long begin = System.currentTimeMillis();

    boolean isChain;
    do {
      isChain = false;
      final String command;
      if (request.url.length() < 2) {
        command = "";
      } else {
        command = request.url.substring(1);
      }

      final String commandString = getCommandString(command);

      // TRY TO FIND EXACT MATCH
      OServerCommand cmd = exactCommands.get(commandString);

      if (cmd == null) {
        // TRY WITH WILDCARD COMMANDS
        // TODO: OPTIMIZE SEARCH!
        String partLeft, partRight;
        for (Entry<String, OServerCommand> entry : wildcardCommands.entrySet()) {
          final int wildcardPos = entry.getKey().indexOf('*');
          partLeft = entry.getKey().substring(0, wildcardPos);
          partRight = entry.getKey().substring(wildcardPos + 1);

          if (commandString.startsWith(partLeft) && commandString.endsWith(partRight)) {
            cmd = entry.getValue();
            break;
          }
        }
      }

      if (cmd != null)
        try {
          if (cmd.beforeExecute(request, response))
            // EXECUTE THE COMMAND
            isChain = cmd.execute(request, response);

        } catch (Exception e) {
          handleError(e);
        }
      else {
        try {
          OLogManager.instance().warn(
              this,
              "->" + channel.socket.getInetAddress().getHostAddress() + ": Command not found: " + request.httpMethod + "."
                  + command);

          sendTextContent(OHttpUtils.STATUS_INVALIDMETHOD_CODE, OHttpUtils.STATUS_INVALIDMETHOD_DESCRIPTION, null,
              OHttpUtils.CONTENT_TEXT_PLAIN, "Command not found: " + command);
        } catch (IOException e1) {
          sendShutdown();
        }
      }
    } while (isChain);

    connection.data.lastCommandInfo = connection.data.commandInfo;
    connection.data.lastCommandDetail = connection.data.commandDetail;

    connection.data.lastCommandExecutionTime = System.currentTimeMillis() - begin;
    connection.data.totalCommandExecutionTime += connection.data.lastCommandExecutionTime;
  }

  protected void handleError(Throwable e) {
    if (OLogManager.instance().isDebugEnabled())
      OLogManager.instance().debug(this, "Caught exception", e);

    int errorCode = 500;
    String errorReason = null;
    String errorMessage = null;
    String responseHeaders = null;

    if (e instanceof IllegalFormatException || e instanceof InputMismatchException) {
      errorCode = OHttpUtils.STATUS_BADREQ_CODE;
      errorReason = OHttpUtils.STATUS_BADREQ_DESCRIPTION;
    } else if (e instanceof ORecordNotFoundException) {
      errorCode = OHttpUtils.STATUS_NOTFOUND_CODE;
      errorReason = OHttpUtils.STATUS_NOTFOUND_DESCRIPTION;
    } else if (e instanceof OConcurrentModificationException) {
      errorCode = OHttpUtils.STATUS_CONFLICT_CODE;
      errorReason = OHttpUtils.STATUS_CONFLICT_DESCRIPTION;
    } else if (e instanceof OLockException) {
      errorCode = 423;
    } else if (e instanceof UnsupportedOperationException) {
      errorCode = OHttpUtils.STATUS_NOTIMPL_CODE;
      errorReason = OHttpUtils.STATUS_NOTIMPL_DESCRIPTION;
    } else if (e instanceof IllegalArgumentException)
      errorCode = OHttpUtils.STATUS_INTERNALERROR_CODE;

    if (e instanceof ODatabaseException || e instanceof OSecurityAccessException || e instanceof OCommandExecutionException
        || e instanceof OLockException) {
      // GENERIC DATABASE EXCEPTION
      Throwable cause;
      do {
        cause = e instanceof OSecurityAccessException ? e : e.getCause();
        if (cause instanceof OSecurityAccessException) {
          // SECURITY EXCEPTION
          if (account == null) {
            // UNAUTHORIZED
            errorCode = OHttpUtils.STATUS_AUTH_CODE;
            errorReason = OHttpUtils.STATUS_AUTH_DESCRIPTION;
            responseHeaders = "WWW-Authenticate: Basic realm=\"OrientDB db-" + ((OSecurityAccessException) cause).getDatabaseName()
                + "\"";
            errorMessage = null;
          } else {
            // USER ACCESS DENIED
            errorCode = 530;
            errorReason = "Current user has not the privileges to execute the request.";
            errorMessage = "530 User access denied";
          }
          break;
        }

        if (cause != null)
          e = cause;
      } while (cause != null);
    }

    if (errorMessage == null) {
      // FORMAT GENERIC MESSAGE BY READING THE EXCEPTION STACK
      final StringBuilder buffer = new StringBuilder();
      buffer.append(e);
      Throwable cause = e.getCause();
      while (cause != null && cause != cause.getCause()) {
        buffer.append("\r\n--> ");
        buffer.append(cause);
        cause = cause.getCause();
      }
      errorMessage = buffer.toString();
    }

    if (errorReason == null) {
      errorReason = OHttpUtils.STATUS_INTERNALERROR_DESCRIPTION;
      OLogManager.instance().error(this, "Internal server error:\n%s", errorMessage);
    }

    try {
      sendTextContent(errorCode, errorReason, responseHeaders, OHttpUtils.CONTENT_TEXT_PLAIN, errorMessage);
    } catch (IOException e1) {
      sendShutdown();
    }
  }

  /**
   * Register all the names for the same instance
   * 
   * @param iServerCommandInstance
   */
  @Override
  public void registerCommand(final Object iServerCommandInstance) {
    OServerCommand cmd = (OServerCommand) iServerCommandInstance;

    for (String name : cmd.getNames())
      if (OStringSerializerHelper.contains(name, '*'))
        wildcardCommands.put(name, cmd);
      else
        exactCommands.put(name, cmd);
  }

  protected void sendTextContent(final int iCode, final String iReason, String iHeaders, final String iContentType,
      final String iContent) throws IOException {
    final boolean empty = iContent == null || iContent.length() == 0;

    sendStatus(empty && iCode == 200 ? 204 : iCode, iReason);
    sendResponseHeaders(iContentType);
    if (iHeaders != null)
      writeLine(iHeaders);

    writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (empty ? 0 : iContent.length()));

    writeLine(null);

    if (!empty)
      writeLine(iContent);

    channel.flush();
  }

  protected void writeLine(final String iContent) throws IOException {
    if (iContent != null)
      channel.outStream.write(iContent.getBytes());
    channel.outStream.write(OHttpUtils.EOL);
  }

  protected void sendStatus(final int iStatus, final String iReason) throws IOException {
    writeLine(request.httpVersion + " " + iStatus + " " + iReason);
  }

  protected void sendResponseHeaders(final String iContentType) throws IOException {
    writeLine("Cache-Control: no-cache, no-store, max-age=0, must-revalidate");
    writeLine("Pragma: no-cache");
    writeLine("Date: " + new Date());
    writeLine("Content-Type: " + iContentType + "; charset=" + responseCharSet);
    writeLine("Server: " + connection.data.serverInfo);
    writeLine("Connection: Keep-Alive");
    if (getAdditionalResponseHeaders() != null)
      for (String h : getAdditionalResponseHeaders())
        writeLine(h);
  }

  protected void readAllContent(final OHttpRequest iRequest) throws IOException {
    iRequest.content = null;

    int in;
    char currChar;
    int contentLength = -1;
    boolean endOfHeaders = false;

    final StringBuilder request = new StringBuilder();

    while (!channel.socket.isInputShutdown()) {
      in = channel.read();
      if (in == -1)
        break;

      currChar = (char) in;

      if (currChar == '\r') {
        if (request.length() > 0 && !endOfHeaders) {
          final String line = request.toString();
          if (OStringSerializerHelper.startsWithIgnoreCase(line, OHttpUtils.HEADER_AUTHORIZATION)) {
            // STORE AUTHORIZATION INFORMATION INTO THE REQUEST
            final String auth = line.substring(OHttpUtils.HEADER_AUTHORIZATION.length());
            if (!OStringSerializerHelper.startsWithIgnoreCase(auth, OHttpUtils.AUTHORIZATION_BASIC))
              throw new IllegalArgumentException("Only HTTP Basic authorization is supported");

            iRequest.authorization = auth.substring(OHttpUtils.AUTHORIZATION_BASIC.length() + 1);

            iRequest.authorization = new String(OBase64Utils.decode(iRequest.authorization));

          } else if (OStringSerializerHelper.startsWithIgnoreCase(line, OHttpUtils.HEADER_COOKIE)) {
            final String sessionPair = line.substring(OHttpUtils.HEADER_COOKIE.length());

            final String[] sessionItems = sessionPair.split(";");
            for (String sessionItem : sessionItems) {
              final String[] sessionPairItems = sessionItem.split("=");
              if (sessionPairItems.length == 2 && OHttpUtils.OSESSIONID.equals(sessionPairItems[0])) {
                iRequest.sessionId = sessionPairItems[1];
                break;
              }
            }

          } else if (OStringSerializerHelper.startsWithIgnoreCase(line, OHttpUtils.HEADER_CONTENT_LENGTH)) {
            contentLength = Integer.parseInt(line.substring(OHttpUtils.HEADER_CONTENT_LENGTH.length()));
            if (contentLength > requestMaxContentLength)
              OLogManager.instance().warn(
                  this,
                  "->" + channel.socket.getInetAddress().getHostAddress() + ": Error on content size " + contentLength
                      + ": the maximum allowed is " + requestMaxContentLength);

          } else if (OStringSerializerHelper.startsWithIgnoreCase(line, OHttpUtils.HEADER_CONTENT_TYPE)) {
            iRequest.contentType = line.substring(OHttpUtils.HEADER_CONTENT_TYPE.length());
            if (OStringSerializerHelper.startsWithIgnoreCase(iRequest.contentType, OHttpUtils.CONTENT_TYPE_MULTIPART)) {
              iRequest.isMultipart = true;
              iRequest.boundary = new String(line.substring(OHttpUtils.HEADER_CONTENT_TYPE.length()
                  + OHttpUtils.CONTENT_TYPE_MULTIPART.length() + 2 + OHttpUtils.BOUNDARY.length() + 1));
            }
          } else if (OStringSerializerHelper.startsWithIgnoreCase(line, OHttpUtils.HEADER_IF_MATCH))
            iRequest.ifMatch = line.substring(OHttpUtils.HEADER_IF_MATCH.length());

          else if (OStringSerializerHelper.startsWithIgnoreCase(line, OHttpUtils.HEADER_X_FORWARDED_FOR))
            connection.data.caller = line.substring(OHttpUtils.HEADER_X_FORWARDED_FOR.length());

        }

        // CONSUME /r or /n
        in = channel.read();
        if (in == -1)
          break;

        currChar = (char) in;

        if (!endOfHeaders && request.length() == 0) {
          if (contentLength <= 0)
            return;

          // FIRST BLANK LINE: END OF HEADERS
          endOfHeaders = true;
        }

        request.setLength(0);
      } else if (endOfHeaders && request.length() == 0 && currChar != '\r' && currChar != '\n') {
        // END OF HEADERS
        if (iRequest.isMultipart) {
          iRequest.content = "";
          iRequest.multipartStream = new OHttpMultipartBaseInputStream(channel.inStream, currChar, contentLength);
          return;
        } else {
          byte[] buffer = new byte[contentLength];
          buffer[0] = (byte) currChar;

          channel.read(buffer, 1, contentLength - 1);

          iRequest.content = new String(buffer);
          return;
        }
      } else
        request.append(currChar);
    }

    if (OLogManager.instance().isDebugEnabled())
      OLogManager.instance().debug(this, "Error on parsing HTTP content from client %s:\n%s",
          channel.socket.getInetAddress().getHostAddress(), request);

    return;
  }

  @Override
  protected void execute() throws Exception {
    if (channel.socket.isInputShutdown()) {
      connectionClosed();
      return;
    }

    connection.data.commandInfo = "Listening";
    connection.data.commandDetail = null;

    try {
      channel.socket.setSoTimeout(socketTimeout);
      connection.data.lastCommandReceived = -1;

      char c = (char) channel.read();

      if (channel.inStream.available() == 0) {
        connectionClosed();
        return;
      }

      channel.socket.setSoTimeout(socketTimeout);
      connection.data.lastCommandReceived = Orient.instance().getProfiler().startChrono();

      requestContent.setLength(0);
      request.isMultipart = false;

      if (c != '\n')
        // AVOID INITIAL /N
        requestContent.append(c);

      while (!channel.socket.isInputShutdown()) {
        c = (char) channel.read();

        if (c == '\r') {
          final String[] words = requestContent.toString().split(" ");
          if (words.length < 3) {
            OLogManager.instance().warn(this,
                "->" + channel.socket.getInetAddress().getHostAddress() + ": Error on invalid content:\n" + requestContent);
            while (channel.inStream.available() > 0) {
              channel.read();
            }
            break;
          }

          // CONSUME THE NEXT \n
          channel.read();

          request.httpMethod = words[0];
          request.url = words[1].trim();

          final int parametersPos = request.url.indexOf('?');
          if (parametersPos > -1) {
            request.parameters = OHttpUtils.getParameters(request.url.substring(parametersPos));
            request.url = request.url.substring(0, parametersPos);
          }

          request.httpVersion = words[2];
          readAllContent(request);

          if (request.content != null && request.contentType.equals(OHttpUtils.CONTENT_TYPE_URLENCODED))
            request.content = URLDecoder.decode(request.content, "UTF-8").trim();

          if (OLogManager.instance().isDebugEnabled())
            OLogManager.instance().debug(this, "[ONetworkProtocolHttpAbstract.execute] Requested: %s %s", request.httpMethod,
                request.url);

          service();
          return;
        }
        requestContent.append(c);
      }

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this,
            "Parsing request from client " + channel.socket.getInetAddress().getHostAddress() + ":\n" + requestContent);

    } catch (SocketException e) {
      connectionError();

    } catch (SocketTimeoutException e) {
      timeout();

    } catch (Throwable t) {
      if (request.httpMethod != null && request.url != null) {
        try {
          sendTextContent(505, "Error on executing of " + request.httpMethod + " for the resource: " + request.url, null,
              "text/plain", t.toString());
        } catch (IOException e) {
        }
      } else
        sendTextContent(505, "Error on executing request", null, "text/plain", t.toString());

      readAllContent(request);
    } finally {
      if (connection.data.lastCommandReceived > -1)
        Orient.instance().getProfiler()
            .stopChrono("server.http." + listeningAddress + ".request", connection.data.lastCommandReceived);
    }
  }

  protected void connectionClosed() {
    Orient.instance().getProfiler().updateCounter("server.http." + listeningAddress + ".closed", +1);
    sendShutdown();
  }

  protected void timeout() {
    Orient.instance().getProfiler().updateCounter("server.http." + listeningAddress + ".timeout", +1);
    sendShutdown();
  }

  protected void connectionError() {
    Orient.instance().getProfiler().updateCounter("server.http." + listeningAddress + ".error", +1);
    sendShutdown();
  }

  @Override
  public void sendShutdown() {
    super.sendShutdown();

    try {
      // FORCE SOCKET CLOSING
      channel.socket.close();
    } catch (final Exception e) {
    }
  }

  @Override
  public void shutdown() {
    try {
      sendShutdown();
      channel.close();

    } finally {
      OClientConnectionManager.instance().disconnect(connection.id);

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Connection shutdowned");
    }
  }

  @Override
  public OChannel getChannel() {
    return channel;
  }

  public OUser getAccount() {
    return account;
  }

  private String getCommandString(final String command) {
    final int getQueryPosition = command.indexOf('?');

    final StringBuilder commandString = new StringBuilder();
    commandString.append(request.httpMethod);
    commandString.append(COMMAND_SEPARATOR);

    if (getQueryPosition > -1)
      commandString.append(command.substring(0, getQueryPosition));
    else
      commandString.append(command);
    return commandString.toString();
  }

  public String getResponseCharSet() {
    return responseCharSet;
  }

  public void setResponseCharSet(String responseCharSet) {
    this.responseCharSet = responseCharSet;
  }

  public String[] getAdditionalResponseHeaders() {
    return additionalResponseHeaders;
  }
}
