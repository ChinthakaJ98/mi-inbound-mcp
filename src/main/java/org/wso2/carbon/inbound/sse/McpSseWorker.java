/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.inbound.sse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.synapse.transport.passthru.Pipe;
import org.apache.synapse.transport.passthru.SourceContext;
import org.apache.synapse.transport.passthru.SourceRequest;
import org.apache.synapse.transport.passthru.SourceResponse;
import org.apache.synapse.transport.passthru.config.SourceConfiguration;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
/**
 * Manages a persistent Server-Sent Events (SSE) connection.
 */
public class McpSseWorker implements Runnable {

    private static final Log log = LogFactory.getLog(McpSseWorker.class);

    private static final byte[] KEEPALIVE_BYTES =
            McpConstants.SSE_KEEPALIVE_COMMENT.getBytes(StandardCharsets.UTF_8);
    private static final int MAX_EVENT_QUEUE_SIZE = 1000;

    private final SourceRequest request;
    private final SourceConfiguration sourceConfiguration;
    private final CorsConfig corsConfig;
    private final long sseKeepaliveIntervalMs;
    private final String sessionId;
    private final BlockingQueue<String> eventQueue = new LinkedBlockingQueue<>(MAX_EVENT_QUEUE_SIZE);

    /** Constructs an SSE worker for a GET request to /mcp. */
    public McpSseWorker(SourceRequest request, SourceConfiguration sourceConfiguration, CorsConfig corsConfig, long sseKeepaliveIntervalMs) {
        this.request = request;
        this.sourceConfiguration = sourceConfiguration;
        this.corsConfig = corsConfig;
        this.sseKeepaliveIntervalMs = sseKeepaliveIntervalMs;
        this.sessionId = UUID.randomUUID().toString();
    }

    /** Returns the unique session ID for this SSE connection. */
    public String getSessionId() {
        return sessionId;
    }

    /** Queues an SSE event without an explicit event name. */
    public void sendEvent(String eventData) {
        enqueueEvent("data: " + eventData + "\n\n");
    }

    /** Queues an SSE event with an explicit event name and data. */
    public void sendEvent(String eventName, String eventData) {
        enqueueEvent("event: " + eventName + "\ndata: " + eventData + "\n\n");
    }

    /** Enqueues an event to the bounded queue. Drops oldest if full. */
    private void enqueueEvent(String event) {
        if (!eventQueue.offer(event)) {
            log.warn("SSE event queue full for session " + sessionId + "; dropping oldest event to make room");
            eventQueue.poll();
            eventQueue.offer(event);
        }
    }

    /** Runs the SSE worker: sends events and keep-alive comments. */
    @Override
    public void run() {
        SourceResponse sourceResponse = new SourceResponse(sourceConfiguration, 200, request);
        sourceResponse.addHeader(McpConstants.HEADER_CONTENT_TYPE, McpConstants.CONTENT_TYPE_SSE);
        sourceResponse.addHeader(McpConstants.HEADER_CACHE_CONTROL, "no-cache");
        sourceResponse.addHeader(McpConstants.HEADER_CONNECTION, "keep-alive");
        sourceResponse.addHeader(McpConstants.HEADER_CORS_ALLOW_ORIGIN, corsConfig.getAllowOrigin());
        sourceResponse.addHeader(McpConstants.HEADER_CORS_EXPOSE_HEADERS, corsConfig.getExposeHeaders());
        
        if (log.isDebugEnabled()) {
            log.debug("SSE response CORS headers set - Allow-Origin: " + corsConfig.getAllowOrigin() 
                    + ", Expose-Headers: " + corsConfig.getExposeHeaders());
        }

        // Echo back the session ID the client opened this stream with, if present
        String mcpSessionId = getHeader(McpConstants.HEADER_MCP_SESSION_ID);
        if (mcpSessionId != null) {
            sourceResponse.addHeader(McpConstants.HEADER_MCP_SESSION_ID, mcpSessionId);
        }

        Pipe pipe = new Pipe(sourceConfiguration.getBufferFactory().getBuffer(),
                "MCP-SSE-" + sessionId, sourceConfiguration);
        pipe.attachConsumer(request.getConnection());
        sourceResponse.connect(pipe);

        SourceContext.setResponse(request.getConnection(), sourceResponse);
        request.getConnection().requestOutput();

        McpSseSessionRegistry.getInstance().register(sessionId, this);
        log.info("MCP SSE session opened: " + sessionId);
        
        if (log.isDebugEnabled()) {
            log.debug("SSE session " + sessionId + " using keepalive interval: "
                    + sseKeepaliveIntervalMs + "ms");
        }

        try (OutputStream out = pipe.getOutputStream()) {
            String host = getHeader("Host");
            String scheme = getScheme();
            String endpointUrl = (host != null)
                    ? scheme + "://" + host + McpConstants.PATH_MCP + "?sessionId=" + sessionId
                    : McpConstants.PATH_MCP + "?sessionId=" + sessionId;
            writeRaw(out, "event: endpoint\ndata: " + endpointUrl + "\n\n");

            while (true) {
                String event = eventQueue.poll(sseKeepaliveIntervalMs,
                        TimeUnit.MILLISECONDS);
                if (event != null) {
                    writeRaw(out, event);
                } else {
                    // No event within the keepalive window — send a comment to stay alive
                    out.write(KEEPALIVE_BYTES);
                    out.flush();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("MCP SSE worker interrupted for session: " + sessionId);
        } catch (IOException e) {
            // Client disconnected — normal SSE termination
            log.info("MCP SSE session closed (client disconnected): " + sessionId);
        } finally {
            McpSseSessionRegistry.getInstance().unregister(sessionId);
            pipe.setSerializationComplete(true);
        }
    }

    /** Retrieves an HTTP header value from the request. */
    private String getHeader(String headerName) {
        if (request.getRequest() == null) {
            return null;
        }
        Header h = request.getRequest().getFirstHeader(headerName);
        return h != null ? h.getValue() : null;
    }

    /** Determines the scheme (http or https) for the endpoint URL. */
    private String getScheme() {
        // Check X-Forwarded-Proto header (set by TLS-terminating proxies like nginx, AWS ALB)
        String forwardedProto = getHeader("X-Forwarded-Proto");
        if (forwardedProto != null && !forwardedProto.trim().isEmpty()) {
            String proto = forwardedProto.trim().toLowerCase();
            // Accept https, http, or https with port (https:443)
            if (proto.startsWith("https") || proto.equals("http")) {
                return proto.startsWith("https") ? "https" : "http";
            }
        }

        // Default to HTTP (most MI deployments use reverse proxies that set X-Forwarded-Proto)
        return "http";
    }

    /** Writes a UTF-8 string to the output stream and flushes it. */
    private void writeRaw(OutputStream out, String text) throws IOException {
        out.write(text.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
