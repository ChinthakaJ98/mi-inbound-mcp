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

/**
 * Configuration holder for CORS headers.
 */
public class CorsConfig {

    private final String allowOrigin;
    private final String allowMethods;
    private final String allowHeaders;
    private final String exposeHeaders;

    /** Constructs CORS configuration. */
    public CorsConfig(String allowOrigin, String allowMethods, String allowHeaders, String exposeHeaders) {
        this.allowOrigin = allowOrigin;
        this.allowMethods = allowMethods;
        this.allowHeaders = allowHeaders;
        this.exposeHeaders = exposeHeaders;
    }

    /** Returns the Access-Control-Allow-Origin header value. */
    public String getAllowOrigin() {
        return allowOrigin;
    }

    /** Returns the Access-Control-Allow-Methods header value. */
    public String getAllowMethods() {
        return allowMethods;
    }

    /** Returns the Access-Control-Allow-Headers header value. */
    public String getAllowHeaders() {
        return allowHeaders;
    }

    /** Returns the Access-Control-Expose-Headers header value. */
    public String getExposeHeaders() {
        return exposeHeaders;
    }
}
