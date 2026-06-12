/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.cdc.messaging.config;

import java.util.Map;

/**
 * Authentication configuration for messaging client.
 * Supports various authentication mechanisms.
 */
public interface AuthConfig {
    
    /**
     * Get authentication plugin class name.
     * Provider-specific authentication implementation.
     * 
     * @return Plugin class name
     */
    String getPluginClassName();
    
    /**
     * Get authentication parameters.
     * Format depends on authentication mechanism:
     * <ul>
     *   <li>Token: "token:xxxxx"</li>
     *   <li>OAuth: JSON with client credentials</li>
     *   <li>Username/Password: "username:password"</li>
     * </ul>
     * 
     * @return Authentication parameters
     */
    String getAuthParams();
    
    /**
     * Get additional authentication properties.
     * 
     * @return Immutable map of properties
     */
    Map<String, String> getProperties();
}

