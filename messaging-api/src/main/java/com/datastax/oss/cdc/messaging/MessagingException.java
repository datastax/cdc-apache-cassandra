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
package com.datastax.oss.cdc.messaging;

/**
 * Base exception for all messaging operations.
 * Wraps platform-specific exceptions into a common abstraction.
 */
public class MessagingException extends Exception {
    
    /**
     * Create exception with message.
     * @param message Error message
     */
    public MessagingException(String message) {
        super(message);
    }
    
    /**
     * Create exception with message and cause.
     * @param message Error message
     * @param cause Root cause
     */
    public MessagingException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Create exception with cause.
     * @param cause Root cause
     */
    public MessagingException(Throwable cause) {
        super(cause);
    }
}

