# Kafka Implementation API Reference

## Stats API Methods

### BaseProducerStats
- `recordSend(long bytes, long latencyMs)` - Record successful send
- `recordSendError()` - Record send error
- `incrementPendingMessages()` - Increment pending count
- `decrementPendingMessages()` - Decrement pending count

### BaseConsumerStats
- `recordReceive(long bytes)` - Record received message
- `recordAcknowledgment(long processingLatencyMs)` - Record acknowledgment
- `recordNegativeAcknowledgment()` - Record negative ack
- `recordReceiveError()` - Record receive error

## Config API Methods

### ProducerConfig<K, V>
- `String getTopic()` - Get topic name
- `Optional<String> getProducerName()` - Get producer name
- `SchemaDefinition getKeySchema()` - Get key schema
- `SchemaDefinition getValueSchema()` - Get value schema
- `Optional<BatchConfig> getBatchConfig()` - Get batch config
- `Optional<RoutingConfig> getRoutingConfig()` - Get routing config
- `int getMaxPendingMessages()` - Get max pending
- `long getSendTimeoutMs()` - Get send timeout
- `boolean isBlockIfQueueFull()` - Check block on full
- `Optional<CompressionType> getCompressionType()` - Get compression
- `Map<String, Object> getProviderProperties()` - Get provider props

### ConsumerConfig<K, V>
- `String getTopic()` - Get topic (singular)
- `String getSubscriptionName()` - Get subscription name
- `SubscriptionType getSubscriptionType()` - Get subscription type
- `Optional<String> getConsumerName()` - Get consumer name
- `SchemaDefinition getKeySchema()` - Get key schema
- `SchemaDefinition getValueSchema()` - Get value schema
- `InitialPosition getInitialPosition()` - Get initial position
- `int getReceiverQueueSize()` - Get queue size
- `long getAckTimeoutMs()` - Get ack timeout
- `boolean isAutoAcknowledge()` - Check auto-ack
- `Map<String, Object> getProviderProperties()` - Get provider props

### ClientConfig
- `MessagingProvider getProvider()` - Get provider type
- `String getServiceUrl()` - Get service URL
- `Optional<AuthConfig> getAuthConfig()` - Get auth config
- `Optional<SslConfig> getSslConfig()` - Get SSL config
- `Map<String, Object> getProviderProperties()` - Get provider props
- `long getMemoryLimitBytes()` - Get memory limit
- `long getOperationTimeoutMs()` - Get operation timeout
- `long getConnectionTimeoutMs()` - Get connection timeout

### AuthConfig
- `String getPluginClassName()` - Get plugin class
- `String getAuthParams()` - Get auth params (String, not Map)
- `Map<String, String> getProperties()` - Get additional properties

### SslConfig
- `boolean isEnabled()` - Check if enabled
- `Optional<String> getTrustStorePath()` - Get truststore path
- `Optional<String> getTrustStorePassword()` - Get truststore password
- `Optional<String> getTrustStoreType()` - Get truststore type
- `Optional<String> getKeyStorePath()` - Get keystore path
- `Optional<String> getKeyStorePassword()` - Get keystore password
- `Optional<String> getKeyStoreType()` - Get keystore type
- `Optional<String> getTrustedCertificates()` - Get trusted certs
- `Optional<String> getClientCertificate()` - Get client cert
- `Optional<String> getClientKey()` - Get client key
- `boolean isHostnameVerificationEnabled()` - Check hostname verification
- `Optional<Set<String>> getCipherSuites()` - Get cipher suites
- `Optional<Set<String>> getProtocols()` - Get protocols

## MessagingClientProvider SPI
- `MessagingProvider getProvider()` - Return enum (not String)
- `MessagingClient createClient(ClientConfig config)` - Create client
- `boolean supports(MessagingProvider provider)` - Check support
- `String getProviderType()` - Get type as string (default impl)

## AbstractMessagingClient
- Has `protected ClientConfig config` field
- Subclasses can access via `this.config`