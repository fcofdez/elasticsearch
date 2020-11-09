/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.azure;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.AffixSetting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

final class AzureStorageSettings {

    // prefix for azure client settings
    private static final String AZURE_CLIENT_PREFIX_KEY = "azure.client.";

    /** Azure account name */
    public static final AffixSetting<SecureString> ACCOUNT_SETTING =
        Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "account", key -> SecureSetting.secureString(key, null));

    /** Azure key */
    public static final AffixSetting<SecureString> KEY_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "key",
        key -> SecureSetting.secureString(key, null));

    /** Azure SAS token */
    public static final AffixSetting<SecureString> SAS_TOKEN_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "sas_token",
        key -> SecureSetting.secureString(key, null));

    /** max_retries: Number of retries in case of Azure errors. Defaults to 3 (RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT). */
    public static final AffixSetting<Integer> MAX_RETRIES_SETTING =
        Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "max_retries",
            (key) -> Setting.intSetting(key, 4, Setting.Property.NodeScope),
            () -> ACCOUNT_SETTING, () -> KEY_SETTING);
    /**
     * Azure endpoint suffix. Default to core.windows.net (CloudStorageAccount.DEFAULT_DNS).
     */
    public static final AffixSetting<String> ENDPOINT_SUFFIX_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "endpoint_suffix",
        key -> Setting.simpleString(key, Property.NodeScope), () -> ACCOUNT_SETTING, () -> KEY_SETTING);

    public static final AffixSetting<TimeValue> TIMEOUT_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueMinutes(-1), Property.NodeScope), () -> ACCOUNT_SETTING, () -> KEY_SETTING);

    /** The type of the proxy to connect to azure through. Can be direct (no proxy, default), http or socks */
    public static final AffixSetting<Proxy.Type> PROXY_TYPE_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "proxy.type",
        (key) -> new Setting<>(key, "direct", s -> Proxy.Type.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope)
        , () -> ACCOUNT_SETTING, () -> KEY_SETTING);

    /** The host name of a proxy to connect to azure through. */
    public static final AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "proxy.host",
        (key) -> Setting.simpleString(key, Property.NodeScope), () -> KEY_SETTING, () -> ACCOUNT_SETTING, () -> PROXY_TYPE_SETTING);

    /** The port of a proxy to connect to azure through. */
    public static final Setting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.port",
        (key) -> Setting.intSetting(key, 0, 0, 65535, Setting.Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING,
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING);

    private final String account;
    private final String connectString;
    private final String endpointSuffix;
    private final TimeValue timeout;
    private final int maxRetries;
    private final Proxy proxy;
    private final LocationMode locationMode;

    // copy-constructor
    private AzureStorageSettings(String account, String connectString, String endpointSuffix, TimeValue timeout, int maxRetries,
                                 Proxy proxy, LocationMode locationMode) {
        this.account = account;
        this.connectString = connectString;
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        this.proxy = proxy;
        this.locationMode = locationMode;
    }

    private AzureStorageSettings(String account, String key, String sasToken, String endpointSuffix, TimeValue timeout, int maxRetries,
                                 Proxy.Type proxyType, String proxyHost, Integer proxyPort) {
        this.account = account;
        this.connectString = buildConnectString(account, key, sasToken, endpointSuffix);
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        // Register the proxy if we have any
        // Validate proxy settings
        if (proxyType.equals(Proxy.Type.DIRECT) && ((proxyPort != 0) || Strings.hasText(proxyHost))) {
            throw new SettingsException("Azure Proxy port or host have been set but proxy type is not defined.");
        }
        if ((proxyType.equals(Proxy.Type.DIRECT) == false) && ((proxyPort == 0) || Strings.isEmpty(proxyHost))) {
            throw new SettingsException("Azure Proxy type has been set but proxy host or port is not defined.");
        }

        if (proxyType.equals(Proxy.Type.DIRECT)) {
            proxy = null;
        } else {
            try {
                proxy = new Proxy(proxyType, new InetSocketAddress(InetAddress.getByName(proxyHost), proxyPort));
            } catch (final UnknownHostException e) {
                throw new SettingsException("Azure proxy host is unknown.", e);
            }
        }

        this.locationMode = LocationMode.PRIMARY_ONLY;
    }

    public String getEndpointSuffix() {
        return endpointSuffix;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Proxy getProxy() {
        return proxy;
    }

    public String getConnectString() {
        return connectString;
    }

    private static String buildConnectString(String account, @Nullable String key, @Nullable String sasToken, String endpointSuffix) {
        final boolean hasSasToken = Strings.hasText(sasToken);
        final boolean hasKey = Strings.hasText(key);
        if (hasSasToken == false && hasKey == false) {
            throw new SettingsException("Neither a secret key nor a shared access token was set.");
        }
        if (hasSasToken && hasKey) {
            throw new SettingsException("Both a secret as well as a shared access token were set.");
        }
        final StringBuilder connectionStringBuilder = new StringBuilder();
        connectionStringBuilder.append("DefaultEndpointsProtocol=https").append(";AccountName=").append(account);
        if (hasKey) {
            connectionStringBuilder.append(";AccountKey=").append(key);
        } else {
            connectionStringBuilder.append(";SharedAccessSignature=").append(sasToken);
        }
        if (Strings.hasText(endpointSuffix)) {
            connectionStringBuilder.append(";EndpointSuffix=").append(endpointSuffix);
        }
        return connectionStringBuilder.toString();
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AzureStorageSettings{");
        sb.append("account='").append(account).append('\'');
        sb.append(", timeout=").append(timeout);
        sb.append(", endpointSuffix='").append(endpointSuffix).append('\'');
        sb.append(", maxRetries=").append(maxRetries);
        sb.append(", proxy=").append(proxy);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Parse and read all settings available under the azure.client.* namespace
     * @param settings settings to parse
     * @return All the named configurations
     */
    public static Map<String, AzureStorageSettings> load(Settings settings) {
        // Get the list of existing named configurations
        final Map<String, AzureStorageSettings> storageSettings = new HashMap<>();
        for (final String clientName : ACCOUNT_SETTING.getNamespaces(settings)) {
            storageSettings.put(clientName, getClientSettings(settings, clientName));
        }
        if (false == storageSettings.containsKey("default") && false == storageSettings.isEmpty()) {
            // in case no setting named "default" has been set, let's define our "default"
            // as the first named config we get
            final AzureStorageSettings defaultSettings = storageSettings.values().iterator().next();
            storageSettings.put("default", defaultSettings);
        }
        assert storageSettings.containsKey("default") || storageSettings.isEmpty() : "always have 'default' if any";
        return Collections.unmodifiableMap(storageSettings);
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    private static AzureStorageSettings getClientSettings(Settings settings, String clientName) {
        try (SecureString account = getConfigValue(settings, clientName, ACCOUNT_SETTING);
             SecureString key = getConfigValue(settings, clientName, KEY_SETTING);
             SecureString sasToken = getConfigValue(settings, clientName, SAS_TOKEN_SETTING)) {
            return new AzureStorageSettings(account.toString(), key.toString(), sasToken.toString(),
                getValue(settings, clientName, ENDPOINT_SUFFIX_SETTING),
                getValue(settings, clientName, TIMEOUT_SETTING),
                getValue(settings, clientName, MAX_RETRIES_SETTING),
                getValue(settings, clientName, PROXY_TYPE_SETTING),
                getValue(settings, clientName, PROXY_HOST_SETTING),
                getValue(settings, clientName, PROXY_PORT_SETTING));
        }
    }

    private static <T> T getConfigValue(Settings settings, String clientName,
                                        Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }

    private static <T> T getValue(Settings settings, String groupName, Setting<T> setting) {
        final Setting.AffixKey k = (Setting.AffixKey) setting.getRawKey();
        final String fullKey = k.toConcreteKey(groupName).toString();
        return setting.getConcreteSetting(fullKey).get(settings);
    }

    static Map<String, AzureStorageSettings> overrideLocationMode(Map<String, AzureStorageSettings> clientsSettings,
                                                                  LocationMode locationMode) {
        final var map = new HashMap<String, AzureStorageSettings>();
        for (final Map.Entry<String, AzureStorageSettings> entry : clientsSettings.entrySet()) {
            map.put(entry.getKey(),
                new AzureStorageSettings(entry.getValue().account, entry.getValue().connectString, entry.getValue().endpointSuffix,
                    entry.getValue().timeout, entry.getValue().maxRetries, entry.getValue().proxy, locationMode));
        }
        return Map.copyOf(map);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureStorageSettings that = (AzureStorageSettings) o;
        return maxRetries == that.maxRetries &&
            Objects.equals(account, that.account) &&
            Objects.equals(connectString, that.connectString) &&
            Objects.equals(endpointSuffix, that.endpointSuffix) &&
            Objects.equals(timeout, that.timeout) &&
            Objects.equals(proxy, that.proxy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(account, connectString, endpointSuffix, timeout, maxRetries, proxy);
    }
}
