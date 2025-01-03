package com.jpmc.databricks;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.credential.*;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

public class OauthCredentialPropertiesProvider extends DefaultCredentialPropertiesProvider {
    public static final String OAUTH_TOKEN_NAME = "oauth-token";
    private final CredentialProvider delegate;

    public OauthCredentialPropertiesProvider(CredentialProvider delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    @Override
    public Map<String, Object> getCredentialProperties(ConnectorIdentity identity)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.putAll(super.getCredentialProperties(identity));
        getConnectionOauth(Optional.of(identity)).ifPresent(oauthtoken -> properties.put("oauth-token", oauthtoken));
        return properties.buildOrThrow();
    }

    private Optional<String> getConnectionOauth(Optional<ConnectorIdentity> jdbcIdentity) {
        if (jdbcIdentity.isPresent()) {
            Map<String, String> extraCredentials = jdbcIdentity.get().getExtraCredentials();
            if (extraCredentials.containsKey(OAUTH_TOKEN_NAME)) {
                return Optional.of(extraCredentials.get(OAUTH_TOKEN_NAME));
            }
        }
        return Optional.empty();
    }
}
