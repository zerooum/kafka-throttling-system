package com.throttling.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.HashMap;
import java.util.Map;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

public class WiremockTestResource implements QuarkusTestResourceLifecycleManager {

    private WireMockServer server;

    @Override
    public Map<String, String> start() {
        server = new WireMockServer(options().dynamicPort());
        server.start();
        Map<String, String> cfg = new HashMap<>();
        cfg.put("quarkus.rest-client.legacy-api.url", "http://localhost:" + server.port());
        cfg.put("wiremock.port", String.valueOf(server.port()));
        return cfg;
    }

    @Override
    public void inject(TestInjector testInjector) {
        testInjector.injectIntoFields(server,
            new TestInjector.MatchesType(WireMockServer.class));
    }

    @Override
    public void stop() {
        if (server != null) server.stop();
    }
}
