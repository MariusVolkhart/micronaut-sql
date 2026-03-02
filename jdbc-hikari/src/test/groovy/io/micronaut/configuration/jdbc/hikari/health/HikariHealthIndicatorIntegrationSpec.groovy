/*
 * Copyright 2026 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.jdbc.hikari.health

import io.micronaut.configuration.jdbc.hikari.DatasourceConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.context.exceptions.NoSuchBeanException
import io.micronaut.health.HealthStatus
import io.micronaut.management.endpoint.health.HealthEndpoint
import io.micronaut.management.health.indicator.HealthResult
import reactor.core.publisher.Flux
import spock.lang.Specification

import java.time.Duration

/**
 * Integration tests for {@link HikariHealthIndicator}.
 *
 * Uses a real H2 in-memory datasource to verify the indicator works end-to-end with a real
 * HikariCP pool and {@link io.micronaut.management.health.aggregator.HealthAggregator}.
 */
class HikariHealthIndicatorIntegrationSpec extends Specification {

    void "indicator reports UP when datasource is available"() {
        given:
        ApplicationContext ctx = ApplicationContext.run([
                'datasources.default'                          : [:],
                (HealthEndpoint.PREFIX + '.enabled')           : true,
                (HealthEndpoint.PREFIX + '.sensitive')         : false,
        ])

        when:
        HikariHealthIndicator indicator = ctx.getBean(HikariHealthIndicator)
        HealthResult result = Flux.from(indicator.result).blockFirst(Duration.ofSeconds(5))

        then:
        result != null
        result.status == HealthStatus.UP

        cleanup:
        ctx.close()
    }

    void "aggregated result contains pool details"() {
        given:
        ApplicationContext ctx = ApplicationContext.run([
                'datasources.default'                 : [:],
                (HealthEndpoint.PREFIX + '.enabled')  : true,
                (HealthEndpoint.PREFIX + '.sensitive'): false,
        ])

        when:
        HikariHealthIndicator indicator = ctx.getBean(HikariHealthIndicator)
        HealthResult result = Flux.from(indicator.result).blockFirst(Duration.ofSeconds(5))
        Map<String, Object> details = (Map<String, Object>) result.details

        then:
        // The HealthAggregator wraps per-pool results under the "hikariCP" parent.
        // The details map is keyed by pool name, with each value being a nested HealthResult.
        details != null
        details.size() == 1

        cleanup:
        ctx.close()
    }

    void "not registered when no DatasourceConfiguration is present"() {
        given:
        // No datasource properties → @Requires(beans = DatasourceConfiguration.class) is not satisfied.
        // This test would fail if Java annotation processing were absent, which the runtime logic
        // tests in HikariHealthIndicatorSpec cannot catch (they instantiate the class directly).
        ApplicationContext ctx = ApplicationContext.run([:])

        expect:
        !ctx.containsBean(DatasourceConfiguration)
        !ctx.containsBean(HikariHealthIndicator)

        cleanup:
        ctx.close()
    }

    void "indicator is absent when disabled via property"() {
        given:
        ApplicationContext ctx = ApplicationContext.run([
                'datasources.default'                                   : [:],
                (HealthEndpoint.PREFIX + '.enabled')                    : true,
                (HealthEndpoint.PREFIX + '.jdbc.hikari.enabled')        : false,
        ])

        when:
        ctx.getBean(HikariHealthIndicator)

        then:
        thrown(NoSuchBeanException)

        cleanup:
        ctx.close()
    }
}
