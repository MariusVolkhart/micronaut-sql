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

import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.HikariPoolMXBean
import io.micronaut.configuration.jdbc.hikari.DatasourceConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.health.HealthStatus
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.management.health.aggregator.HealthAggregator
import io.micronaut.management.health.indicator.HealthResult
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.Executors

/**
 * Unit tests for {@link HikariHealthIndicator}.
 *
 * Uses mocked {@link DataSource}/{@link HikariDataSource} to verify error paths that cannot be
 * triggered against a healthy database. Integration tests with a real datasource are in
 * {@link HikariHealthIndicatorIntegrationSpec}.
 */
class HikariHealthIndicatorSpec extends Specification {

    // Passthrough aggregator: returns the per-pool publisher unchanged,
    // bypassing DefaultHealthAggregator's wrapping/nesting behavior.
    // This lets single-pool tests assert directly on the per-pool HealthResult.
    HealthAggregator passthroughAggregator = Mock(HealthAggregator) {
        aggregate(_, _) >> { String name, Publisher results -> results }
    }

    HikariPoolMXBean mockMxBean = Mock(HikariPoolMXBean) {
        getTotalConnections() >> 10
        getActiveConnections() >> 10
        getIdleConnections() >> 0
        getThreadsAwaitingConnection() >> 5
    }

    HikariDataSource mockHikari = Mock(HikariDataSource) {
        getPoolName() >> "HikariPool-test"
        getHikariPoolMXBean() >> mockMxBean
    }

    DataSource mockDataSource = Mock(DataSource) {
        unwrap(HikariDataSource) >> mockHikari
    }

    DatasourceConfiguration mockConfig = Mock(DatasourceConfiguration) {
        getName() >> "default"
        getHealthCheckTimeout() >> Duration.ofSeconds(2)
    }

    ApplicationContext mockApplicationContext = Mock(ApplicationContext) {
        getBean(DataSource, Qualifiers.byName("default")) >> mockDataSource
    }

    def testExecutor = Executors.newCachedThreadPool()

    void "reports UP when connection is acquired successfully"() {
        given:
        Connection mockConnection = Mock(Connection)
        mockHikari.getConnection() >> mockConnection

        when:
        def result = getHealthResult()

        then:
        result.status == HealthStatus.UP
        !(result.details instanceof Map && ((Map) result.details).containsKey("error"))
    }

    void "reports DOWN when connection throws SQLException"() {
        given:
        mockHikari.getConnection() >> { throw new SQLException("Connection refused") }

        when:
        def result = getHealthResult()

        then:
        result.status == HealthStatus.DOWN
    }

    void "includes pool stats on success"() {
        given:
        Connection mockConnection = Mock(Connection)
        mockHikari.getConnection() >> mockConnection

        when:
        def details = detailsMap(getHealthResult())

        then:
        details["pool"] == "HikariPool-test"
        details["totalConnections"] == 10
        details["activeConnections"] == 10
        details["idleConnections"] == 0
        details["threadsAwaitingConnection"] == 5
    }

    void "includes pool stats even when connection fails"() {
        given:
        mockHikari.getConnection() >> { throw new SQLException("Connection refused") }

        when:
        def details = detailsMap(getHealthResult())

        then:
        details["pool"] == "HikariPool-test"
        details["totalConnections"] == 10
        details["activeConnections"] == 10
        details["idleConnections"] == 0
        details["threadsAwaitingConnection"] == 5
    }

    void "reports DOWN when connection close throws"() {
        given:
        Connection badConnection = Mock(Connection) {
            close() >> { throw new SQLException("Connection reset during close") }
        }
        mockHikari.getConnection() >> badConnection

        when:
        def result = getHealthResult()

        then:
        result.status == HealthStatus.DOWN
    }

    void "reports DOWN when connection acquisition blocks beyond timeout"() {
        given:
        mockHikari.getConnection() >> { Thread.sleep(10_000L); Mock(Connection) }

        when:
        def result = getHealthResult()

        then:
        result.status == HealthStatus.DOWN
    }

    void "timeout fires within expected duration"() {
        given:
        mockHikari.getConnection() >> { Thread.sleep(10_000L); Mock(Connection) }

        when:
        long start = System.currentTimeMillis()
        getHealthResult()
        long elapsed = System.currentTimeMillis() - start

        then:
        // Should complete in roughly 2s (the configured timeout), not 10s (the mock sleep).
        // Allow generous margin for CI but must be well under the mock's 10s sleep.
        elapsed < 5_000L
    }

    void "uses pool name as health result key"() {
        given:
        Connection mockConnection = Mock(Connection)
        mockHikari.getConnection() >> mockConnection

        when:
        def result = getHealthResult()

        then:
        result.name == "HikariPool-test"
    }

    void "checks multiple Hikari pools"() {
        given:
        HikariPoolMXBean mxBean2 = Mock(HikariPoolMXBean) {
            getTotalConnections() >> 5
            getActiveConnections() >> 2
            getIdleConnections() >> 3
            getThreadsAwaitingConnection() >> 0
        }
        HikariDataSource hikari2 = Mock(HikariDataSource) {
            getPoolName() >> "HikariPool-secondary"
            getHikariPoolMXBean() >> mxBean2
        }
        DataSource dataSource2 = Mock(DataSource) {
            unwrap(HikariDataSource) >> hikari2
        }
        Connection mockConnection = Mock(Connection)
        mockHikari.getConnection() >> mockConnection
        hikari2.getConnection() >> mockConnection

        DatasourceConfiguration mockConfig2 = Mock(DatasourceConfiguration) {
            getName() >> "secondary"
            getHealthCheckTimeout() >> Duration.ofSeconds(2)
        }
        ApplicationContext twoPoolCtx = Mock(ApplicationContext) {
            getBean(DataSource, Qualifiers.byName("default")) >> mockDataSource
            getBean(DataSource, Qualifiers.byName("secondary")) >> dataSource2
        }

        when:
        def results = getAllHealthResults([mockConfig, mockConfig2] as DatasourceConfiguration[], twoPoolCtx)

        then:
        results.size() == 2
        results.collect { it.name }.sort() == ["HikariPool-secondary", "HikariPool-test"].sort()
        results.every { it.status == HealthStatus.UP }
    }

    void "skips non-Hikari DataSources"() {
        given:
        DataSource nonHikariDataSource = Mock(DataSource) {
            unwrap(HikariDataSource) >> { throw new SQLException("Not a wrapper for HikariDataSource") }
        }
        Connection mockConnection = Mock(Connection)
        mockHikari.getConnection() >> mockConnection

        DatasourceConfiguration mockConfig2 = Mock(DatasourceConfiguration) {
            getName() >> "other"
            getHealthCheckTimeout() >> Duration.ofSeconds(2)
        }
        ApplicationContext twoPoolCtx = Mock(ApplicationContext) {
            getBean(DataSource, Qualifiers.byName("default")) >> mockDataSource
            getBean(DataSource, Qualifiers.byName("other")) >> nonHikariDataSource
        }

        when:
        def results = getAllHealthResults([mockConfig, mockConfig2] as DatasourceConfiguration[], twoPoolCtx)

        then:
        // Only the Hikari-backed DataSource should produce a result.
        results.size() == 1
        results[0].name == "HikariPool-test"
    }

    void "returns empty when no DataSources are Hikari"() {
        given:
        DataSource nonHikariDataSource = Mock(DataSource) {
            unwrap(HikariDataSource) >> { throw new SQLException("Not a wrapper") }
        }
        DatasourceConfiguration badConfig = Mock(DatasourceConfiguration) {
            getName() >> "default"
            getHealthCheckTimeout() >> Duration.ofSeconds(2)
        }
        ApplicationContext badCtx = Mock(ApplicationContext) {
            getBean(DataSource, Qualifiers.byName("default")) >> nonHikariDataSource
        }

        when:
        def results = getAllHealthResults([badConfig] as DatasourceConfiguration[], badCtx)

        then:
        results.isEmpty()
    }

    void "returns empty when DataSource array is empty"() {
        when:
        def results = getAllHealthResults([] as DatasourceConfiguration[], Mock(ApplicationContext))

        then:
        results.isEmpty()
    }

    void "reports individual pool status when one pool is down"() {
        given:
        HikariPoolMXBean mxBean2 = Mock(HikariPoolMXBean) {
            getTotalConnections() >> 5
            getActiveConnections() >> 5
            getIdleConnections() >> 0
            getThreadsAwaitingConnection() >> 3
        }
        HikariDataSource hikari2 = Mock(HikariDataSource) {
            getPoolName() >> "HikariPool-secondary"
            getHikariPoolMXBean() >> mxBean2
        }
        DataSource dataSource2 = Mock(DataSource) {
            unwrap(HikariDataSource) >> hikari2
        }
        Connection mockConnection = Mock(Connection)
        mockHikari.getConnection() >> mockConnection
        hikari2.getConnection() >> { throw new SQLException("Connection refused") }

        DatasourceConfiguration mockConfig2 = Mock(DatasourceConfiguration) {
            getName() >> "secondary"
            getHealthCheckTimeout() >> Duration.ofSeconds(2)
        }
        ApplicationContext twoPoolCtx = Mock(ApplicationContext) {
            getBean(DataSource, Qualifiers.byName("default")) >> mockDataSource
            getBean(DataSource, Qualifiers.byName("secondary")) >> dataSource2
        }

        when:
        def results = getAllHealthResults([mockConfig, mockConfig2] as DatasourceConfiguration[], twoPoolCtx)
        def byName = results.collectEntries { [it.name, it] }

        then:
        results.size() == 2
        byName["HikariPool-test"].status == HealthStatus.UP
        byName["HikariPool-secondary"].status == HealthStatus.DOWN
    }

    void "aggregator receives name hikariCP"() {
        given:
        String capturedName = null
        HealthAggregator capturingAggregator = Mock(HealthAggregator) {
            aggregate(_, _) >> { String name, Publisher results ->
                capturedName = name
                results
            }
        }
        Connection mockConnection = Mock(Connection)
        mockHikari.getConnection() >> mockConnection

        when:
        def indicator = new HikariHealthIndicator(
                [mockConfig] as DatasourceConfiguration[],
                null,
                mockApplicationContext,
                testExecutor,
                capturingAggregator
        )
        Flux.from(indicator.result).blockFirst(Duration.ofSeconds(10))

        then:
        capturedName == "hikariCP"
    }

    private HealthResult getHealthResult() {
        def indicator = new HikariHealthIndicator(
                [mockConfig] as DatasourceConfiguration[],
                null,
                mockApplicationContext,
                testExecutor,
                passthroughAggregator
        )
        def result = Flux.from(indicator.result).blockFirst(Duration.ofSeconds(10))
        assert result != null : "Health check did not emit a result"
        return result
    }

    private List<HealthResult> getAllHealthResults(DatasourceConfiguration[] configs, ApplicationContext appCtx) {
        def indicator = new HikariHealthIndicator(
                configs,
                null,
                appCtx,
                testExecutor,
                passthroughAggregator
        )
        return Flux.from(indicator.result).collectList().block(Duration.ofSeconds(10)) ?: []
    }

    private static Map<String, Object> detailsMap(HealthResult result) {
        return (Map<String, Object>) result.details
    }
}
