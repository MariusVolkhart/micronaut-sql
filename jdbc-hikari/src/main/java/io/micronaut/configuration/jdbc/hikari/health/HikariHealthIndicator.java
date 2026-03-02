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
package io.micronaut.configuration.jdbc.hikari.health;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import io.micronaut.configuration.jdbc.hikari.DatasourceConfiguration;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import io.micronaut.health.HealthStatus;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.jdbc.DataSourceResolver;
import io.micronaut.management.endpoint.health.HealthEndpoint;
import io.micronaut.management.health.aggregator.HealthAggregator;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.micronaut.management.health.indicator.annotation.Readiness;
import io.micronaut.scheduling.TaskExecutors;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Health indicator for HikariCP connection pools.
 *
 * <p>Reports {@link HealthStatus#DOWN} when a connection cannot be acquired in a timely manner.
 * Includes pool stats (active/idle connections, threads awaiting) that
 * {@link io.micronaut.management.health.indicator.jdbc.JdbcIndicator} does not provide.
 *
 * @since 7.0
 */
@Singleton
@Readiness
@Requires(property = HealthEndpoint.PREFIX + ".jdbc.hikari.enabled", notEquals = StringUtils.FALSE)
@Requires(beans = HealthEndpoint.class)
@Requires(beans = DatasourceConfiguration.class)
public class HikariHealthIndicator implements HealthIndicator {

    /**
     * The name reported to {@link HealthAggregator} when multiple pools are present.
     */
    public static final String NAME = "hikariCP";

    private final List<PoolEntry> poolEntries;
    private final ExecutorService blockingExecutor;
    private final HealthAggregator<?> healthAggregator;

    /**
     * Creates a new indicator from the application's configured datasources.
     *
     * @param datasourceConfigurations all configured {@link DatasourceConfiguration} beans,
     *   auto-collected by Micronaut. Each configuration provides the pool name and
     *   health-check timeout; the corresponding {@link DataSource} bean is resolved by name.
     * @param dataSourceResolver unwraps Micronaut's proxy to the real {@link DataSource}.
     *   {@code null} falls back to {@link DataSourceResolver#DEFAULT} (identity).
     * @param applicationContext used to look up each {@link DataSource} bean by qualifier name.
     * @param blockingExecutor Micronaut's blocking I/O thread pool.
     * @param healthAggregator merges per-pool results under a single {@code "hikariCP"} parent.
     */
    public HikariHealthIndicator(
            DatasourceConfiguration[] datasourceConfigurations,
            @Nullable DataSourceResolver dataSourceResolver,
            ApplicationContext applicationContext,
            @Named(TaskExecutors.BLOCKING) ExecutorService blockingExecutor,
            HealthAggregator<?> healthAggregator) {
        DataSourceResolver resolver = dataSourceResolver != null ? dataSourceResolver : DataSourceResolver.DEFAULT;
        this.poolEntries = Arrays.stream(datasourceConfigurations)
                .map(config -> {
                    try {
                        DataSource dataSource = applicationContext.getBean(DataSource.class, Qualifiers.byName(config.getName()));
                        HikariDataSource hikari = resolver.resolve(dataSource).unwrap(HikariDataSource.class);
                        return new PoolEntry(hikari, config.getHealthCheckTimeout());
                    } catch (Exception e) {
                        // datasource disabled, not started, or not Hikari-backed
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();
        this.blockingExecutor = blockingExecutor;
        this.healthAggregator = healthAggregator;
    }

    @Override
    public Publisher<HealthResult> getResult() {
        if (poolEntries.isEmpty()) {
            return Flux.empty();
        }
        List<Publisher<HealthResult>> perPool = poolEntries.stream()
                .map(this::getResult)
                .toList();
        return healthAggregator.aggregate(NAME, Flux.merge(perPool));
    }

    private Publisher<HealthResult> getResult(PoolEntry entry) {
        HikariDataSource hikari = entry.hikari();
        return Mono.fromCallable(() -> {
                    // Hikari validates connections internally, so we don't need to perform any explicit action.
                    hikari.getConnection().close();
                    return HealthResult.builder(hikari.getPoolName(), HealthStatus.UP)
                            .details(poolStats(hikari)).build();
                })
                // subscribeOn() so the callable runs off the subscribing thread — without this,
                // fromCallable executes synchronously and timeout() cannot interrupt a blocked getConnection().
                // Uses Micronaut's blocking executor (same pool as AbstractHealthIndicator and JdbcIndicator)
                // rather than Reactor's boundedElastic() to avoid introducing a second thread pool.
                .subscribeOn(Schedulers.fromExecutorService(blockingExecutor))
                .timeout(entry.timeout())
                .onErrorResume(ex -> Mono.just(
                        HealthResult.builder(hikari.getPoolName(), HealthStatus.DOWN)
                                .exception(ex)
                                .details(poolStats(hikari)).build()
                ));
    }

    private Map<String, Object> poolStats(HikariDataSource hikari) {
        HikariPoolMXBean mxBean = hikari.getHikariPoolMXBean();
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("pool", hikari.getPoolName());
        stats.put("totalConnections", mxBean != null ? mxBean.getTotalConnections() : 0);
        stats.put("activeConnections", mxBean != null ? mxBean.getActiveConnections() : 0);
        stats.put("idleConnections", mxBean != null ? mxBean.getIdleConnections() : 0);
        stats.put("threadsAwaitingConnection", mxBean != null ? mxBean.getThreadsAwaitingConnection() : 0);
        return stats;
    }

    private record PoolEntry(HikariDataSource hikari, Duration timeout) { }
}
