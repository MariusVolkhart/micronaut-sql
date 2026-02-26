/*
 * Copyright 2017-2026 original authors
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
package io.micronaut.configuration.jooq;

import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionStatus;
import io.micronaut.transaction.jdbc.DataSourceTransactionManager;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;

import java.sql.Connection;
import java.util.function.Function;

/**
 * Programmatic transaction helper for jOOQ that executes work inside a Micronaut-managed
 * JDBC transaction scope and provides a transaction-scoped {@link DSLContext}.
 * <p>
 * This is the recommended way to use jOOQ transactions outside {@code @Transactional}/{@code @Connectable}
 * methods when Micronaut Data contextual connections are enabled.
 *
 * @author Micronaut Foundation
 * @since 6.0.0
 */
@Requires(classes = DataSourceTransactionManager.class)
@EachBean(DataSourceTransactionManager.class)
@Internal
public final class JooqTransactionOperations {

    private final DataSourceTransactionManager transactionManager;
    private final Configuration configuration;

    JooqTransactionOperations(DataSourceTransactionManager transactionManager, Configuration configuration) {
        this.transactionManager = transactionManager;
        this.configuration = configuration;
    }

    /**
     * Execute work in a Micronaut-managed transaction and provide a tx-scoped {@link DSLContext}.
     *
     * @param work The work
     * @param <T> Result type
     * @return The result
     */
    public <T> @NonNull T execute(@NonNull Function<DSLContext, T> work) {
        return transactionManager.execute(TransactionDefinition.DEFAULT, status -> work.apply(txDsl(status)));
    }

    private @NonNull DSLContext txDsl(@NonNull TransactionStatus<Connection> status) {
        Connection connection = status.getConnectionStatus().getConnection();
        Configuration derived = configuration.derive(new DefaultConnectionProvider(connection));
        return DSL.using(derived);
    }
}
