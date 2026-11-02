package com.example.orders.consumer.persistence;

import com.example.orders.consumer.domain.ProcessingOutcome;
import com.example.orders.consumer.domain.ProcessingStats;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** PostgreSQL adapter for {@link ProcessingStats}. */
@Component
public class JdbcProcessingStats implements ProcessingStats {

    private static final String UPSERT = """
            INSERT INTO consumer_processing_stats (outcome, occurrences, last_seen_at)
            VALUES (:outcome, 1, :now)
            ON CONFLICT (outcome) DO UPDATE
                SET occurrences  = consumer_processing_stats.occurrences + 1,
                    last_seen_at = EXCLUDED.last_seen_at
            """;

    private static final String SELECT_ALL = """
            SELECT outcome, occurrences FROM consumer_processing_stats ORDER BY outcome
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public JdbcProcessingStats(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    @Transactional
    public void increment(ProcessingOutcome outcome, Instant now) {
        jdbc.update(UPSERT, new MapSqlParameterSource()
                .addValue("outcome", outcome.name())
                .addValue("now", Timestamp.from(now)));
    }

    @Override
    @Transactional(readOnly = true)
    public Map<String, Long> counts() {
        Map<String, Long> result = new LinkedHashMap<>();
        jdbc.query(SELECT_ALL, rs -> {
            result.put(rs.getString("outcome"), rs.getLong("occurrences"));
        });
        return result;
    }
}
