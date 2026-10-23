package io.lytecache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Internal: lets the hash and event SQL use {@code :name} parameters, as the other
 * implementations do, on top of JDBC's positional binding. Each distinct name becomes one
 * SQLite numbered parameter ({@code ?N}), so a name used several times in one statement binds
 * once. Names are only recognized outside single-quoted SQL literals.
 */
final class NamedSql {
    private static final Pattern NAME = Pattern.compile("'(?:[^']|'')*'|:([A-Za-z_][A-Za-z0-9_]*)");

    private final String sql;
    private final Map<String, Integer> slots;

    private NamedSql(String sql, Map<String, Integer> slots) {
        this.sql = sql;
        this.slots = slots;
    }

    static NamedSql of(String namedSql) {
        Map<String, Integer> slots = new LinkedHashMap<>();
        Matcher m = NAME.matcher(namedSql);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            if (m.group(1) == null) {
                m.appendReplacement(out, Matcher.quoteReplacement(m.group()));
                continue;
            }
            int slot = slots.computeIfAbsent(m.group(1), k -> slots.size() + 1);
            m.appendReplacement(out, "?" + slot);
        }
        m.appendTail(out);
        return new NamedSql(out.toString(), slots);
    }

    /** Prepares the statement and binds every parameter it uses from {@code params}. */
    PreparedStatement prepare(Connection conn, Map<String, ?> params) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        try {
            for (Map.Entry<String, Integer> e : slots.entrySet()) {
                if (!params.containsKey(e.getKey())) {
                    throw new IllegalArgumentException("missing SQL parameter :" + e.getKey());
                }
                Object v = params.get(e.getKey());
                if (v == null) {
                    ps.setNull(e.getValue(), java.sql.Types.NULL);
                } else {
                    ps.setObject(e.getValue(), v);
                }
            }
            return ps;
        } catch (SQLException | RuntimeException e) {
            ps.close();
            throw e;
        }
    }
}
