package de.komoot.photon.config;

import com.beust.jcommander.Parameter;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;

@NullMarked
public class PostgresqlConfig {
    public static final String GROUP = "PostgreSQL options";

    @Parameter(names = "-host", category = GROUP, placeholder = "HOST", validateWith =  HostNameValidator.class, description = """
            Hostname of the PostgreSQL database
            """)
    private String host = "127.0.0.1";

    @Parameter(names = "-port", category = GROUP, placeholder = "PORT", description = """
            Port of the PostgreSQL database
            """)
    private int port = 5432;

    @Parameter(names = "-database", category = GROUP, placeholder = "NAME", description = """
            Database name of Nominatim database
            """)
    private String database = "nominatim";

    @Parameter(names = "-user", category = GROUP, placeholder = "NAME", description = """
            User for the PostgreSQL database
            """)
    @Nullable private String user = null;

    @Parameter(names = "-password", category = GROUP, placeholder = "PASSWORD", description = """
            Password for the PostgreSQL user (using parameter not recommended, use a pgpass file instead)
            """)
    @Nullable private String password = null;

    public DataSource getDataSource() {
        var dataSource = new PGSimpleDataSource();

        dataSource.setDatabaseName(database);
        dataSource.setServerNames(new String[]{host});
        dataSource.setPortNumbers(new int[]{port});

        if (user != null) {
            dataSource.setUser(user);
        }
        if (password != null) {
            dataSource.setPassword(password);
        }

        var connFactory = new PoolableConnectionFactory(new DataSourceConnectionFactory(dataSource), null);
        var connPool = new GenericObjectPool<>(connFactory);

        connFactory.setPool(connPool);
        connFactory.setDefaultAutoCommit(false);

        return new PoolingDataSource<>(connPool);
    }

    @Override
    public String toString() {
        return String.format("database %s at %s:%d (user: %s)",
                database, host, port, user == null ? "-" : user);
    }
}
