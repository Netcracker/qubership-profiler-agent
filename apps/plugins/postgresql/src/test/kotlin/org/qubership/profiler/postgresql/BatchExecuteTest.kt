package org.qubership.profiler.postgresql

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AutoClose
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
@Disabled("It is not clear how to capture profiling results after executing end-to-end sql")
class BatchExecuteTest {
    companion object {
        @AutoClose
        val datasource = HikariDataSource(
            HikariConfig().apply {
                driverClassName = "org.testcontainers.jdbc.ContainerDatabaseDriver"
                jdbcUrl = "jdbc:tc:postgresql:17-alpine:///testdb"
            }
        )

        @BeforeAll
        @JvmStatic
        fun createTables() {
            datasource.connection.use { con ->
                con.createStatement().use { st ->
                    st.execute("create table test_batch(id int8, value int8)")
                }
            }
        }

        @AfterAll
        @JvmStatic
        fun dropTables() {
            datasource.connection.use { con ->
                con.createStatement().use { st ->
                    st.execute("drop table if exists test_batch")
                }
            }
        }
    }

    @AutoClose
    val con = datasource.connection

    @Test
    fun batchExecuteTest() {
        con.prepareStatement("insert into test_batch(id, value) values(?,?)").use { ps ->
            (1..6).forEach {
                ps.setLong(1, it.toLong())
                ps.setLong(2, it.toLong() * 2)
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }
}
