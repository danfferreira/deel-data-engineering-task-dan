from pyflink.table import EnvironmentSettings, TableEnvironment

def create_source_tables(table_env: TableEnvironment):
    # Create Kafka source table for orders
    table_env.execute_sql("DROP TABLE IF EXISTS kafka_orders;")
    table_env.execute_sql("""
        CREATE TABLE kafka_orders (
            raw_msg BYTES,
            payload STRING,
            order_id AS CAST(JSON_VALUE(payload, '$.after.order_id') AS BIGINT),
            customer_id AS CAST(JSON_VALUE(payload, '$.after.customer_id') AS BIGINT),
            order_date AS JSON_VALUE(payload, '$.after.order_date'),
            delivery_date AS JSON_VALUE(payload, '$.after.delivery_date'),
            status AS JSON_VALUE(payload, '$.after.status'),
            updated_at AS CAST(JSON_VALUE(payload, '$.after.updated_at') AS BIGINT),
            op AS JSON_VALUE(payload, '$.op'),
            event_time AS TO_TIMESTAMP_LTZ(
                              CAST(JSON_VALUE(payload, '$.after.updated_at') AS BIGINT),
                              3
                          ),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'finance.operations.orders',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-analytics-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        );
    """)

    # Create Kafka source table for order_items
    table_env.execute_sql("DROP TABLE IF EXISTS kafka_order_items;")
    table_env.execute_sql("""
        CREATE TABLE kafka_order_items (
            raw_msg BYTES,
            payload STRING,
            order_item_id AS CAST(JSON_VALUE(payload, '$.after.order_item_id') AS BIGINT),
            order_id AS CAST(JSON_VALUE(payload, '$.after.order_id') AS BIGINT),
            product_id AS CAST(JSON_VALUE(payload, '$.after.product_id') AS BIGINT),
            quanity AS CAST(JSON_VALUE(payload, '$.after.quanity') AS INT),
            updated_at AS CAST(JSON_VALUE(payload, '$.after.updated_at') AS BIGINT),
            op AS JSON_VALUE(payload, '$.op'),
            event_time AS TO_TIMESTAMP_LTZ(
                              CAST(JSON_VALUE(payload, '$.after.updated_at') AS BIGINT),
                              3
                          ),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'finance.operations.order_items',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-analytics-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        );
    """)

    # Create Kafka source table for customers (if needed for pending orders ranking)
    table_env.execute_sql("DROP TABLE IF EXISTS kafka_customers;")
    table_env.execute_sql("""
        CREATE TABLE kafka_customers (
            raw_msg BYTES,
            payload STRING,
            customer_id AS CAST(JSON_VALUE(payload, '$.after.customer_id') AS BIGINT),
            customer_name AS JSON_VALUE(payload, '$.after.customer_name'),
            is_active AS CAST(JSON_VALUE(payload, '$.after.is_active') AS BOOLEAN),
            updated_at AS CAST(JSON_VALUE(payload, '$.after.updated_at') AS BIGINT),
            op AS JSON_VALUE(payload, '$.op'),
            event_time AS TO_TIMESTAMP_LTZ(
                              CAST(JSON_VALUE(payload, '$.after.updated_at') AS BIGINT),
                              3
                          ),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'finance.operations.customers',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-analytics-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        );
    """)

def create_sink_tables(table_env: TableEnvironment):
    # 1. Open orders by delivery_date and status
    table_env.execute_sql("DROP TABLE IF EXISTS sink_open_orders_by_delivery_status;")
    table_env.execute_sql("""
        CREATE TABLE sink_open_orders_by_delivery_status (
            delivery_date VARCHAR,
            status VARCHAR,
            open_count BIGINT,
            PRIMARY KEY (delivery_date, status) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://db-sink:5432/sink_db',
            'table-name' = 'open_orders_by_delivery_status',
            'username' = 'sink_user',
            'password' = 'sink_password'
        );
    """)

    # 2. Top 3 delivery dates with more open orders
    table_env.execute_sql("DROP TABLE IF EXISTS sink_top3_delivery_dates;")
    table_env.execute_sql("""
        CREATE TABLE sink_top3_delivery_dates (
            rank_num BIGINT,
            delivery_date VARCHAR,
            open_count BIGINT,
            PRIMARY KEY (rank_num, delivery_date) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://db-sink:5432/sink_db',
            'table-name' = 'top3_delivery_dates',
            'username' = 'sink_user',
            'password' = 'sink_password'
        );
    """)

    # 3. Pending items by product_id
    table_env.execute_sql("DROP TABLE IF EXISTS sink_pending_items_by_product;")
    table_env.execute_sql("""
        CREATE TABLE sink_pending_items_by_product (
            product_id BIGINT,
            pending_count BIGINT,
            PRIMARY KEY (product_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://db-sink:5432/sink_db',
            'table-name' = 'pending_items_by_product',
            'username' = 'sink_user',
            'password' = 'sink_password'
        );
    """)

    # 4. Top 3 customers with more pending orders
    table_env.execute_sql("DROP TABLE IF EXISTS sink_top3_customers_pending_orders;")
    table_env.execute_sql("""
        CREATE TABLE sink_top3_customers_pending_orders (
            rank_num BIGINT,
            customer_id BIGINT,
            pending_orders BIGINT,
            PRIMARY KEY (rank_num, customer_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://db-sink:5432/sink_db',
            'table-name' = 'top3_customers_pending_orders',
            'username' = 'sink_user',
            'password' = 'sink_password'
        );
    """)

def apply_analytics(table_env: TableEnvironment):

    # Number of open orders by delivery_date and status.
    table_env.execute_sql("""
        INSERT INTO sink_open_orders_by_delivery_status
        SELECT
            delivery_date,
            status,
            COUNT(*) AS open_count
        FROM (
            SELECT
                delivery_date,
                status
            FROM kafka_orders
            WHERE status NOT IN ('COMPLETED','CANCELED')
            AND op IN ('c', 'u', 'r')
        ) t
        GROUP BY delivery_date, status;
    """)

    # Top 3 delivery dates with more open orders.
    table_env.execute_sql("""
        INSERT INTO sink_top3_delivery_dates
        SELECT rank_num, delivery_date, open_count FROM (
            SELECT
                delivery_date,
                open_count,
                ROW_NUMBER() OVER (ORDER BY open_count DESC) AS rank_num
            FROM (
                SELECT
                    delivery_date,
                    COUNT(*) AS open_count
                FROM kafka_orders
                WHERE status NOT IN ('COMPLETED','CANCELED')
                AND op IN ('c', 'u', 'r')
                GROUP BY delivery_date
            ) subquery
        ) subquery2
        WHERE rank_num <= 3;
    """)

    #Number of pending items by product_id.
    table_env.execute_sql("""
        INSERT INTO sink_pending_items_by_product
        SELECT
        oi.product_id,
        SUM(oi.quanity) AS pending_count
        FROM kafka_order_items AS oi
        JOIN kafka_orders AS o
          ON oi.order_id = o.order_id
        WHERE o.status NOT IN ('COMPLETED','CANCELED')
        AND oi.op IN ('c', 'u', 'r')
        GROUP BY oi.product_id;
    """)


    # Top 3 customers with more pending orders.
    table_env.execute_sql("""
        INSERT INTO sink_top3_customers_pending_orders
        SELECT rank_num, customer_id, pending_orders FROM (
        SELECT
        customer_id,
        pending_orders,
        ROW_NUMBER() OVER (ORDER BY pending_orders DESC) AS rank_num
            FROM (
                    SELECT
                    customer_id,
                    COUNT(order_id) AS pending_orders
                    FROM kafka_orders
                    WHERE status NOT IN ('COMPLETED','CANCELED')
                    AND op IN ('c', 'u', 'r')          
                    GROUP BY customer_id
            ) subquery
        ) subquery2
        WHERE rank_num <= 3;
    """)

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    create_source_tables(table_env)
    create_sink_tables(table_env)
    apply_analytics(table_env)

if __name__ == '__main__':
    main()
