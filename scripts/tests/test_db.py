import clickhouse_connect

def test_connection():
    try:
        # Create a client
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,  # Using HTTP port
            username='default',
            password='clickhouse'
        )
        
        # Test connection
        result = client.query('SELECT 1')
        print("Successfully connected to ClickHouse!")
        print(f"Query result: {result.result_rows}")
        
        # Create database for macroeconomic data
        print("\nCreating database...")
        client.command('CREATE DATABASE IF NOT EXISTS macro')
        
        # Create sample table
        print("\nCreating sample table...")
        client.command('''
            CREATE TABLE IF NOT EXISTS macro.economic_indicators
            (
                date Date,
                indicator_name LowCardinality(String),
                value Float64,
                source LowCardinality(String),
                updated_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, indicator_name)
        ''')
        print("Successfully created table!")
        
        # Show databases
        print("\nAvailable databases:")
        dbs = client.query('SHOW DATABASES')
        for db in dbs.result_rows:
            print(f"- {db[0]}")
            
        # Show tables in macro database
        print("\nTables in macro database:")
        tables = client.query('SHOW TABLES FROM macro')
        for table in tables.result_rows:
            print(f"- {table[0]}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    test_connection() 