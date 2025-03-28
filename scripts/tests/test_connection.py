from clickhouse_driver import Client
import os

def test_clickhouse_connection():
    # Connect to ClickHouse
    client = Client(
        host='localhost',
        port=9000,
        user='default',
        password='clickhouse',  # Updated password
        database='default'
    )
    
    try:
        # Test connection
        client.execute('SELECT 1')
        print("Successfully connected to ClickHouse!")
        
        # Check if tables exist
        tables = client.execute('SHOW TABLES')
        print("\nExisting tables:")
        for table in tables:
            print(f"- {table[0]}")
            
        # Test table creation
        print("\nTesting table creation...")
        client.execute('''
            CREATE TABLE IF NOT EXISTS test_table
            (
                id UInt32,
                name String
            )
            ENGINE = MergeTree()
            ORDER BY id
        ''')
        print("Successfully created test table!")
        
        # Clean up
        client.execute('DROP TABLE IF EXISTS test_table')
        print("Successfully cleaned up test table!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    test_clickhouse_connection() 