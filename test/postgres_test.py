import psycopg2
import os
import sys

def test_postgres_connection():
    # Connection parameters
    host = os.environ.get("POSTGRES_HOST", "postgres")
    database = os.environ.get("POSTGRES_DB", "streaming")
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "password")
    port = os.environ.get("POSTGRES_PORT", "5432")
    
    print(f"Attempting to connect to PostgreSQL with:")
    print(f"  Host: {host}")
    print(f"  Database: {database}")
    print(f"  User: {user}")
    print(f"  Port: {port}")
    print(f"  Password: {'*' * len(password)}")
    
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        
        print("✅ Connection successful!")
        
        # Test querying the database
        cur = conn.cursor()
        print("Testing if tables exist...")
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = cur.fetchall()
        
        if tables:
            print(f"Tables in database: {', '.join(table[0] for table in tables)}")
            
            # Check if fraud_predictions table exists
            if any(table[0] == 'fraud_predictions' for table in tables):
                print("✅ fraud_predictions table exists")
                
                # Check table structure
                print("Checking fraud_predictions table structure:")
                cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='fraud_predictions'")
                columns = cur.fetchall()
                for col in columns:
                    print(f"  - {col[0]}: {col[1]}")
                
                # Check if table has data
                cur.execute("SELECT COUNT(*) FROM fraud_predictions")
                count = cur.fetchone()[0]
                print(f"✅ fraud_predictions table has {count} rows")
            else:
                print("❌ fraud_predictions table does not exist!")
        else:
            print("❌ No tables found in the database")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nPossible solutions:")
        print("1. Make sure PostgreSQL container is running: docker-compose ps")
        print("2. Check logs for errors: docker-compose logs postgres")
        print("3. Try connecting with correct host:")
        print("   - Use 'localhost' if running script outside Docker")
        print("   - Use 'postgres' if running script inside Docker network")
        print("4. Check if database was initialized properly")
        print("5. For local testing, try: POSTGRES_HOST=localhost python postgres_test.py")
        return False
        
    return True

if __name__ == "__main__":
    # Allow host override from command line
    if len(sys.argv) > 1:
        os.environ["POSTGRES_HOST"] = sys.argv[1]
        
    test_postgres_connection() 