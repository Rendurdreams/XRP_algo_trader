import sqlite3

def connect_db(db_file='xrp_data.db'):
    """Connect to the SQLite database."""
    conn = sqlite3.connect(db_file)
    return conn

def create_table(conn):
    """Create the xrp_ticker table."""
    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS xrp_ticker (
        time TEXT PRIMARY KEY,
        price REAL,
        open_24h REAL,
        volume_24h REAL,
        low_24h REAL,
        high_24h REAL,
        best_bid REAL,
        best_bid_size REAL,
        best_ask REAL,
        best_ask_size REAL,
        side TEXT,
        trade_id INTEGER,
        last_size REAL
    );
    '''
    conn.execute(create_table_sql)

def insert_data(conn, data):
    """Insert data into the xrp_ticker table."""
    insert_sql = ''' INSERT INTO xrp_ticker(time, price, open_24h, volume_24h, low_24h, high_24h, best_bid, best_bid_size, best_ask, best_ask_size, side, trade_id, last_size)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    conn.execute(insert_sql, data)
    conn.commit()

def main():
    # Connect to the database
    conn = connect_db()

    # Create table
    create_table(conn)

    # Sample data for testing
    test_data = ('2023-12-30T14:08:17.498783Z', 0.6246, 0.6365, 50414392.72112, 0.6122, 0.6365, 0.6245, 1575.000000, 0.6246, 1949.452858, 'buy', 36428518, 1306.164851)

    # Insert test data
    insert_data(conn, test_data)

    # Fetch and print the inserted data for verification
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM xrp_ticker")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    # Close the connection
    conn.close()

if __name__ == '__main__':
    main()
