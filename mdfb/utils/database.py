import sqlite3
import platformdirs
import os

def create_db(path: str):
    con = sqlite3.connect(os.path.join(path, "test.db"))
    cur = con.cursor()
    res = cur.execute("""
        CREATE TABLE IF NOT EXISTS downloaded_posts (
            user_did TEXT NOT NULL,
            user_post_uri TEXT NOT NULL,
            feed_type TEXT NOT NULL,
            poster_post_uri TEXT NOT NULL,
            PRIMARY KEY (user_post_uri, user_did, feed_type)
        );
    """)
    con.close()

def connect_db() -> sqlite3.Connection:
    con = sqlite3.connect(os.path.join(platformdirs.user_data_path("mdfb"), "test.db"))
    return con

def insert_post(cur: sqlite3.Cursor, rows: list[tuple]) -> bool:
    res = cur.executemany("""
        INSERT OR IGNORE INTO downloaded_posts (user_did, user_post_uri, feed_type, poster_post_uri) 
        VALUES (?, ?, ?, ?)
    """, rows)
    
    if res.rowcount > 0:
        return True
    return False

def check_post_exists(cur: sqlite3.Cursor, user_did: str, user_post_uri: str, feed_type: str) -> bool:
    res = cur.execute("""
        SELECT * FROM downloaded_posts 
        WHERE user_did = ? 
        AND user_post_uri = ?
        AND feed_type = ?
    """, (user_did, user_post_uri, feed_type))

    row = res.fetchone()
    if row:
        return True
    return False