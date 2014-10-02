import sqlite3 as lite
from os import listdir


class Database(object): 

  def __init__(self, db_file="test.db"): #
    self.conn = lite.connect(db_file)
    self.setupDefaultData()
    print "Database '{0}' initialized!".format(db_file)

  def setupDefaultData(self):
    
    # conn = lite.connect(':memory:')

    with self.conn:

      self.conn.row_factory = lite.Row 

      cur = self.conn.cursor()

      ###################
      # DATA DEFINITION #
      ###################


      # Tables for simulating ENUM state
      cur.execute("DROP TABLE IF EXISTS a_state")
      cur.execute("CREATE TABLE a_state(state TEXT PRIMARY KEY)")
  
      cur.execute("INSERT INTO a_state VALUES ('enabled') ")
      cur.execute("INSERT INTO a_state VALUES ('disabled') ")

      # Tables for simulating ENUM type
      cur.execute("DROP TABLE IF EXISTS a_type")
      cur.execute("CREATE TABLE a_type(type TEXT PRIMARY KEY)")
      cur.execute("INSERT INTO a_type VALUES('web')")
      cur.execute("INSERT INTO a_type VALUES('app')")

      # Tables for simulating ENUM platform
      cur.execute("DROP TABLE IF EXISTS platform")
      cur.execute("CREATE TABLE platform(pname CHAR PRIMARY KEY)")
      cur.execute("INSERT INTO platform VALUES('i')")
      cur.execute("INSERT INTO platform VALUES('a')")

      # Enable FOREIGN KEY constraint 
      cur.execute("PRAGMA foreign_keys = ON;")

      cur.execute("DROP TABLE IF EXISTS audience")
      cur.execute("""CREATE TABLE audience(id INTEGER PRIMARY KEY,              
                        name TEXT NOT NULL UNIQUE,              
                        state TEXT NOT NULL,              
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,  
                        updated_at DATETIME,              
                        FOREIGN KEY(state) REFERENCES a_state(state)  
                         )""")
      cur.execute("DROP TABLE IF EXISTS audience_rule")
      cur.execute("""CREATE TABLE audience_rule( id INTEGER PRIMARY KEY,              
                            audience_id INT NOT NULL, /* last_insert_rowid()*/            
                            type TEXT NOT NULL,               
                            condition TEXT, 
                            platform CHAR NOT NULL,                
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,  
                            updated_at DATETIME,                
                            FOREIGN KEY(audience_id) REFERENCES audience(id)
                            FOREIGN KEY(type) REFERENCES a_type(type) 
                            FOREIGN KEY(platform) REFERENCES platform(pname) 
                            )""")

      # Create a trigger to update updated_at 
      cur.execute("DROP TRIGGER IF EXISTS after_update_audience")
      cur.execute("""CREATE TRIGGER after_update_audience AFTER UPDATE ON audience  
              BEGIN                           
                update audience SET updated_at = DATETIME('now')    
                WHERE id = NEW.id;                    
              END ;""")

      cur.execute("DROP TRIGGER IF EXISTS after_update_audience_rule")
      cur.execute("""CREATE TRIGGER after_update_audience_rule AFTER UPDATE ON audience_rule  
              BEGIN                                 
                update audience_rule SET updated_at = DATETIME('now')       
                WHERE id = NEW.id;                          
              END ;""")



      ################
      # MANIPULATION #
      ################
      audience_vals = [('aud1', 'enabled'), 
                       ('aud2', 'enabled'), 
                       ('aud3', 'disabled')]
     
      cur.executemany("""INSERT INTO audience(name, state) 
                       VALUES (?,?)""", audience_vals)
      cur.execute("UPDATE audience SET state = 'enabled' WHERE name = 'aud3';")

      ## NOTE: use audience_id = last_insert_rowid() to insert records to audience_rule 
      audience_rule_vals = [(1, 'app', 'a', "(com.android.Preconfig and com.amazon.mp3) or com.android.bluetooth and NOT com.android.apps.tag"), 
                            (2, 'app', 'i', "597986893 and 435719709 and 501995569"), 
                            (3, 'web', 'i', "(312506856 and 463835885 or 310633997) or not 51492171")]
      cur.executemany("""INSERT INTO audience_rule(audience_id, type, platform, condition) 
                     VALUES(?,?,?,?)""", audience_rule_vals)  
      cur.execute("UPDATE audience_rule SET type = 'app' WHERE audience_id = 3")


if __name__ == "__main__":
  Database()
