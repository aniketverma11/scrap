
import mysql.connector

def parse_mysql_connector_string(connector_string):
    pairs = connector_string.split(";")
    parsed_dict = {}
    for pair in pairs:
        key, value = pair.strip().split("=")
        parsed_dict[key.lower()] = value
    return parsed_dict

class SimpleMysqlCacher:

    connector_string = None
    conn = None
    cursor = None

    def __init__(self, connector_string=None, base=None):
        self.setConnectorstring(connector_string)
        self.setBase(base)

    def getConnectorstring(self):
        return self.connector_string

    def setConnectorstring(self, connector_string):
        self.connector_string = connector_string
        self.connectDatabase()

    def connectDatabase(self):
        parsed_dict = parse_mysql_connector_string(self.connector_string)
        self.conn = mysql.connector.connect(
            host=parsed_dict["host"],
            user=parsed_dict["user"],
            password=parsed_dict["password"],
            database=parsed_dict["database"]
        )
        self.cursor = self.conn.cursor()

    def getBase(self):
        return self.base

    def setBase(self, base):
        self.base = base
        self.connectBase()

    def connectBase(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.base} (
            `key` VARCHAR(255) PRIMARY KEY,
            `value` MEDIUMBLOB
        )
        """
        self.cursor.execute(query)
        self.conn.commit()

    def set(self, key, binary_value):
        query = f"INSERT INTO {self.base} (`key`, `value`) VALUES (%s, %s) ON DUPLICATE KEY UPDATE `value` = %s"
        self.cursor.execute(query, (key, binary_value, binary_value))
        self.conn.commit()

    def get(self, key):
        query = f"SELECT `value` FROM {self.base} WHERE `key` = %s"
        self.cursor.execute(query, (key,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        return None

    def getAll(self):
        query = f"SELECT `key`, `value` FROM {self.base}"
        self.cursor.execute(query)
        for row in self.cursor:
            yield row[0], row[1]

    def clear(self, delete_base=False):
        query = f"DELETE FROM {self.base}"
        self.cursor.execute(query)
        if delete_base:
            query = f"DROP TABLE IF EXISTS {self.base}"
            self.cursor.execute(query)

    def importFrom(self, from_cacher=None):
        for item in from_cacher.getAll():
            self.set(item[0], item[1])

    def close(self):
        self.cursor.close()
        self.conn.close()

