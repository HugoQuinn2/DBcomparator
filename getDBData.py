import mysql.connector;
import pyodbc;

class DBData:

    db_usr  = None
    db_pwd  = None
    db_host = None
    db_name = None
    db_pag  = 300
    db_con  = None
    db_connector = ["mysql", "sqlserver"]
    db_sql  = None

    def setCredentials(self, db_usr, db_pwd):
        self.db_usr = db_usr
        self.db_pwd = db_pwd

    def setDataBase(self, db_host, db_name):
        self.db_host = db_host
        self.db_name = db_name

    def setSql(self, db_sql):
        self.db_sql = db_sql

    def getPrimaryKey(self, con, tbl):
        cur = con.cursor()
        cur.execute(f"SHOW COLUMNS FROM {tbl}")
        info_tbl = cur.fetchall()

        for column in info_tbl:
            nome_column, data, nulo, primary_key, _ = column
        if primary_key == "PRI":
            con.close()
            return name_column
        else:
            return False

    def init_con(self, db_conector):
        if db_conector == self.db_connector[0]:
            if self.db_usr is not None or self.db_pwd is not None or self.db_host is not None or self.db_name is not None:
                con = mysql.connector.connect(
                    host=self.db_host,
                    user=self.db_usr,
                    password=self.db_pwd,
                    database=self.db_name
                )
                return con
        else:
            return False

    def getTblName(self, sql):
        patron = re.compile(r'\bFROM\s+(\w+)', re.IGNORECASE)
        coincidencia = patron.search(consulta_sql)

        if coincidencia:
            return coincidencia.group(1)
        else:
            return None


    def getPagData(self, con, sql, primary_key, last_id):
        cur = con.cursor()
        self.db_sql = self.db_sql + f"WHERE {primary_key} > %s LIMIT %s"
        cur.execute(self.db_sql, (last_id, self.db_pag))
        data = cur.fetchall()
        con.close()

        return data


