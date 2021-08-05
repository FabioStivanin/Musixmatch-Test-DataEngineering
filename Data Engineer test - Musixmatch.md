# DONE
- connessione al db MySQL v 5.6.10 
SELECT VERSION();
- non vedeva la tabella, dall'editor MySQL Workbench ho eseguito la seguente query per vedere le tabelle dello schema:
    SELECT table_name FROM information_schema.tables;
- creata prima tabella con colonne utili (piulizia del dato, trim, passaggi da string a number quando possibile, via chiavi null)
- indici su data (portata da timestamp a int), indici su device (stringa lunga max 136)
- uso mysql nel jdbc driver invece di mariadb altrimenti errori lettura

# PARAMETRI CONNESSIONE AL DB
driver = "org.mariadb.jdbc.Driver"
url = "jdbc:mysql://mxm-testbi.c72srgqwk8ib.us-east-1.rds.amazonaws.com:3306/testbi"
table_users = "testbi.user"
table_views = "testbi.views"
user = "mxmtest"
password = "LDX6uq26J4JTqWzm"

# TABELLA USERS
users = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", table_users)\
  .option("user", user)\
  .option("password", password)\
  .load()

# TABELLA VIEWS
views = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", table_views)\
  .option("user", user)\
  .option("password", password)\
  .load()


# ALTRO
jdbcHostname = "mxm-testbi.c72srgqwk8ib.us-east-1.rds.amazonaws.com"
jdbcDatabase = "testbi"
jdbcPort = 3306
username = "mxmtest"
password = "LDX6uq26J4JTqWzm"
jdbcUrl = "jdbc:mariadb://{0}:{1}/{2}?user={3}&password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)

connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "org.mariadb.jdbc.Driver"
}

#pushdown_query = "(select * from user limit 100) emp_alias"
pushdown_query = "(select count(user_id) from user) conto"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

display(employees_table.select("age", "salary").groupBy("age").avg("salary"))