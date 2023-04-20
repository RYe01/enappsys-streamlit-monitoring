import mysql.connector

db = mysql.connector.connect(
    host = "145.14.156.52",
    user = "u375301929_monitoring",
    passwd = "2H:yK*swmfc"   
)

mycursor = db.cursor()

mycursor.execute("CREATE DATABASE completeness")