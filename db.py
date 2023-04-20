import mysql.connector
from dotenv import dotenv_values

config = dotenv_values(".env")

def connection():
    db = mysql.connector.connect(
        host = config["host"],
        user = config["user"],
        db = config["db"],
        passwd = config["passwd"]   
    )
    return db

def create_category_table():
    db = connection()
    mycursor = db.cursor()
    mycursor.execute("""CREATE TABLE categories (
        category_id INT NOT NULL AUTO_INCREMENT,
        category_name VARCHAR(50) NOT NULL,
        PRIMARY KEY (category_id)
    );""")
    
def insert_new_category(category):
    db = connection()
    mycursor = db.cursor()
    mycursor.execute(f"INSERT INTO categories (category_name) VALUES ('{category}');")
    
    db.commit()
    
def create_country_codes_table():
    db = connection()
    mycursor = db.cursor()
    mycursor.execute("""CREATE TABLE country_codes (
        id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
        country_code VARCHAR(6) NOT NULL UNIQUE,
        country_name VARCHAR(255)
    );""")
    
def insert_new_country(country):
    db = connection()
    mycursor = db.cursor()
    mycursor.execute(f"INSERT INTO country_codes (country_code) VALUES ('{country}');")
    
    db.commit()

def create_base_completeness_table():
    db = connection()
    mycursor = db.cursor()

    categories = fetch_all_categories()
    query = "CREATE TABLE completeness (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, country_code_id INT UNSIGNED NOT NULL,"
    
    for category in categories:
        query += f"{category}_state VARCHAR(255),"
        
    query += "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, FOREIGN KEY (country_code_id) REFERENCES country_codes(id));"
    
    mycursor.execute(query)
    
    trigger_to_save_history_of_completeness_before_update()
    
def create_completeness_history_table():
    db = connection()
    mycursor = db.cursor()
    
    categories = fetch_all_categories()
    query = "CREATE TABLE completeness_history (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, country_code VARCHAR(2) NOT NULL,"
    
    for category in categories:
        query += f"{category}_state VARCHAR(255),"
        
    query += "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, INDEX (country_code));"
    
    mycursor.execute(query)
    
def trigger_to_save_history_of_completeness_before_update():
    db = connection()
    mycursor = db.cursor()
    
    mycursor.execute("""
        CREATE TRIGGER completeness_update_trigger
        BEFORE UPDATE ON completeness
        FOR EACH ROW
        INSERT INTO completeness_history (country_code_id, demand_state, solar_state, wind_state)
        VALUES (OLD.country_code_id, OLD.demand_state, OLD.solar_state, OLD.wind_state
    );""")

    
def insert_country_completeness(country_code, states):
    db = connection()
    mycursor = db.cursor()
    
    categories = fetch_all_categories()
    categories_string = ", ".join(categories)

    values = []
    
    for category in categories:
        values.append(states[category])
        
    values_string = ", ".join(values)
    
    query = f"SELECT id FROM country_codes WHERE country_code = {country_code}"
    mycursor.execute(query)
    country_id = mycursor.fetchone()[0]
    
    sql = f"INSERT INTO completeness (country_code_id, {categories_string}) VALUES ({country_id}, {values_string})"
    
    mycursor.execute(sql)
    
    db.commit()
    print(mycursor.rowcount, "record inserted.")

def update_country_completeness(country_code_with_values):
    
    categories = fetch_all_categories()
    
    db = connection()
    mycursor = db.cursor()
    
    query = "UPDATE completeness SET "
    for country_code in country_code_with_values.keys():
        for category in categories:
            query += f"{category}_state = CASE "
            for country_code, value in country_code_with_values[country_code].items():
                query += f"WHEN country_code = '{country_code}' THEN '{value[category]}' "
            query += f"ELSE {category}_state END, "
        query += "updated_at = NOW() WHERE country_code IN ('" + "', '".join(country_code_with_values.keys()) + "')"
    
    mycursor.execute(query)
    
    db.commit()
    print(mycursor.rowcount, "record inserted.")
    
def create_datatype_entity_tables():
    db = connection()
    mycursor = db.cursor()
    
    mycursor.execute("""
        CREATE TABLE datatypes (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL UNIQUE
        );
    """)

    mycursor.execute("""
        CREATE TABLE entities (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL
        );
    """)

    mycursor.execute("""
        CREATE TABLE datatype_entities (
            id INT PRIMARY KEY AUTO_INCREMENT,
            datatype_id INT NOT NULL,
            entity_id INT NOT NULL,
            CONSTRAINT fk_datatype FOREIGN KEY (datatype_id) REFERENCES datatypes(id),
            CONSTRAINT fk_entity FOREIGN KEY (entity_id) REFERENCES entities(id)
        );
    """)

    mycursor.execute("""
        CREATE TABLE locations (
            id INT PRIMARY KEY AUTO_INCREMENT,
            country_code_id INT UNSIGNED NOT NULL,
            name VARCHAR(255) NOT NULL,
            FOREIGN KEY (country_code_id) REFERENCES country_codes(id)
        );
    """)

    mycursor.execute("""
        CREATE TABLE datatype_entity_locations (
            id INT PRIMARY KEY AUTO_INCREMENT,
            datatype_entity_id INT NOT NULL,
            location_id INT NOT NULL,
            CONSTRAINT fk_datatype_entity FOREIGN KEY (datatype_entity_id) REFERENCES datatype_entities(id),
            CONSTRAINT fk_location FOREIGN KEY (location_id) REFERENCES locations(id)
        );  
    """)
    
def fetch_all_categories():
    db = connection()
    mycursor = db.cursor()
    mycursor.execute("SELECT category_name FROM categories")
    categories = mycursor.fetchall()

    category_list = []
    for category in categories:
        category_list.append(category[0])

    db.close()
    
    return category_list

if __name__ == '__main__':
    countries = ['eu', 'al', 'at', 'ba', 'be', 'bg', 'ch', 'cz', 'de', 'dk', 'ee', 'es', 'fi', 'fr', 'gb', 'gr', 'hr', 'hu', 'isem', 'it', 'xk', 'lt', 'lv', 'me', 'mk', 'nl', 'no', 'pl', 'pt', 'ro', 'rs', 'se', 'si', 'sk']
    all_categories = ['demand', 'solar', 'wind']
    
    create_country_codes_table()
    create_category_table()
    
    for country in countries:
        insert_new_country(country)
    
    for category in all_categories:
        insert_new_category(category)
        
    create_base_completeness_table()
    create_completeness_history_table()
    create_datatype_entity_tables()