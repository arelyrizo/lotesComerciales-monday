import json
import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Definir la api_key
api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjM3MDQyMzg5MCwiYWFpIjoxMSwidWlkIjo2MjAxOTMxMCwiaWFkIjoiMjAyNC0wNi0xMVQwMDoxMjo0MS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTU0MzkwNjcsInJnbiI6InVzZTEifQ.66tqiACqx06_MQ2vJXHWslItMyb5MQeQUTQCQ0vqZrw'

# URL de la API de Monday.com
url = 'https://api.monday.com/v2'

# ID de la tabla de Monday.com
board_id = 4199313956

# Nombre de la Tabla
table_name = 'lotescomerciales150724'

try:
    # Conexión a la base de datos MySQL en Azure
    cnx = mysql.connector.connect(
        host='monday-comercial.mysql.database.azure.com',
        user='FOM',
        password='Admin5678',
        database='hf_monday'
    )

    if cnx.is_connected():
        cursor = cnx.cursor()

        # Eliminar la tabla si existe
        drop_table_query = f"""DROP TABLE IF EXISTS {table_name}"""
        cursor.execute(drop_table_query)
        print(f"Tabla {table_name} eliminada si existía")

        # Función para obtener los registros de la tabla y escribirlos en la base de datos MySQL
        def fetch_items_and_write_to_db(api_key, board_id, url):
            headers = {'Authorization': api_key,
                       'Content-Type': 'application/json'}
            cursor_position = None
            columns_set = set()
            while True:
                cursor_param = f'cursor: "{
                    cursor_position}"' if cursor_position else ''
                query = f'''
                {{
                    boards(ids: {board_id}) {{
                        items_page(limit: 1, {cursor_param}) {{
                            cursor
                            items {{
                                id
                                name
                                column_values {{
                                    column {{
                                        title
                                    }}
                                    text
                                }}
                            }}
                        }}
                    }}
                }}
                '''
                response = requests.post(
                    url, headers=headers, json={'query': query})
                data = response.json()
                if 'data' in data and 'boards' in data['data'] and len(data['data']['boards']) > 0:
                    items_page = data['data']['boards'][0]['items_page']
                    items = items_page['items']
                    next_cursor = items_page['cursor']
                    for item in items:
                        columns_set.update([col['column']['title']
                                            for col in item.get('column_values', [])
                                            if col['column']['title'] != "id formula"])
                    if not next_cursor:
                        break
                    cursor_position = next_cursor
                else:
                    print("No hay datos disponibles o ID de tabla no válido")
                    break
                break

            # Crear la tabla con las columnas detectadas
            columns_list = list(columns_set)
            columns_definition = ", ".join(
                [f"`{col}` TEXT" for col in columns_list])
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                id_formula VARCHAR(255),
                {columns_definition}
            )
            """
            cursor.execute(create_table_query)

            # Reset cursor position for data fetching
            cursor_position = None
            while True:
                cursor_param = f'cursor: "{
                    cursor_position}"' if cursor_position else ''
                query = f'''
                {{
                    boards(ids: {board_id}) {{
                        items_page(limit: 500, {cursor_param}) {{
                            cursor
                            items {{
                                id
                                name
                                column_values {{
                                    column {{
                                        title
                                    }}
                                    text
                                }}
                            }}
                        }}
                    }}
                }}
                '''
                response = requests.post(
                    url, headers=headers, json={'query': query})
                data = response.json()
                if 'data' in data and 'boards' in data['data'] and len(data['data']['boards']) > 0:
                    items_page = data['data']['boards'][0]['items_page']
                    items = items_page['items']
                    next_cursor = items_page['cursor']
                    for item in items:
                        item_id = item['id']
                        item_name = item['name']
                        column_values = item.get('column_values', [])
                        column_values_dict = {
                            column['column']['title']: column['text'] for column in column_values}

                        # Concatenar los valores para 'id formula'
                        id_formula = (
                            column_values_dict.get('Codigo Fracc', '') +
                            column_values_dict.get('MZA', '') +
                            column_values_dict.get('LOTE', '') +
                            column_values_dict.get('LETRA ALFABETICA', '')
                        )

                        # Preparar datos para la inserción
                        columns = ", ".join(
                            ["id", "name", "id_formula"] + [f"`{col}`" for col in columns_list])
                        placeholders = ", ".join(
                            ["%s"] * (len(columns_list) + 3))
                        values = [
                            item_id, item_name, id_formula] + [column_values_dict.get(col, '') for col in columns_list]

                        # Insertar datos en la base de datos con manejo de duplicados
                        insert_query = f"""
                        INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
                        ON DUPLICATE KEY UPDATE
                        name=VALUES(name),
                        id_formula=VALUES(id_formula),
                        {", ".join(
                            [f"`{col}`=VALUES(`{col}`)" for col in columns_list])}
                        """
                        cursor.execute(insert_query, values)
                        cnx.commit()

                    if not next_cursor:
                        break
                    cursor_position = next_cursor
                else:
                    print("No hay datos disponibles o ID de tabla no válido")
                    break

        # Llamar a la función fetch_items_and_write_to_db() con la api_key proporcionada
        fetch_items_and_write_to_db(api_key, board_id, url)

        # Cerrar la conexión a la base de datos
        cursor.close()
    else:
        print("Error: No se pudo conectar a la base de datos.")
except Error as e:
    print(f"Error: {e}")
finally:
    if cnx.is_connected():
        cnx.close()
