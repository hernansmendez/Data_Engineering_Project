import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
import os
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import timedelta, datetime
import requests
import smtplib
from email.mime.text import MIMEText
from airflow.models import Variable


# Argumentos por defecto para el DAG
default_args = {
    'owner': 'HernanM',
    'start_date': datetime(2023, 6, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='ETL_Airbnb-Docker_Threshold',
    default_args=default_args,
    description='Agrega data de Airbnb de forma diaria',
    schedule_interval="@daily",
    catchup=False
)

# Variables de Airflow
threshold_success = Variable.get("threshold_exitoso", default_var=5)
threshold_failure = Variable.get("threshold_fallido", default_var=0)


def extract(ti):
    try:
        # Obtener la ruta actual de trabajo
        current_dir = os.getcwd()

        # Ruta del archivo de texto que contiene el token de acceso personal
        ruta_token = os.path.join(current_dir, "token.txt")

        # Leer el token de acceso personal desde el archivo
        with open(ruta_token, "r") as file:
            token = file.read().strip()

        # Detalles de autenticación
        headers = {"Authorization": f"Bearer {token}"}

        # Detalles del repositorio y archivo
        usuario = "hernansmendez"
        repositorio = "airflow"
        nombre_archivo = "Berlin.json"

        # URL del archivo RAW en el repositorio
        url_raw = f"https://raw.githubusercontent.com/{usuario}/{repositorio}/main/{nombre_archivo}"

        # Realizar solicitud GET para descargar el archivo
        response = requests.get(url_raw, headers=headers)

        # Verificar si la solicitud fue exitosa (código de estado 200)
        if response.status_code == 200:
            # Obtener el contenido del archivo JSON
            contenido_archivo = response.json()

            # Crear el DataFrame utilizando pd.json_normalize()
            df = pd.json_normalize(contenido_archivo, record_path=['airbnbHotels'])

            # Guardar el DataFrame como un archivo CSV
            csv_path = f"{current_dir}/extracted_data.csv"
            df.to_csv(csv_path, index=False)

            # Pasar el path del CSV a la siguiente tarea utilizando XCom
            ti.xcom_push(key='csv_path', value=csv_path)

    except Exception as e:
        # Enviar correo electrónico de falla
        send_mail_failure()
        raise


def transform(ti):
    try:
        # Obtener el path del CSV desde XCom
        csv_path = ti.xcom_pull(key='csv_path')

        # Leer el CSV como un DataFrame
        df = pd.read_csv(csv_path)

        # Crear la columna "room_id" utilizando el índice actual
        df['room_id'] = df.index
        # Extrae la columna "room_id"
        room_id_column = df['room_id']
        # Elimina la columna "room_id" del DataFrame
        df.drop('room_id', axis=1, inplace=True)
        # Inserta la columna "room_id" al principio del DataFrame
        df.insert(0, 'room_id', room_id_column)

        import ast

        def get_values(x):
            try:
                x = ast.literal_eval(x)  # Convierte la cadena en una lista
                if isinstance(x, list) and len(x) >= 3:
                    return pd.Series(x[:3])
            except (ValueError, SyntaxError):
                pass

            return pd.Series([None, None, None])

        df[['desc', 'bed', 'check_in']] = df['subtitles'].apply(get_values)
        
          
        # Elimino la columna de subtítulos original
        df.drop('subtitles', axis=1, inplace=True)

        # Renombro columna 'Price.value'
        df = df.rename(columns={'price.value': 'price($)'})
        # Borro columna 'price.currency'
        df = df.drop('price.currency', axis=1)

        # Divido columna "check-in" por delimitador "–" y obtengo el primer elemento de la lista
        df['check_in'] = df['check_in'].str.split('–').str[0]
        # Convierto columna "check-in" en datetime type
        df['check_in'] = pd.to_datetime(df['check_in'] + ' 2023')
        # Extrae la columna "check_in"
        check_in_column = df['check_in']
        # Elimina la columna "room_id" del DataFrame
        df.drop('check_in', axis=1, inplace=True)
        # Inserta la columna "room_id" al principio del DataFrame
        df.insert(1, 'check_in', check_in_column)

        # Reemplazo valor "No rating" en columna "Rating" por valor nulo
        df["rating"].replace("No rating", np.nan, inplace=True)

        # Para lidiar con valores mal cargados y poder dividir la columna bed en 2 columnas, "number of beds" y "bed type"
        # defino función lambda para comprobar si el valor comienza con un entero
        def comienza_con_entero(valor):
            try:
                int(valor[0])
                return True
            except:
                return False

        # Aplico función lambda a la columna y reemplazo los valores que no comienzan con un entero por un nulo
        df['bed'] = df['bed'].apply(lambda x: x if comienza_con_entero(x) else np.nan)
        # Divido la columna 'bed' en dos columnas
        df[['number of beds', 'bed_type']] = df['bed'].str.split(' ', 1, expand=True)
        # Extraigo el número y el tipo de cama si el valor no es nulo
        if not pd.isnull(df['number of beds']).all():
            df['number of beds'] = pd.to_numeric(df['number of beds'], errors='coerce').astype('Int64')
            df['bed_type'] = np.where(pd.isnull(df['number of beds']), np.nan, df['bed_type'].str.replace('beds', '').str.strip())
        else:
            df['number of beds'] = np.nan
            df['bed_type'] = np.nan    
        # Elimino la columna bed
        df = df.drop('bed', axis=1)

        # Elimino las filas con valores nulos en la columna 'check_in' ya que es una de las columnas de la clave primaria compuesta
        df.dropna(subset=['check_in'], inplace=True)

        # Resetear el índice
        df.reset_index(inplace=True)
        # Asignar los valores del índice a la columna 'room_id'
        df['room_id'] = df.index
        # Eliminar la columna 'index'
        df.drop('index', axis=1, inplace=True)
        
        
        # Obtener la ruta actual de trabajo
        current_dir = os.getcwd()
        # Guardar el DataFrame transformado como un nuevo archivo CSV
        transformed_csv_path = f"{current_dir}/transformed_data.csv"
        df.to_csv(transformed_csv_path, index=False)

        # Pasar el path del CSV transformado a la siguiente tarea utilizando XCom
        ti.xcom_push(key='transformed_csv_path', value=transformed_csv_path)

    except Exception as e:
        # Enviar correo electrónico de falla
        send_mail_failure()
        raise


def load(ti):
    try:
        
        
        
        # Obtener el path del CSV transformado desde XCom
        transformed_csv_path = ti.xcom_pull(key='transformed_csv_path')

        # Leer el CSV transformado como un DataFrame
        df = pd.read_csv(transformed_csv_path)
        
        # Obtener la ruta actual de trabajo
        current_dir = os.getcwd()
        # Leer las credenciales de conexión desde un archivo
        with open(f'{current_dir}/pwd_coder.txt', "r") as f:
            lines = f.readlines()

        if len(lines) >= 4:
            host = lines[0].strip()
            data_base = lines[1].strip()
            user = lines[2].strip()
            pwd = lines[3].strip()

        # Conexión a Redshift
        try:
            conn = psycopg2.connect(
                host=host,
                dbname=data_base,
                user=user,
                password=pwd,
                port='5439'
            )
            print("Conexión a Redshift exitosa!")
        except Exception as e:
            print("No es posible conectarse a Redshift.")
            print(e)


        # Función para ejecutar Querys en Python
        def execute_read_query(connection, query):
            cur = connection.cursor()
            result = None
            try:
                cur.execute(query)
                result = cur.fetchall()
                return result
            except e as e:
                print(f"Error '{e}' ha ocurrido")


        # Query que se quiere ejecutar dentro de Python      
        query = """SELECT * FROM airbnb"""

        # Verifico si fue creada la tabla Airbnb en redshift
        print('Verificando si fue creada la tabla en Redshift...')
        cursor = conn.cursor()
        cursor.execute(query)
        columnas = [description[0] for description in cursor.description]
        cursor.fetchall()
        print(pd.DataFrame(execute_read_query(conn, query),columns=columnas))

        # Consulto si existen registros cargados con anterioridad, creo un dataframe con room_id y check_in

        query2 = """SELECT room_id, check_in FROM airbnb"""
        cursor = conn.cursor()
        cursor.execute(query2)
        columnas = [description[0] for description in cursor.description]
        cursor.fetchall()
        df2 = pd.DataFrame(execute_read_query(conn, query2),columns=columnas)


        # Creo un dataframe df3 de pandas con las filas cuyas claves compuestas no se encuentren cargadas en redshift
        df3 = pd.DataFrame()
        # Convertir el tipo de datos de la columna "check_in" en df2 a datetime
        df["check_in"] = pd.to_datetime(df["check_in"])
        df2["check_in"] = pd.to_datetime(df2["check_in"])
        # Combinar los DataFrames df y df2 en base a las columnas "room_id" y "check_in"
        df3 = pd.merge(df, df2, on=["room_id", "check_in"], how="outer", indicator=True)

        # Seleccionar solo las filas que no coinciden
        df3 = df3[df3["_merge"] == "left_only"]
        # Eliminar la columna "_merge"
        df3 = df3.drop(columns="_merge")


        # Cargo sólo los datos del dataframe df3

        cur = conn.cursor()
        # Defino el nombre de la tabla
        table_name = 'airbnb'
        # Defino las columnas para insertar los insertar datos
        columns = ['room_id','check_in','thumbnail', 'title', 'rating','link','price','period','description', 'number_of_beds', 'bed_type']
        values = [tuple(None if pd.isna(i) else i for i in x) for x in df3.to_numpy()]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"


        print('Cargando datos...')

        # Imprimo room_id y check_in de aquellas filas que no pudieron ser cargadas
        df4 = pd.merge(df, df2, on=["room_id", "check_in"], how="inner")
        for index, row in df4.iterrows():
            room_id = row['room_id']
            check_in = row['check_in']
            print(f"No se puede cargar el room_id: {room_id}, check_in: {check_in}. Ya existe un registro con la misma clave compuesta")  


        # Ejecuto la sentencia INSERT usando execute_values
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT") # Guardo los cambios
        print(f"{len(df3.index)} nuevas filas fueron cargadas a la tabla {table_name}")


        # Verifico si los datos fueron cargados
        print('Verificando si los datos fueron cargados correctamente...')
        cursor = conn.cursor()
        cursor.execute(query)
        columnas = [description[0] for description in cursor.description]
        cursor.fetchall()
        dff = pd.DataFrame(execute_read_query(conn, query),columns=columnas)
        print(dff.shape)
        
        
        dataframe_info = (len(dff.index), len(dff.columns), dff.columns.tolist())

        ti.xcom_push(key='dataframe_info', value=dataframe_info)


        #Cierro conexión
        cur.close()
        conn.close()
        print('Conexión a Redshift cerrada.')


        # Determinar qué tarea se debe ejecutar según el tamaño del DataFrame
        if len(dff) >= threshold_success:
            return 'success'
        elif len(dff) == threshold_failure:
            return 'failure'

    except Exception as e:
        # Enviar correo electrónico de falla
        send_mail_failure()
        raise


def send_mail_success(ti):
    subject = 'El DAG de ETL_Airbnb se ha completado exitosamente.'

    # Obtener la información del DataFrame desde XCom
    dataframe_info = ti.xcom_pull(task_ids='Load', key='dataframe_info')

    # Crear el cuerpo del correo electrónico con la información del DataFrame
    body_text = f'Tarea de ETL_Airbnb completada exitosamente.\n\nCantidad de filas: {dataframe_info[0]}\nCantidad de columnas: {dataframe_info[1]}\n\nNombres de las columnas:\n{dataframe_info[2]}\n\n{dataframe_info[0]} filas y {dataframe_info[1]} columnas han sido cargadas de manera exitosa en la tabla Airbnb en Redshift.'

    enviar_mail(subject, body_text)


def send_mail_failure():
    subject = 'Error en la ejecución del DAG de ETL_Airbnb.'
    body_text = 'La tarea de ETL_Airbnb ha fallado al ejecutarse.'
    enviar_mail(subject, body_text)


def enviar_mail(subject, body_text):
    try:
        # Obtener la ruta actual del contenedor
        current_dir = os.getcwd()

        # Ruta del archivo de texto que contiene la contraseña
        password_file = os.path.join(current_dir, "pass_mail.txt")

        # Leer la contraseña desde el archivo
        with open(password_file, "r") as file:
            password = file.read().strip()

        # Resto del código de envío del correo
        msg = MIMEText(body_text)
        msg['Subject'] = subject
        msg['From'] = 'hernansmendez84@gmail.com'
        msg['To'] = 'hernansmendez84@gmail.com'

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login('hernansmendez84@gmail.com', password)
        server.send_message(msg)
        server.quit()

        print('Mail enviado a la casilla hernansmendez84@gmail.com')
    except Exception as exception:
        print(exception)
        raise


def decide_next_task(ti):
    log = ti.xcom_pull(task_ids='Load')
    if log == 'success':
        return 'Send_Mail_Success'
    else:
        return 'Send_Mail_Failure'


# Tareas
extract_task = PythonOperator(
    task_id='Extract',
    python_callable=extract,
    provide_context=True,
    dag=BC_dag,
)

transform_task = PythonOperator(
    task_id='Transform',
    python_callable=transform,
    provide_context=True,
    dag=BC_dag,
)

load_task = PythonOperator(
    task_id='Load',
    python_callable=load,
    provide_context=True,
    dag=BC_dag,
)

send_mail_success_task = PythonOperator(
    task_id='Send_Mail_Success',
    python_callable=send_mail_success,
    provide_context=True,
    dag=BC_dag,
)

send_mail_failure_task = PythonOperator(
    task_id='Send_Mail_Failure',
    python_callable=send_mail_failure,
    provide_context=True,
    dag=BC_dag,
)

next_task = BranchPythonOperator(
    task_id='Next_Task',
    python_callable=decide_next_task,
    provide_context=True,
    dag=BC_dag,
)

# Definir el orden de ejecución de las tareas
extract_task >> transform_task >> load_task >> next_task >> [send_mail_success_task, send_mail_failure_task]