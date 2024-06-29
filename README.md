# Carpeta DAGs de Airflow

Esta carpeta contiene los Directed Acyclic Graphs (DAGs) utilizados para programar y automatizar flujos de trabajo en Apache Airflow. Cada DAG define un conjunto de tareas y sus dependencias para la extracción, transformación y carga (ETL) de datos, entre otros procesos automatizados.

## DAGs Disponibles

### Spotify ETL

Este DAG realiza un proceso ETL para extraer tus canciones reproducidas recientemente de Spotify, transformar los datos y cargarlos en una base de datos SQLite. Utiliza la API de Spotify para obtener los datos y requiere un token de autenticación válido.

### Spotify ETL con Token Refresh

Este DAG es una versión mejorada del anterior, que también maneja la actualización automática del token de acceso de Spotify utilizando un refresh token. Se programa para ejecutarse cada hora en el minuto 20 y asegura que el access token siempre esté actualizado antes de ejecutar el proceso ETL.

## Cómo Programar un DAG

Para programar y ejecutar un DAG en Apache Airflow:

1. Asegúrate de que Apache Airflow esté instalado y configurado correctamente en tu entorno.
2. Coloca el archivo del DAG (por ejemplo, `spotify_etl.py` o `new_spotify_dag.py`) en la carpeta `dags` de tu instalación de Airflow.
3. Inicia el servidor web de Airflow y el scheduler:

    ```sh
    airflow webserver -D
    airflow scheduler -D
    ```

4. Abre la interfaz web de Airflow accediendo a [http://localhost:8080](http://localhost:8080) en tu navegador.
5. Activa el DAG deseado desde la interfaz de usuario de Airflow.

## Requisitos

- Python 3.6 o superior.
- Bibliotecas de Python: `sqlite3`, `pandas`, `requests`.
- Un token de acceso válido de Spotify para autenticación.

## Notas Adicionales

- Asegúrate de revisar y cumplir con las directrices de la API de Spotify, especialmente en lo que respecta a la autenticación y la tasa de límites de solicitud.
- El DAG `Spotify ETL` está diseñado para ejecutarse una vez al día, extrayendo las canciones reproducidas en las últimas 24 horas.
- El DAG `Spotify ETL con Token Refresh` está diseñado para ejecutarse cada hora en el minuto 20, asegurando que el access token siempre esté actualizado.

