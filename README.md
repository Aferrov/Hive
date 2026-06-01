# Procesamiento Distribuido y Análisis Analítico con Hadoop y Hive en Amazon EMR

Este repositorio contiene las implementaciones prácticas desarrolladas en el laboratorio para la comparación de rendimiento entre el enfoque procedimental (**Hadoop MapReduce**) y el enfoque declarativo (**Apache Hive**). Adicionalmente, se incluye el pipeline analítico implementado para procesar el dataset masivo de viajes de taxi de Nueva York (**NYC Taxi Trips**).

---

## Descripción del Proyecto

El trabajo se ejecutó de forma controlada sobre un único clúster administrado en la nube de AWS (**Amazon EMR**), bajo el identificador de clúster `j-2QCK2QFWR1LCL`. El proyecto se divide en tres fases principales de ingeniería de datos:

1. **WordCount (Conteo de palabras):** Evaluación de tiempos y uso de recursos al comparar scripts de Python Streaming frente a consultas optimizadas en HiveQL.
2. **Índice Invertido:** Mapeo distributivo de términos únicos asociados a sus documentos de origen HDFS, analizando el impacto del peso de las cadenas en la fase de mezcla (*Shuffle Stage*).
3. **Análisis de NYC Taxi Trips:** Procesamiento de más de 3.5 millones de registros en formato Apache Parquet utilizando técnicas de particionamiento dinámico por año y mes.

---

## Estructura de Archivos del Repositorio

* **`wordcount_mapper.py` / `wordcount_reducer.py`:** Scripts procedimentales en Python utilizados para la línea base de conteo de frecuencias mediante Hadoop Streaming.
* **`indice_mapper.py` / `indice_reducer.py`:** Scripts en Python que capturan la variable de entorno `map_input_file` para construir el índice inverso de documentos.
* **`enviar_job_hive.py`:** Script de automatización en Python que utiliza la librería `boto3` para inyectar y monitorear la consulta de WordCount en Hive.
* **`enviar_indice_invertido_hive.py`:** Orquestador en Python para la creación del índice invertido en Hive aprovechando la columna virtual `INPUT__FILE__NAME`.
* **`enviar_job_taxi.py`:** Pipeline analítico completo en Python y HiveQL para aplicar particionado dinámico, ingeniería de atributos en marcas de tiempo y extracción de KPIs del dataset de taxis.

---

## Tecnologías y Herramientas Utilizadas

* **Lenguajes:** Python 3, SQL / HiveQL.
* **Infraestructura Cloud:** Amazon Web Services (AWS), Amazon EMR v6.x, Amazon S3.
* **Ecosistema Hadoop:** HDFS, YARN (ResourceManager).
* **Formatos de Almacenamiento:** Archivos de texto plano (`TEXTFILE`), Apache Parquet (`PARQUET`).

---

## Resumen de Resultados Analíticos (NYC Taxi)

El procesamiento del dataset masivo de taxis en el clúster arrojó las siguientes métricas de negocio:
* **Volumen total:** 3,561,815 viajes procesados en el mes de análisis.
* **Ingreso total:** \$105,511,145.72 USD acumulados.
* **Tarifa promedio:** \$29.62 USD por viaje, con una distancia media de 6.71 millas.
* **Ventanas pico:** Las horas de mayor demanda en la ciudad de Nueva York se concentraron a las 18:00 (6:00 PM) con 232,859 viajes y a las 17:00 (5:00 PM) con 224,210 viajes.

---

## Modo de Ejecución

1. **Carga de recursos:** Alojar los scripts de procesamiento y los datos base dentro del bucket configurado en Amazon S3 (`s3://cluster-arleen/`).
2. **Conexión al clúster:** Asegurar que el clúster EMR se encuentre activo y con el ID correspondiente en los scripts.
3. **Despacho de tareas:** Ejecutar de forma local cualquiera de los scripts orquestadores para enviar los pasos (*Steps*) automáticamente al nodo maestro del clúster:
   ```bash
   python enviar_job_taxi.py
   ```
4. **Recuperación:** Los resultados analíticos procesados serán depositados en el HDFS del almacén de Hive y respaldados de manera automática en las rutas designadas de Amazon S3.
