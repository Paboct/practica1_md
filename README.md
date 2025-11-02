# Proyecto de Minería de Datos: Análisis Batch y Streaming del IBEX 35

## Descripción general
Este proyecto implementa un sistema completo de **procesamiento, análisis y visualización de datos financieros** del IBEX 35, siguiendo una arquitectura **MVC (Modelo–Vista–Controlador)**.  

Permite:
- Descargar datos históricos mediante la API de **Yahoo Finance**.
- Procesar datos en **streaming** recibidos a través de **sockets TCP**.
- Almacenar la información en formato **Parquet**.
- Calcular métricas y efectos financieros relevantes (efecto viernes, estacionalidad, relación precio–volumen, comportamiento de gaps, etc.).
- Generar visualizaciones profesionales usando **matplotlib** y **seaborn**.

---

## Estructura del proyecto

project/
│
├── data/ # Datos procesados y almacenados
│ ├── ibex/ # Datos históricos (batch)
│ └── ibex_stream/ # Datos de streaming (microlotes)
│
├── tools/
│ └── datos.py # Script emisor de datos en tiempo real (socket)
│
├── src/
│ ├── main.py # Punto de entrada de la aplicación
│ │
│ ├── config/
│ │ └── settings.py # Variables de configuración y rutas globales
│ │
│ ├── model/
│ │ ├── sparkSession.py # SparkSessionSingleton
│ │ │
│ │ ├── datasources/ # Ingesta de datos
│ │ │ ├── yfinanceClient.py # Descarga datos históricos
│ │ │ └── streamingClient.py # Recepción de datos en tiempo real (socket)
│ │ │
│ │ ├── persistence/ # Gestión de almacenamiento
│ │ │ ├── parquet_store.py # Lectura/escritura de archivos Parquet
│ │ │ └── schema_definition.py # Esquemas batch y streaming
│ │ │
│ │ └── logic/ # Lógica de negocio e ingeniería de variables
│ │ ├── cleaners.py # Limpieza y validación de datos históricos
│ │ ├── additioners.py # Cálculo de nuevas columnas (OpenGap, GapBehaviour…)
│ │ ├── auxiliars.py # Funciones auxiliares (retornos, rolling means…)
│ │ └── data_plotting.py # Factoría de gráficos (PlottingFactory)
│ │
│ ├── controller/ # Controladores de la aplicación
│ │ ├── btchController.py # Lógica de flujo batch (descarga, limpieza, guardado)
│ │ ├── strmController.py # Lógica de flujo streaming (socket, microlotes)
│ │ └── pltController.py # Lógica de visualización
│ │
│ └── view/ # Capas de presentación
│ ├── console_view.py # Salida textual en consola
│ └── plot_view.py # Muestra o guarda los gráficos


---

## Arquitectura MVC

El sistema sigue el patrón **Modelo–Vista–Controlador**:

- **Modelo (`model/`)**  
  Gestiona la **lógica de negocio**, el acceso a datos y las transformaciones:  
  - Obtención (batch o streaming)  
  - Limpieza y validación  
  - Cálculo de métricas financieras (CloseGap, OpenGap, Perc_Variation, RollingMean, etc.)  
  - Definición y generación de figuras (`PlottingFactory`)

- **Vista (`view/`)**  
  Encargada exclusivamente de la **presentación**:  
  - `console_view` imprime resultados de consola.  
  - `plot_view` muestra o guarda los gráficos en `/data/graphs/<ticker>/`.

- **Controladores (`controller/`)**  
  Son los **coordinadores** entre modelo y vista:  
  - `btchController`: flujo batch completo.  
  - `strmController`: gestión y análisis en streaming.  
  - `pltController`: generación de gráficos (Efecto Viernes, Estacionalidad, Volumen, Streaming, Gaps).

---

## Requisitos

- Python 3.10 o superior  
- Apache Spark  
- Librerías principales:
    - pandas
    - pyspark
    - yfinance
    - matplotlib
    - seaborn


## Autor
Nombre: Pablo Ruíz Morán
Universidad: Universidad de León
Grado: Ingeniería de Datos e Inteligencia Artificial
Asignatura: Minería de Datos
Curso: 2025-2026