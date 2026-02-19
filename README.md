# BigDadiks2 -- Crypto Data Pipeline

Pipeline end-to-end que ingesta datos de criptomonedas desde TradingView, los transforma y calcula indicadores tecnicos (SMA, EMA, RSI, MACD), todo orquestado con AWS Glue sobre un data lake en S3.

```
TradingView --> Bronze (CSV) --> Silver (Parquet) --> Gold (Parquet + KPIs)
```

---

## Que hace

1. **Descarga** datos historicos OHLCV (Open, High, Low, Close, Volume) de criptomonedas desde TradingView
2. **Almacena** los datos crudos en S3 como CSV particionado por asset/year/month (capa Bronze)
3. **Limpia y normaliza** los datos: tipos correctos, columnas estandarizadas, formato Parquet (capa Silver)
4. **Calcula indicadores tecnicos** sobre los datos limpios (capa Gold):
   - **SMA 200** -- Media movil simple de 200 periodos
   - **EMA 50** -- Media movil exponencial de 50 periodos
   - **RSI 14** -- Indice de fuerza relativa
   - **MACD** -- Linea MACD, senal e histograma
5. **Cataloga** cada capa con Glue Crawlers para que sea consultable desde Athena

---

## Requisitos previos

- **Python 3.10+**
- **Cuenta AWS** con acceso a S3 y Glue (AWS Academy funciona)
- **Credenciales AWS** configuradas (env vars o `~/.aws/credentials`)
- **Dos IAM Roles** creados en AWS:
  - Uno para los Glue Jobs (necesita acceso a S3 y Glue)
  - Uno para los Glue Crawlers (necesita acceso a S3 y al Glue Catalog)

### Dependencias Python

```bash
pip install boto3 pandas
```

La libreria `TradingviewData` debe estar disponible (incluida en el directorio del proyecto).

---

## Configuracion

### 1. Variables de entorno (obligatorias)

Antes de ejecutar, exporta los ARNs de los roles de IAM:

```bash
# Linux / macOS
export GLUE_JOB_ROLE_ARN="arn:aws:iam::123456789:role/TuRolGlueJob"
export GLUE_CRAWLER_ROLE_ARN="arn:aws:iam::123456789:role/TuRolGlueCrawler"

# Windows (PowerShell)
$env:GLUE_JOB_ROLE_ARN = "arn:aws:iam::123456789:role/TuRolGlueJob"
$env:GLUE_CRAWLER_ROLE_ARN = "arn:aws:iam::123456789:role/TuRolGlueCrawler"
```

Tambien necesitas las credenciales de AWS (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` si usas AWS Academy).

### 2. Configuracion del pipeline

Toda la configuracion esta centralizada en `src/constants.py`:

| Variable | Valor por defecto | Que controla |
|----------|-------------------|--------------|
| `REGION` | `eu-south-2` | Region de AWS |
| `BUCKET` | `trade-data-big-daddyks-main` | Bucket S3 del data lake |
| `GLUE_DB` | `trade_data_imat3a05` | Base de datos en Glue Catalog |
| `DEFAULT_ASSET` | `SOLUSD` | Activo por defecto si no se detecta |

Para cambiar los activos a descargar, edita la lista `ASSETS` en `src/complete.py`:

```python
ASSETS: List[str] = ["SOLUSD"]  # Anade mas: ["SOLUSD", "BTCUSD", "ETHUSD"]
```

---

## Ejecucion

### Un solo comando

```bash
cd src
python run_all.py
```

Esto ejecuta todo el pipeline de forma automatica:

```
 1. Crea el bucket S3 si no existe
 2. Sube los scripts de Glue a S3
 3. Descarga datos de TradingView y los sube a Bronze
 4. Lanza el pipeline de transformacion en Glue:
      Crawler Bronze
        --> Job: Bronze a Silver
          --> Crawler Silver
            --> Job: Silver a Gold (KPIs)
              --> Crawler Gold
```

### Ejecutar pasos por separado

Si prefieres ejecutar cada fase de forma independiente:

```bash
# Solo la ingesta (descarga datos y sube a Bronze)
python complete.py

# Solo el pipeline de transformacion (Bronze -> Silver -> Gold)
python pipeline_launcher.py
```

### Tiempos estimados

| Paso | Duracion aproximada |
|------|-------------------|
| Ingesta (complete.py) | 1-2 min |
| Crawler Bronze | 2-3 min |
| Bronze -> Silver | 3-5 min |
| Crawler Silver | 2-3 min |
| Silver -> Gold | 3-5 min |
| Crawler Gold | 2-3 min |
| **Total** | **~15-20 min** |

---

## Estructura del proyecto

```
src/
  constants.py               Configuracion central (bucket, region, nombres de jobs/crawlers)
  run_all.py         Runner principal: ejecuta todo el pipeline con un comando
  complete.py                 Ingesta: descarga datos de TradingView y sube CSVs a Bronze
  pipeline_launcher.py        Orquestador: crea jobs/crawlers en Glue y ejecuta el pipeline
  job_bronze_to_silver.py     [Glue Job] Transforma Bronze CSV -> Silver Parquet
  job_silver_to_gold_kpis.py  [Glue Job] Calcula KPIs sobre Silver -> Gold Parquet
  job_run_crawler.py          [Glue Job] Ejecuta un crawler desde dentro de Glue
  deletion_buckets.py         Utilidad: borra buckets S3 del proyecto
  deletion_crawler.py         Utilidad: borra crawlers y base de datos de Glue
```

### Que se ejecuta donde

| Archivo | Donde corre | Descripcion |
|---------|-------------|-------------|
| `run_all.py` | Tu maquina | Orquesta todo |
| `complete.py` | Tu maquina | Descarga de TradingView + upload a S3 |
| `pipeline_launcher.py` | Tu maquina | Crea recursos en Glue y lanza el pipeline |
| `job_bronze_to_silver.py` | AWS Glue (Spark) | Se sube a S3 y lo ejecuta Glue |
| `job_silver_to_gold_kpis.py` | AWS Glue (Spark) | Se sube a S3 y lo ejecuta Glue |
| `job_run_crawler.py` | AWS Glue (Spark) | Se sube a S3 y lo ejecuta Glue |

---

## Estructura del Data Lake en S3

```
s3://trade-data-big-daddyks-main/
  bronze/
    asset=SOLUSD/
      year=2024/
        month=01/
          data.csv
        month=02/
          data.csv
        ...
  silver/
    asset=SOLUSD/
      year=2024/
        month=01/
          part-00000-xxxx.snappy.parquet
        ...
  gold/
    asset=SOLUSD/
      year=2024/
        month=01/
          part-00000-xxxx.snappy.parquet   (incluye sma_200, ema_50, rsi_14, macd, ...)
        ...
  scripts/
    job_bronze_to_silver.py
    job_silver_to_gold_kpis.py
    job_run_crawler.py
    pipeline_launcher.py
```

---

## Consultar los datos

Una vez que el pipeline termina, los datos quedan catalogados en el **Glue Data Catalog** y se pueden consultar desde **Amazon Athena**:

```sql
-- Ver datos limpios (Silver)
SELECT * FROM trade_data_imat3a05.lake_silver
WHERE asset = 'SOLUSD' AND year = 2025
LIMIT 10;

-- Ver datos con indicadores (Gold)
SELECT datetime, close, sma_200, ema_50, rsi_14, macd
FROM trade_data_imat3a05.lake_gold
WHERE asset = 'SOLUSD'
ORDER BY datetime DESC
LIMIT 20;
```

---

## Limpieza de recursos

Para eliminar los recursos creados en AWS:

```bash
# Borrar buckets S3 (revisa primero con dry_run=True)
python deletion_buckets.py

# Borrar crawlers y base de datos de Glue
python deletion_crawler.py
```

---

## Problemas frecuentes

### "Session token expired" o errores de credenciales

Las credenciales de AWS Academy expiran cada ~1 hora. Refresca tus credenciales y vuelve a exportar `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` y `AWS_SESSION_TOKEN`.

### "GLUE_JOB_ROLE_ARN is empty"

Asegurate de haber exportado las variables de entorno con los ARNs de los roles antes de ejecutar.

### El job de Glue falla con "Partition does not match table schema"

Esto ocurre si un solo crawler apunta a multiples capas (bronze + silver). El pipeline ya usa un crawler separado por capa, asi que no deberia pasar. Si ocurre, borra los crawlers con `deletion_crawler.py` y vuelve a ejecutar.

### TradingView no devuelve datos

- Comprueba tu conexion a internet
- El simbolo puede no estar disponible en el exchange `BINANCE`
- TradingView puede limitar las peticiones si se hacen muchas seguidas

---

## Stack tecnologico

| Componente | Tecnologia |
|------------|-----------|
| Ingesta de datos | TradingView API (Python) |
| Almacenamiento | Amazon S3 |
| Procesamiento | AWS Glue 4.0 (Apache Spark 3.3) |
| Catalogo | AWS Glue Data Catalog + Crawlers |
| Consultas | Amazon Athena |
| Formato de datos | CSV (Bronze), Apache Parquet con Snappy (Silver/Gold) |
| Lenguaje | Python 3.10 |
