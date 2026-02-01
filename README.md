# TradeData – Sprint 1

Arquitectura Big Data para datos de trading de criptomonedas

## 📌 Descripción del proyecto

Este proyecto forma parte de la asignatura **Tecnologías de Procesamiento Big Data** del **3º Grado en Ingeniería Matemática e Inteligencia Artificial**.
En este **Sprint 1** se diseña e implementa la base de un **Data Lake en AWS** para la ingesta y almacenamiento de datos históricos de trading de criptomonedas.

El objetivo principal es obtener datos históricos diarios desde **TradingView (BINANCE)**, almacenarlos de forma eficiente en **Amazon S3** y preparar el sistema para futuros procesos de análisis y procesamiento Big Data.

---

## 🎯 Objetivos del Sprint 1

* Descargar un histórico de ~4 años de datos diarios de mercado.
* Diseñar una arquitectura de almacenamiento escalable en Amazon S3.
* Implementar una ingesta **incremental** e **idempotente**.
* Aplicar políticas de **retención automática** de datos.
* Realizar un **Análisis Exploratorio de Datos (EDA)** para validar la calidad del dataset.

---

## 🧱 Arquitectura

* **Proveedor Cloud**: AWS
* **Servicio principal**: Amazon S3
* **Región**: `eu-south-2` (España)
* **Estrategia**: Bucket único con particionado lógico

### Estructura en S3

```
Asset=CRIPTO/
 └── Year=YYYY/
     └── month=MM/
         └── data.csv
```

Ejemplo:

```
Asset=SOLUSD/Year=2024/month=03/data.csv
```

---

## 📊 Datos

* **Fuente**: TradingView
* **Exchange**: BINANCE
* **Activo**: SOLUSD (Solana)
* **Frecuencia**: Diaria (1D)
* **Formato**: CSV
* **Schema**:

  ```
  datetime, symbol, open, high, low, close, volume
  ```

---

## ⚙️ Implementación

El pipeline está implementado en **Python** y automatiza todo el proceso ETL:

### Funcionalidades clave

* **Smart Caching**: detecta el último dato disponible en S3 y descarga solo los datos faltantes.
* **Carga incremental**: evita reprocesar históricos completos innecesariamente.
* **Gestión de retención**: elimina automáticamente datos con más de 4 años.
* **Idempotencia**: crea el bucket si no existe y permite ejecuciones repetidas sin errores.
* **Infraestructura como código**: la infraestructura se gestiona desde el propio script.

---

## 📈 Análisis Exploratorio de Datos (EDA)

Se ha realizado un EDA exhaustivo mediante un notebook Jupyter (`eda.ipynb`) para:

* Validar integridad y coherencia de los datos OHLCV.
* Comprobar cobertura temporal completa (sin días faltantes).
* Analizar volatilidad, retornos, outliers y volumen.
* Confirmar que los datos son aptos para futuros sprints de análisis y visualización.

---

## ▶️ Ejecución

1. Configurar credenciales de AWS.
2. Ejecutar el script principal de ingesta:

   ```bash
   python complete.py
   ```
3. (Opcional) Eliminar infraestructura/datos:

   ```bash
   python deletion.py
   ```

---

## 🧪 Resultados

* Creación automática del bucket en S3.
* Estructura particionada por activo, año y mes.
* Descargas optimizadas (solo datos nuevos).
* Dataset limpio, completo y validado.

---

## 👥 Autores

* Jorge Carnicero Príncipe
* Andrés Gil Vicente
* Jorge González Pérez

---
