# BigData2026

Proyecto de Prueba de GitHub Actions y MLFlow

## Nueva práctica integrada en Databricks

El piloto de la nueva secuencia práctica se encuentra en [`practicas/`](practicas/README.md). La primera clase incluye setup para Databricks Free Edition, generación de datos, ingesta CSV/JSON/Parquet, tablas Bronze en Delta y un desafío de evolución de esquema. Las carpetas históricas `clase-1` a `clase-4` se mantienen como referencia.

Este proyecto fue creado con el objetivo de probar y demostrar cómo configurar y utilizar **GitHub Actions** en un entorno de desarrollo Python.
Ademas como ejemplo para mostrar capacidades de MLflow 

Actions
-------

El propósito de este repositorio es realizar pruebas simples de integración continua (CI) usando **GitHub Actions**. Actualmente, el flujo de trabajo básico está configurado para ejecutar un script de Python en cada push a la rama `main`.


python -m prueba .venv   
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
./prueba/Scripts/activate  
git clone https://github.com/guillermohenrion/BigData2026.git
git status
git add .
git status
git commit -m "probar actions"
git push origin main


EJECUTAR MLFlow
---------------
1. mlflow server --backend-store-uri sqlite:///mlflow.db --port 5000
MLflow escuchando en http://localhost:5000/

2. Para Docker
Crear dockerfile con:
###################
FROM python:3.10

RUN pip install mlflow

EXPOSE 5000

CMD ["mlflow", "server", "--backend-store-uri", "sqlite:///mlflow.db", "--default-artifact-root", "/mlruns", "--host", "0.0.0.0", "--port", "5000"]
###############
Y ejecutar
    docker build -t mlflow-server .      
    docker run -p 5000:5000 mlflow-server .

3. local: mlflow ui
