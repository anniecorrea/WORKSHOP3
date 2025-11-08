from kafka import KafkaProducer
import pandas as pd
import json
import time
import pickle
import numpy as np
import os
import sys
from sklearn.model_selection import train_test_split

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from ETL import extract_data, transform_data

# --- CONFIGURACIÓN ---
KAFKA_TOPIC = "happiness_topic"
KAFKA_SERVER = "localhost:9092"
MODEL_PATH = "../regression_model.pkl"

print("=" * 60)
print("KAFKA PRODUCER - HAPPINESS PREDICTION")
print("=" * 60)

# --- CARGAR MODELO ---
print("\n1. Cargando modelo ML...")
try:
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    print(f"   ✓ Modelo cargado desde {MODEL_PATH}")
except Exception as e:
    print(f"   ✗ Error cargando modelo: {e}")
    exit(1)

# --- INICIALIZAR PRODUCTOR ---
print("\n2. Inicializando Kafka Producer...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        acks='all',  # Esperar confirmación de todos los replicas
        retries=3
    )
    print(f"   ✓ Conectado a Kafka en {KAFKA_SERVER}")
except Exception as e:
    print(f"   ✗ Error conectando a Kafka: {e}")
    exit(1)

# --- EXTRACCIÓN Y TRANSFORMACIÓN ---
print("\n3. Extrayendo y transformando datos...")
try:
    raw_data = extract_data()
    df = transform_data(raw_data)
    
    print(f"   ✓ Datos transformados: {len(df)} registros")
except Exception as e:
    print(f"   ✗ Error en ETL: {e}")
    exit(1)

# --- HACER PREDICCIONES ---
print("\n4. Generando predicciones...")
try:
    # --- Seleccionar columnas según el modelo ---
    features = [
        'Country', 'Region', 'GDP_per_Capita', 'Social_Support',
        'Healthy_Life_Expectancy', 'Freedom', 'Generosity',
        'Perceptions_of_Corruption'
    ]
    target = 'Happiness_Score'

    # Verificar columnas
    missing_cols = [col for col in features if col not in df.columns]
    if missing_cols:
        print(f"⚠ Columnas faltantes: {missing_cols}")

    # --- División train/test ---
    X = df[features].copy()
    y = df[target].copy() if target in df.columns else None
    year = df['Year'] if 'Year' in df.columns else None  # Guardar año aparte

    X_train, X_test, y_train, y_test, year_train, year_test = train_test_split(
        X, y, year,
        train_size=0.7,
        random_state=1234,
        shuffle=True
    )

    # --- Crear copias con marca de origen ---
    df_train = X_train.copy()
    df_train['Year'] = year_train
    df_train['Happiness_Score'] = y_train
    df_train['Data_Split'] = 'train'

    df_test = X_test.copy()
    df_test['Year'] = year_test
    df_test['Happiness_Score'] = y_test
    df_test['Data_Split'] = 'test'

    # --- Predicciones separadas ---
    df_train['Predicted_Score'] = model.predict(X_train)
    df_test['Predicted_Score'] = model.predict(X_test)

    # --- Calcular error ---
    df_train['Predicted_Error'] = np.abs(df_train['Happiness_Score'] - df_train['Predicted_Score'])
    df_test['Predicted_Error'] = np.abs(df_test['Happiness_Score'] - df_test['Predicted_Score'])

    # --- Combinar todo correctamente ---
    df_final = pd.concat([df_train, df_test], ignore_index=True)

    print(f"   ✓ Train: {len(df_train)} | Test: {len(df_test)}")
    print(f"   ✓ Total: {len(df_final)} registros combinados")
    print("Distribución de Data_Split:")
    print(df_final['Data_Split'].value_counts())

except Exception as e:
    print(f"✗ Error generando predicciones: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# --- ENVIAR REGISTROS UNO POR UNO ---
print("\n5. Enviando registros a Kafka...")
success_count = 0
error_count = 0

try:
    for idx, row in df_final.iterrows():
        try:
            # Convertir row a diccionario
            data = row.to_dict()
            
            # Convertir valores numpy a tipos nativos de Python
            for key, value in data.items():
                if isinstance(value, (np.integer, np.floating)):
                    data[key] = float(value) if isinstance(value, np.floating) else int(value)
                elif pd.isna(value):
                    data[key] = None
            
            # Enviar a Kafka
            future = producer.send(KAFKA_TOPIC, value=data)
            future.get(timeout=10)  # Esperar confirmación
            
            success_count += 1
            
            if (idx + 1) % 100 == 0:
                print(f"   ✓ Enviados: {idx + 1}/{len(df)} registros")
            
            time.sleep(0.1)  # Pequeño delay para simular streaming
            
        except Exception as e:
            error_count += 1
            print(f"   ✗ Error enviando registro {idx}: {e}")
    
    # Asegurar que todos los mensajes se envíen
    producer.flush()
    
except KeyboardInterrupt:
    print("\n⚠ Producción interrumpida por el usuario")
finally:
    producer.close()

# --- RESUMEN ---
print("\n" + "=" * 60)
print("RESUMEN DE PRODUCCIÓN")
print("=" * 60)
print(f"✓ Registros enviados exitosamente: {success_count}/{len(df)}")
print(f"✗ Errores: {error_count}")
print(f"✓ Producer cerrado correctamente")
print("=" * 60)