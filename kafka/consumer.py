from kafka import KafkaConsumer
import json
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
from datetime import datetime

# --- CONFIGURACIÓN ---
KAFKA_TOPIC = "happiness_topic"
KAFKA_SERVER = "localhost:9092"

DB_USER = "root"
DB_PASS = "annie"
DB_HOST = "localhost"
DB_PORT = 3306
DB_NAME = "happiness_db"

print("=" * 60)
print("KAFKA CONSUMER - HAPPINESS PREDICTIONS TO DATABASE")
print("=" * 60)

# --- CREAR BASE DE DATOS SI NO EXISTE ---
print("\n1. Verificando/Creando base de datos...")
try:
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    print(f"   ✓ Base de datos '{DB_NAME}' verificada/creada")
    conn.close()
except Exception as e:
    print(f"   ✗ Error creando la base de datos: {e}")
    exit(1)

# --- CONEXIÓN SQLALCHEMY ---
DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URI)

# --- CREAR TABLA SI NO EXISTE ---
print("\n2. Verificando/Creando tabla...")
try:
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS predicciones_happiness (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Country VARCHAR(250),
                Region VARCHAR(250),
                Year INT,
                GDP_per_Capita FLOAT,
                Social_Support FLOAT,
                Healthy_Life_Expectancy FLOAT,
                Freedom FLOAT,
                Generosity FLOAT,
                Perceptions_of_Corruption FLOAT,
                Happiness_Score FLOAT,
                Predicted_Score FLOAT,
                Predicted_Error FLOAT,
                Data_Split VARCHAR(10),
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_country (Country),
                INDEX idx_year (Year),
                INDEX idx_region (Region)
            );
        """))
        conn.commit()
    print("   ✓ Tabla 'Predicciones_Happiness' verificada/creada")
except Exception as e:
    print(f"   ✗ Error creando tabla: {e}")
    exit(1)

# --- CONFIGURAR CONSUMIDOR KAFKA ---
print("\n3. Inicializando Kafka Consumer...")
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        group_id="happiness_group",
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',  
        enable_auto_commit=True,
        max_poll_records=100  # Procesar hasta 100 registros a la vez
    )
    print(f"   ✓ Conectado a Kafka topic: {KAFKA_TOPIC}")
except Exception as e:
    print(f"   ✗ Error conectando a Kafka: {e}")
    exit(1)

print("\n" + "=" * 60)
print("🎧 ESPERANDO MENSAJES DE KAFKA...")
print("=" * 60)

# --- CONTADORES ---
processed_count = 0
error_count = 0

# --- PROCESAR Y GUARDAR ---
try:
    for message in consumer:
        try:
            # Extraer datos del mensaje
            data = message.value
            
            # Crear DataFrame con el registro
            df_result = pd.DataFrame([data])
            
            # Seleccionar solo las columnas que existen en la tabla
            columns_to_save = [
                'Country', 'Region', 'Year',
                'GDP_per_Capita', 'Social_Support', 'Healthy_Life_Expectancy',
                'Freedom', 'Generosity', 'Perceptions_of_Corruption',
                'Happiness_Score', 'Predicted_Score', 'Predicted_Error', 'Data_Split'
            ]
            
            # Filtrar solo las columnas que existen en el DataFrame
            available_columns = [col for col in columns_to_save if col in df_result.columns]
            df_to_save = df_result[available_columns]
            
            # Guardar en base de datos
            df_to_save.to_sql(
                "predicciones_happiness", 
                engine, 
                if_exists='append', 
                index=False
            )
            
            processed_count += 1
            
            # Log cada 50 registros
            if processed_count % 100 == 0:
                print(f"✅ Procesados: {processed_count} registros")
            
            # Log detallado para los primeros registros
            if processed_count <= 5:
                print(f"\n📊 Registro #{processed_count}:")
                print(f"   País: {data.get('Country', 'N/A')}")
                print(f"   Año: {data.get('Year', 'N/A')}")
                print(f"   Score Real: {data.get('Happiness_Score', 'N/A'):.4f}")
                print(f"   Score Predicho: {data.get('Predicted_Score', 'N/A'):.4f}")
                print(f"   Error: {data.get('Predicted_Error', 'N/A'):.4f}")
            
        except Exception as e:
            error_count += 1
            print(f"❌ Error procesando mensaje: {e}")
            if error_count <= 3:  # Mostrar detalles solo para los primeros errores
                import traceback
                traceback.print_exc()

except KeyboardInterrupt:
    print("\n⚠ Consumer interrumpido por el usuario")
finally:
    consumer.close()
    
    # --- RESUMEN FINAL ---
    print("\n" + "=" * 60)
    print("RESUMEN DE CONSUMO")
    print("=" * 60)
    print(f"✓ Registros procesados: {processed_count}")
    print(f"✗ Errores: {error_count}")
    
    # Consultar total de registros en BD
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM Predicciones_Happiness"))
            total_in_db = result.fetchone()[0]
            print(f"✓ Total en base de datos: {total_in_db}")
    except:
        pass
    
    print("✓ Consumer cerrado correctamente")
    print("=" * 60)