import pandas as pd
import numpy as np
import os

def extract_data():
    
    print("=" * 60)
    print("FASE 1: EXTRACCIÓN DE DATOS")
    print("=" * 60)

    # Obtener la ruta base del script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")

    # Buscar todos los archivos CSV en la carpeta
    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    raw_data = {}

    # Leer los archivos encontrados
    for file in csv_files:
        try:
            # Extraer el año del nombre del archivo (por ejemplo, "2015.csv" → 2015)
            year = int(os.path.splitext(file)[0])
            file_path = os.path.join(data_dir, file)

            df = pd.read_csv(file_path)
            raw_data[year] = df

            print(f"✓ {year}: {len(df)} registros, {len(df.columns)} columnas")

        except Exception as e:
            print(f"✗ Error al cargar {file}: {str(e)}")

    print(f"\n✓ Extracción completada: {len(raw_data)} archivos cargados\n")
    return raw_data

def transform_data(raw_data):
    """
    Transforma y estandariza los datos de todos los años
    
    Args:
        raw_data (dict): Diccionario con DataFrames por año
        
    Returns:
        pd.DataFrame: DataFrame combinado y transformado
    """
    print("=" * 60)
    print("FASE 2: TRANSFORMACIÓN DE DATOS")
    print("=" * 60)
    
    # Mapeo unificado de columnas
    column_map = {
        # Identificadores
        'Country': 'Country',
        'Country or region': 'Country',
        'Region': 'Region',
        
        # Score
        'Happiness Rank': 'Happiness_Rank',
        'Happiness.Rank': 'Happiness_Rank',
        'Overall rank': 'Happiness_Rank',
        'Happiness Score': 'Happiness_Score',
        'Happiness.Score': 'Happiness_Score',
        'Score': 'Happiness_Score',
        
        # GDP
        'Economy (GDP per Capita)': 'GDP_per_Capita',
        'Economy..GDP.per.Capita.': 'GDP_per_Capita',
        'GDP per capita': 'GDP_per_Capita',
        
        # Social Support
        'Family': 'Social_Support',
        'Social support': 'Social_Support',
        
        # Life Expectancy
        'Health (Life Expectancy)': 'Healthy_Life_Expectancy',
        'Health..Life.Expectancy.': 'Healthy_Life_Expectancy',
        'Healthy life expectancy': 'Healthy_Life_Expectancy',
        
        # Freedom
        'Freedom': 'Freedom',
        'Freedom to make life choices': 'Freedom',
        
        # Corruption
        'Trust (Government Corruption)': 'Perceptions_of_Corruption',
        'Trust..Government.Corruption.': 'Perceptions_of_Corruption',
        'Perceptions of corruption': 'Perceptions_of_Corruption',
        
        # Generosity
        'Generosity': 'Generosity',
    }
    
    # Mapeo de nombres de países
    country_map = {
        'Taiwan Province of China': 'Taiwan',
        'Hong Kong S.A.R., China': 'Hong Kong',
        'Trinidad & Tobago': 'Trinidad and Tobago',
        'North Cyprus': 'Northern Cyprus',
        'Northern Cyprus': 'Northern Cyprus',
        'Somaliland Region': 'Somaliland region',
        'Palestinian Territories': 'Palestine',
        'Congo (Kinshasa)': 'Democratic Republic of Congo',
        'Congo (Brazzaville)': 'Republic of Congo',
        'Ivory Coast': "Cote d'Ivoire",
        'Macedonia': 'North Macedonia',
        'North Macedonia': 'North Macedonia',
        'Swaziland': 'Eswatini'
    }
    
    # Crear mapeo de regiones desde 2015 y 2016
    region_map = {}
    for year in [2015, 2016]:
        if year in raw_data:
            df_temp = raw_data[year].rename(columns=column_map)
            if 'Country' in df_temp.columns and 'Region' in df_temp.columns:
                df_temp['Country'] = df_temp['Country'].str.strip()
                df_temp['Country'] = df_temp['Country'].replace(country_map)
                region_dict = df_temp.set_index('Country')['Region'].to_dict()
                region_map.update(region_dict)
    
    print(f"✓ Mapeo de regiones creado: {len(region_map)} países\n")
    
    # Transformar cada año
    transformed_dfs = []
    
    for year, df in raw_data.items():
        print(f"--- Procesando {year} ---")
        
        # 1. Renombrar columnas
        df_clean = df.rename(columns=column_map)
        print(f"  ✓ Columnas renombradas")
        
        # 2. Estandarizar nombres de países
        if 'Country' in df_clean.columns:
            df_clean['Country'] = df_clean['Country'].str.strip()
            df_clean['Country'] = df_clean['Country'].replace(country_map)
            print(f"  ✓ Nombres de países estandarizados")
        
        # 3. Asignar regiones faltantes
        if 'Region' not in df_clean.columns:
            df_clean['Region'] = df_clean['Country'].map(region_map)
            print(f"  ✓ Regiones asignadas desde años 2015-2016")
        elif df_clean['Region'].isnull().any():
            # Rellenar regiones faltantes
            df_clean['Region'].fillna(df_clean['Country'].map(region_map), inplace=True)
            print(f"  ✓ Regiones faltantes rellenadas")
        else:
            print(f"  ✓ Regiones ya presentes")
        
        # 4. Añadir columna Year
        df_clean['Year'] = year
        print(f"  ✓ Columna Year añadida")
        
        # 5. Seleccionar solo las columnas que necesitamos
        columns_to_keep = [
            'Country', 'Region', 'Year', 'Happiness_Score',
            'GDP_per_Capita', 'Social_Support', 'Healthy_Life_Expectancy',
            'Freedom', 'Generosity', 'Perceptions_of_Corruption'
        ]
        
        # Mantener solo las columnas que existen
        available_columns = [col for col in columns_to_keep if col in df_clean.columns]
        df_clean = df_clean[available_columns]
        
        print(f"  ✓ Columnas seleccionadas: {len(available_columns)}")
        print(f"  ✓ Registros procesados: {len(df_clean)}\n")
        
        transformed_dfs.append(df_clean)
    
    # Combinar todos los años
    print("--- Combinando todos los años ---")
    final_df = pd.concat(transformed_dfs, ignore_index=True)
    
    # Ordenar por año y país
    final_df = final_df.sort_values(['Year', 'Country']).reset_index(drop=True)
    
    print(f"✓ Datos combinados exitosamente")
    print(f"✓ Total de registros: {len(final_df)}")
    print(f"✓ Países únicos: {final_df['Country'].nunique()}")
    print(f"✓ Años: {sorted(final_df['Year'].unique())}")
    print(f"✓ Columnas finales: {list(final_df.columns)}\n")

    # Manejo de valores nulos después del merge
    print("--- Revisando y manejando valores nulos ---")
    
    # Contar nulos por columna
    null_counts = final_df.isnull().sum()
    total_nulls = null_counts.sum()
    
    if total_nulls > 0:
        print(f"⚠ Se encontraron {total_nulls} valores nulos:")
        for col, count in null_counts[null_counts > 0].items():
            percentage = (count / len(final_df)) * 100
            print(f"  - {col}: {count} ({percentage:.2f}%)")
        
        print("\n--- Aplicando estrategias de imputación ---")
        
        # Estrategia 1: Regiones faltantes
        if final_df['Region'].isnull().any():
            nulls_before = final_df['Region'].isnull().sum()
            # Intentar rellenar con la región más común del país en otros años
            for country in final_df[final_df['Region'].isnull()]['Country'].unique():
                common_region = final_df[final_df['Country'] == country]['Region'].mode()
                if len(common_region) > 0:
                    final_df.loc[(final_df['Country'] == country) & (final_df['Region'].isnull()), 'Region'] = common_region[0]
            
            # Si aún quedan nulos, asignar 'Unknown'
            final_df['Region'].fillna('Unknown', inplace=True)
            nulls_after = final_df['Region'].isnull().sum()
            print(f"  ✓ Region: {nulls_before} → {nulls_after} nulos")
        
        # Estrategia 2: Valores numéricos - imputar con mediana del país
        numeric_cols = ['Happiness_Score', 'GDP_per_Capita', 'Social_Support', 
                       'Healthy_Life_Expectancy', 'Freedom', 'Generosity', 
                       'Perceptions_of_Corruption']
        
        for col in numeric_cols:
            if col in final_df.columns and final_df[col].isnull().any():
                nulls_before = final_df[col].isnull().sum()
                
                # Intentar imputar con la mediana del mismo país en otros años
                for country in final_df[final_df[col].isnull()]['Country'].unique():
                    country_median = final_df[final_df['Country'] == country][col].median()
                    if pd.notna(country_median):
                        final_df.loc[(final_df['Country'] == country) & (final_df[col].isnull()), col] = country_median
                
                # Si aún quedan nulos, usar mediana de la región
                for region in final_df[final_df[col].isnull()]['Region'].unique():
                    if pd.notna(region) and region != 'Unknown':
                        region_median = final_df[final_df['Region'] == region][col].median()
                        if pd.notna(region_median):
                            final_df.loc[(final_df['Region'] == region) & (final_df[col].isnull()), col] = region_median
        final_nulls = final_df.isnull().sum().sum()
        if final_nulls == 0:
            print(f"\n✓ Todos los valores nulos han sido manejados exitosamente")
        else:
            print(f"\n⚠ Quedan {final_nulls} valores nulos sin resolver")
            print(final_df.isnull().sum()[final_df.isnull().sum() > 0])
    else:
        print(f"✓ No se encontraron valores nulos en el dataset combinado")
    
    print()
    return final_df
def load_data(df, output_file='data/happiness_merged_2015-2019.csv'):
    """
    Carga los datos transformados en un archivo CSV
    
    Args:
        df (pd.DataFrame): DataFrame con los datos transformados
        output_file (str): Nombre del archivo de salida
        
    Returns:
        None
    """
    print("=" * 60)
    print("FASE 3: CARGA DE DATOS")
    print("=" * 60)
    
    try:
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"✓ Datos guardados exitosamente en: {output_file}")
        print(f"✓ Tamaño del archivo: {len(df)} filas x {len(df.columns)} columnas")
        
    except Exception as e:
        print(f"✗ Error al guardar el archivo: {str(e)}")
    
    print("\n" + "=" * 60)
    print("✓ PROCESO ETL COMPLETADO")
    print("=" * 60)

if __name__ == "__main__":
    # Paso 1: Extraer datos
    raw_data = extract_data()
    
    # Paso 2: Transformar datos
    transformed_df = transform_data(raw_data)
    
    # Paso 3: Cargar datos
    load_data(transformed_df, output_file='data/happiness_merged_2015-2019.csv')
    
    print("\n✓ Archivo listo para crear el modelo!")