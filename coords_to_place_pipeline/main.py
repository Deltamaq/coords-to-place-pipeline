import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

spreadsheet_name = input('Digit the path to the file: ')
df = pd.read_csv(spreadsheet_name, encoding='latin-1', sep=';')
df.dropna(inplace=True)
latitude_column_name = input('Digit the name of the LATITUDE column: ')
longitude_column_name = input('Digit the name of the LONGITUDE column: ')

geolocator = Nominatim(user_agent="lat-lon-pipeline", timeout=3)
geocode = RateLimiter(geolocator.reverse, min_delay_seconds=2)

def get_location(row):
    latitude = row[latitude_column_name].replace(',', '.')
    longitude = row[longitude_column_name].replace(',', '.')
    location = geolocator.reverse((latitude, longitude), exactly_one=True).raw['address']

    if location:
        city = location.get('city') if location.get('city') else location.get('town') if location.get('town') else location.get('city_district') if location.get('city_district') else location.get('municipality', 'desconhecido')
        state = location.get('state', 'desconhecido')
        return pd.Series([city, state])
    
    return pd.Series(['desconhecido', 'desconhecido'])

chunk_size = 100
chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

results = []

for i, chunk in enumerate(chunks):
    print(f"Processando chunk {i+1} de {len(chunks)}...")
    chunk[['municipality', 'state']] = chunk.apply(get_location, axis=1)
    results.append(chunk)

df_final = pd.concat(results)

df_final.to_excel('tabela_com_municipio_estado.xlsx', index=True)

print("Processamento concluído e arquivo salvo.")
