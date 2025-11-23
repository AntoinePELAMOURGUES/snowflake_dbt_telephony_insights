-- 1. SE PLACER EN TANT QU'ADMINISTRATEUR SYSTÈME
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

-- 2. CRÉATION DE LA BASE/SCHEMA OUTILS (Si elle n'existe pas encore)
CREATE DATABASE IF NOT EXISTS UTILS_DB;
CREATE SCHEMA IF NOT EXISTS UTILS_DB.PUBLIC;

-- 3. CRÉATION DE LA FONCTION (UDF)
CREATE OR REPLACE FUNCTION UTILS_DB.PUBLIC.UDF_LAMBERT2_TO_GPS(X FLOAT, Y FLOAT)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'convert_lambert_to_wgs84'
AS
$$
import math

def convert_lambert_to_wgs84(x, y):
    if x is None or y is None:
        return None

    # Constantes Lambert II Étendu (France)
    n = 0.7289686274
    c = 11745793.39
    xs = 600000.0
    ys = 8199695.768

    # Conversion
    try:
        dx = x - xs
        dy = y - ys
        r = math.sqrt(dx**2 + dy**2)
        gamma = math.atan2(dx, -dy)

        lat_iso = -1/n * math.log(abs(r/c))

        lon_rad = gamma / n
        lon = math.degrees(lon_rad) + 2.33722917

        e = 0.08248325676
        phi = 2 * math.atan(math.exp(lat_iso)) - math.pi/2
        epsilon = 1e-10

        for _ in range(10):
            phi_prev = phi
            arg = (1 + e * math.sin(phi)) / (1 - e * math.sin(phi))
            phi = 2 * math.atan(math.pow(arg, e/2) * math.exp(lat_iso)) - math.pi/2
            if abs(phi - phi_prev) < epsilon:
                break

        lat = math.degrees(phi)

        return {"lat": lat, "lon": lon}
    except:
        return None
$$;

-- 4. LES GRANTS (CRUCIAL !)
-- Remplace 'SYSADMIN' ci-dessous par le rôle que dbt utilise s'il est différent
-- (Souvent c'est SYSADMIN en dev, mais en prod c'est un rôle de service)

-- Donner le droit d'utiliser la base et le schéma
GRANT USAGE ON DATABASE UTILS_DB TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA UTILS_DB.PUBLIC TO ROLE SYSADMIN;

-- Donner le droit d'utiliser la fonction
GRANT USAGE ON FUNCTION UTILS_DB.PUBLIC.UDF_LAMBERT2_TO_GPS(FLOAT, FLOAT) TO ROLE SYSADMIN;

-- Si tu as d'autres rôles (ex: un rôle pour Streamlit), donne-leur aussi l'accès :
GRANT USAGE ON DATABASE UTILS_DB TO ROLE STREAMLIT_ROLE;
GRANT USAGE ON SCHEMA UTILS_DB.PUBLIC TO ROLE STREAMLIT_ROLE;
GRANT USAGE ON FUNCTION UTILS_DB.PUBLIC.UDF_LAMBERT2_TO_GPS(FLOAT, FLOAT) TO ROLE STREAMLIT_ROLE;