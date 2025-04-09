# SQLite version

En producción es `SQLite version 3.34.1 2021-01-20 14:10:07`

# Consultas interesantes

==============================================================
El historico de los ZPs embebido en el histórico de los nombres
```sql
SELECT t.mac_address, n.name, 
n.valid_since AS name_valid_since, n.valid_until AS name_valid_until, n.valid_state AS name_valid_state, 
t.tess_id, t.zero_point, t.valid_since AS zp_valid_since, t.valid_until AS zp_valid_until, t.valid_state AS zp_valid_state,
t.registered AS registered
FROM tess_t AS t
JOIN name_to_mac_t as n USING (mac_address)
WHERE n.name LIKE 'stars%'
AND  t.valid_since BETWEEN n.valid_since AND n.valid_until
ORDER BY t.mac_address, t.valid_since
```
==================== HISTORIOCO DE LOS TESS-W SOSPECHOSOS DE TEBEER ZP 2.0 EN SU CALIBRACION ===========
```sql
select count(*) from tess_t as t
where mac_address in (select distinct mac_address from tess_t where zero_point = 2.0);

select t.tess_id, t.mac_address, t.zero_point, t.filter,  t.valid_since, t.valid_until, t.valid_state, t.location_id
from tess_t as t
where mac_address in (select distinct mac_address from tess_t where zero_point = 2.0)
```
```sql
SELECT n.name, t.tess_id, t.mac_address, t.zero_point, t.filter,  t.valid_since, t.valid_until, t.valid_state, t.location_id
FROM tess_t AS t
JOIN name_to_mac_t as n USING (mac_address)
WHERE t.mac_address IN (SELECT DISTINCT mac_address FROM tess_t WHERE zero_point = 2.0)
AND n.name LIKE 'stars%'
ORDER BY CAST(SUBSTR(name,6) AS INT) ASC, t.valid_since ASC;
```

select distinct mac_address from tess_t where zero_point = 2.0 and valid_state = 'Current'

name	   tess_id	mac_address	        zero_point	filter		valid_since			valid_until			valid_state	location_id
stars974   2787		4C:75:25:28:9D:C1	2.0  	    UV/IR-cut	2023-09-12 08:26:07	2999-12-31T23:59:59	Current				-1

-- En TESS.DB

CREATE TEMP TABLE IF NOT EXISTS macs_t ( mac_address TEXT );
INSERT INTO macs_t 
SELECT DISTINCT mac_address FROM tess_t WHERE zero_point = 2.0 AND valid_state = 'Current'



 ===> HAY QUE COMPROBAR
 1) Cuantos fotometros con valid_state = 'Current' y zero_point = 2.0 se han quedado tras la actualizacion
 2) Cuantos de esos fotometros no han sido calibrados con ZPTESS
 3) Y si todo casa, lanzar el script ap_2.0_de vuelta en la base de datos de producción


========

CREATE TABLE tess_readings_t
            (
            date_id             INTEGER NOT NULL REFERENCES date_t(date_id), 
            time_id             INTEGER NOT NULL REFERENCES time_t(time_id), 
            tess_id             INTEGER NOT NULL REFERENCES tess_t(tess_id),
            location_id         INTEGER NOT NULL REFERENCES location_t(location_id),
            units_id            INTEGER NOT NULL REFERENCES tess_units_t(units_id),
            sequence_number     INTEGER,
            frequency           REAL,
            magnitude           REAL,
            ambient_temperature REAL,
            sky_temperature     REAL,
            azimuth             REAL,
            altitude            REAL,
            longitude           REAL,
            latitude            REAL,
            height              REAL, signal_strength       INTEGER,
            PRIMARY KEY (date_id, time_id, tess_id)
            )
=======