# -*- coding: utf-8 -*-
"""
Materia: Laboratorio de datos - FCEyN - UBA
Clase  : Clase SQL. Script clase. 
Autor  : Pablo Turjanski
Fecha  : 2025-02-03
"""

# Importamos bibliotecas
import pandas as pd
import duckdb as dd


#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

# /home/Estudiante/Escritorio/Labo_Datos-main/Ejercicios/Sql

carpeta = "./"

# Ejercicios AR-PROJECT, SELECT, RENAME
empleado       = pd.read_csv(carpeta+"empleado.csv")
# Ejercicios AR-UNION, INTERSECTION, MINUS
alumnosBD      = pd.read_csv(carpeta+"alumnosBD.csv")
alumnosTLeng   = pd.read_csv(carpeta+"alumnosTLeng.csv")
# Ejercicios AR-CROSSJOIN
persona        = pd.read_csv(carpeta+"persona.csv")
nacionalidades = pd.read_csv(carpeta+"nacionalidades.csv")
# Ejercicios ¿Mismos Nombres?
se_inscribe_en=pd.read_csv(carpeta+"se_inscribe_en.csv")
materia       =pd.read_csv(carpeta+"materia.csv")
# Ejercicio JOIN múltiples tablas
vuelo      = pd.read_csv(carpeta+"vuelo.csv")    
aeropuerto = pd.read_csv(carpeta+"aeropuerto.csv")    
pasajero   = pd.read_csv(carpeta+"pasajero.csv")    
reserva    = pd.read_csv(carpeta+"reserva.csv")    
# Ejercicio JOIN tuplas espúreas
empleadoRol= pd.read_csv(carpeta+"empleadoRol.csv")    
rolProyecto= pd.read_csv(carpeta+"rolProyecto.csv")    
# Ejercicios funciones de agregación, LIKE, Elección, Subqueries 
# y variables de Python
examen     = pd.read_csv(carpeta+"examen.csv")
# Ejercicios de manejo de valores NULL
examen03 = pd.read_csv(carpeta+"examen03.csv")



#%%===========================================================================
# Ejemplo inicial
#=============================================================================

print(empleado)

consultaSQL = """
               SELECT DISTINCT DNI, Salario
               FROM empleado;
              """

dataframeResultado = dd.sql(consultaSQL).df()

print(dataframeResultado)


#%%===========================================================================
# Ejercicios AR-PROJECT <-> SELECT
#=============================================================================
# a.- Listar DNI y Salario de empleados 
consultaSQL = """
                SELECT dni, salario
                FROM empleado
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# b.- Listar Sexo de empleados 
consultaSQL = """
                SELECT DISTINCT sexo FROM empleado
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
#c.- Listar Sexo de empleados (sin DISTINCT)
consultaSQL = """
                SELECT sexo FROM empleado
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%===========================================================================
# Ejercicios AR-SELECT <-> WHERE
#=============================================================================
# a.- Listar de EMPLEADO sólo aquellos cuyo sexo es femenino
consultaSQL = """
                SELECT * FROM empleado WHERE sexo='F'
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%% -----------
#b.- Listar de EMPLEADO aquellos cuyo sexo es femenino y su salario es mayor a $15.000
consultaSQL = """
                select * from empleado where sexo='F' and salario > 15000
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%===========================================================================
# Ejercicios AR-RENAME <-> AS
#=============================================================================
#a.- Listar DNI y Salario de EMPLEADO, y renombrarlos como id e Ingreso
consultaSQL = """
                SELECT dni as id, salario as ingreso FROM empleado
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%% # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # #                                                                     # #
# #    INICIO -->           EJERCICIO Nro. 01                             # #
 # #                                                                     # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# IMPORTANTE: Recordar que se utilizaran los datos de vuelo, aeropuerto, pasajero y reserva

#%%===========================================================================
# EJERCICIOS PARA REALIZAR DE MANERA INDIVIDUAL --> EJERCICIO Nro. 01
#=============================================================================
# Ejercicio 01.1.- Retornar Codigo y Nombre de los aeropuertos de Londres
consultaSQL = """
                SELECT codigo, nombre FROM aeropuerto WHERE ciudad='Londres'
              """
    
dataframeResultado = dd.sql(consultaSQL).df()


#%% -----------
# Ejercicio 01.2.- ¿Qué retorna 
#                       SELECT DISTINCT Ciudad AS City 
#                       FROM aeropuerto 
#                       WHERE Codigo='ORY' OR Codigo='CDG'; ?
consultaSQL = "SELECT DISTINCT Ciudad AS City FROM aeropuerto WHERE Codigo='ORY' OR Codigo='CDG';"

dataframeResultado = dd.sql(consultaSQL).df()

#%% -----------
# Ejercicio 01.3.- Obtener los números de vuelo que van desde CDG hacia LHR
consultaSQL = """
                SELECT numero FROM vuelo WHERE origen='CDG' AND destino='LHR'
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%% -----------
# Ejercicio 01.4.- Obtener los números de vuelo que van desde CDG hacia LHR o viceversa
consultaSQL = """
                SELECT numero
                FROM vuelo
                WHERE (origen='CDG' AND destino='LHR') OR (origen='LHR' AND destino='CDG')
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%% -----------
# Ejercicio 01.5.- Devolver las fechas de reservas cuyos precios son mayores a $200
consultaSQL = """
                SELECT fecha FROM reserva
                WHERE precio > 200
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%% # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # #                                                                     # #
# #    FIN -->              EJERCICIO Nro. 01                             # #
 # #                                                                     # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    
#=============================================================================
# Ejercicios AR-UNION, INTERSECTION, MINUS <-> UNION, INTERSECTION, EXCEPT
#=============================================================================
# a1.- Listar a los alumnos que cursan BDs o TLENG

consultaSQL = """
                (SELECT * FROM alumnosBD) UNION (SELECT * FROM alumnosTLeng)
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%% -----------
# a2.- Listar a los alumnos que cursan BDs o TLENG (usando UNION ALL)

consultaSQL = """
                (SELECT * FROM alumnosBD) UNION ALL (SELECT * FROM alumnosTLeng)
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%% -----------
# b.- Listar a los alumnos que cursan simultáneamente BDs y TLENG

consultaSQL = """
                (SELECT * FROM alumnosBD) INTERSECT (SELECT * FROM alumnosTLeng)
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%% -----------
# c.- Listar a los alumnos que cursan BDs y no cursan TLENG 

consultaSQL = """
                (SELECT * FROM alumnosBD) EXCEPT (SELECT * FROM alumnosTLeng)
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%% # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # #                                                                     # #
# #    INICIO -->           EJERCICIO Nro. 02                             # #
 # #                                                                     # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# IMPORTANTE: Recordar que se utilizaran los datos de vuelo, aeropuerto, pasajero y reserva

#=============================================================================
#  EJERCICIOS PARA REALIZAR DE MANERA INDIVIDUAL --> EJERCICIO Nro. 02
#=============================================================================
# Ejercicio 02.1.- Devolver los números de vuelo que tienen reservas generadas (utilizar intersección)
consultaSQL = """
                (SELECT numero FROM vuelo) INTERSECT (SELECT nroVuelo FROM reserva)
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# Ejercicio 02.2.- Devolver los números de vuelo que aún no tienen reservas
consultaSQL = """
                (SELECT numero FROM vuelo) EXCEPT (SELECT nroVuelo FROM reserva)
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# Ejercicio 02.3.- Retornar los códigos de aeropuerto de los que parten o arriban los vuelos
consultaSQL = """
               ((SELECT origen FROM vuelo) intersect (SELECT codigo FROM aeropuerto))
               UNION 
               ((SELECT destino FROM vuelo) intersect (SELECT codigo FROM aeropuerto))
              """
              
dataframeResultado = dd.sql(consultaSQL).df()



#%% # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # #                                                                     # #
# #    FIN -->              EJERCICIO Nro. 02                             # #
 # #                                                                     # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

#=============================================================================
# Ejercicios AR-... JOIN <-> ... JOIN
#=============================================================================
# a1.- Listar el producto cartesiano entre las tablas persona y nacionalidades

consultaSQL = """
                SELECT * FROM persona CROSS JOIN nacionalidades
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%%-----------
# a2.- Listar el producto cartesiano entre las tablas persona y nacionalidades (sin usar CROSS JOIN)

consultaSQL = """
                SELECT * FROM persona, nacionalidades
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%% --------------------------------------------------------------------------------------------
# Carga los nuevos datos del dataframe persona para los ejercicios de AR-INNER y LEFT OUTER JOIN
# ----------------------------------------------------------------------------------------------
persona        = pd.read_csv(carpeta+"persona_ejemplosJoin.csv")
# ----------------------------------------------------------------------------------------------
# b1.- Vincular las tablas persona y nacionalidades a través de un INNER JOIN

consultaSQL = """
                SELECT * FROM persona
                INNER JOIN nacionalidades 
                ON IDN = Nacionalidad
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# b2.- Vincular las tablas persona y nacionalidades (sin usar INNER JOIN)

consultaSQL = """
                SELECT * FROM persona, nacionalidades
                WHERE IDN = NACIONALIDAD
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# c.- Vincular las tablas persona y nacionalidades a través de un LEFT OUTER JOIN

consultaSQL = """
                SELECT * FROM persona
                LEFT JOIN nacionalidades
                ON Nacionalidad=IDN 
              """
              
dataframeResultado = dd.sql(consultaSQL).df()

#%%===========================================================================
# Ejercicios SQL - ¿Mismos Nombres?
#=============================================================================
# a.- Vincular las tablas se_inscribe_en y materia. Mostrar sólo LU y Nombre de materia

consultaSQL = """
                SELECT lu, nombre FROM materia as m
                INNER JOIN se_inscribe_en as s
                ON m.Codigo_materia = s.Codigo_materia
                
              """

dataframeResultado = dd.sql(consultaSQL).df()

    
#%% # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # #                                                                     # #
# #    INICIO -->           EJERCICIO Nro. 03                             # #
 # #                                                                     # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# IMPORTANTE: Recordar que se utilizaran los datos de vuelo, aeropuerto, pasajero y reserva

#%%===========================================================================
# EJERCICIOS PARA REALIZAR DE MANERA INDIVIDUAL --> EJERCICIO Nro. 03
#=============================================================================

# Ejercicio 03.0.- en un solo SELECT devolver fecha nombre y hora de salida 

consultaSQL = """
                SELECT fecha, nombre, salida 
                FROM reserva AS r 
                INNER JOIN pasajero AS p ON r.DNI = p.DNI
                INNER JOIN vuelo AS v ON Numero = NroVuelo
              """
              
query_subconsulta = """
                      SELECT fecha, nombre, salida FROM 
                      (SELECT fecha, nombre, nroVuelo FROM reserva AS r INNER JOIN pasajero AS p ON r.DNI = p.DNI)
                      INNER JOIN vuelo ON Numero = nroVuelo 
                    """

dataframeResultado = dd.sql(consultaSQL).df()
#%%-----------
# Ejercicio 03.1.- Devolver el nombre de la ciudad de partida del vuelo número 165

consultaSQL = """
                SELECT nombre FROM aeropuerto
                INNER JOIN vuelo ON Codigo = Origen
                WHERE Numero = 165
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# Ejercicio 03.2.- Retornar el nombre de las personas que realizaron reservas a un valor menor a $200

consultaSQL = """
                SELECT nombre FROM pasajero AS p 
                INNER JOIN reserva AS r
                ON p.DNI = r.DNI
                WHERE precio < 200
              """
consultaSQL = "SELECT * FROM vuelo WHERE origen=MAD"

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# Ejercicio 03.3.- Obtener Nombre, Fecha y Destino del Viaje de todos los pasajeros que vuelan desde Madrid

vuelosAMadrid = dd.sql("""
                       SELECT * FROM vuelo WHERE Origen = 'MAD'
              """).df()
              


dniPersonasDesdeMadrid = dd.sql("""
                                SELECT dni FROM reserva 
                                INNER JOIN vuelosAMadrid ON Numero = NroVuelo
              """).df()

consultaSQL = """
                SELECT fecha, nombre,Destino FROM reserva
                INNER JOIN pasajero ON reserva.DNI = pasajero.DNI
                INNER JOIN vuelosAMadrid ON Numero = NroVuelo
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%% # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # #                                                                     # #
# #    FIN -->              EJERCICIO Nro. 03                             # #
 # #                                                                     # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    
#%%===========================================================================
# Ejercicios SQL - Join de varias tablas en simultáneo
#=============================================================================
# a.- Vincular las tablas Reserva, Pasajero y Vuelo. Mostrar sólo Fecha de reserva, hora de salida del vuelo y nombre de pasajero.
    
consultaSQL = """
                SELECT fecha, nombre, salida FROM reserva
                INNER JOIN pasajero ON reserva.DNI = pasajero.DNI
                INNER JOIN vuelo ON Numero = NroVuelo
              """

dataframeResultado = dd.sql(consultaSQL).df()

    
#%%===========================================================================
# Ejercicios SQL - Tuplas espúreas
#=============================================================================
# a.- Vincular (JOIN)  EmpleadoRol y RolProyecto para obtener la tabla original EmpleadoRolProyecto
    
consultaSQL = """
                SELECT er.empleado, er.rol, rp.proyecto FROM empleadoRol er
                INNER JOIN rolProyecto rp ON er.rol = rp.rol 
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%===========================================================================
# Ejercicios SQL - Funciones de agregación
#=============================================================================
# a.- Usando sólo SELECT contar cuántos exámenes fueron rendidos (en total)
    
consultaSQL = """
                SELECT count(Instancia) AS cantRendidos FROM examen
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%%-----------
# b1.- Usando sólo SELECT contar cuántos exámenes fueron rendidos en cada Instancia
    
consultaSQL = """
                SELECT instancia, count(instancia) AS asistieron
                FROM examen
                GROUP BY instancia
              """
              
consultaSQL = """
                SELECT instancia, sexo, AVG(nota) AS promedio_instancia
                FROM examen
                GROUP BY instancia, sexo
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%%-----------
# b2.- Usando sólo SELECT contar cuántos exámenes fueron rendidos en cada Instancia (ordenado por instancia)
    
consultaSQL = """
                SELECT instancia, count(instancia) as cant FROM examen
                group by instancia
                order by instancia
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%%-----------
# b3.- Ídem ejercicio anterior, pero mostrar sólo las instancias a las que asistieron menos de 4 Estudiantes
    
consultaSQL = """
                SELECT instancia, count(instancia) as cant FROM examen
                group by instancia
                having cant < 4
                order by instancia
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# c.- Mostrar el promedio de edad de los estudiantes en cada instancia de examen
    
consultaSQL = """
                SELECT instancia, avg(edad) as prom_edad
                FROM examen
                GROUP BY instancia
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%%===========================================================================
# Ejercicios SQL - LIKE")
#=============================================================================
# a1.- Mostrar cuál fue el promedio de notas en cada instancia de examen, sólo para instancias de parcial.
    
consultaSQL = """
                SELECT instancia, avg(nota) as prom_edad
                FROM examen
                GROUP BY instancia
                HAVING instancia = parcial-01 or instancia = parcial-02
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# a2.- Mostrar cuál fue el promedio de notas en cada instancia de examen, sólo para instancias de parcial. Esta vez usando LIKE.
    
consultaSQL = """
                SELECT instancia, avg(nota) as prom_edad
                FROM examen
                GROUP BY instancia
                HAVING instancia ilike 'parcial%'   -- ilike no es case sensitive
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%%===========================================================================
# Ejercicios SQL - Eligiendo
#=============================================================================
# a1.- Listar a cada alumno que rindió el Parcial-01 y decir si aprobó o no (se aprueba con nota >=4).
    
consultaSQL = """
                SELECT Nombre, CASE WHEN nota >=4 THEN 'APROBO' ELSE 'DESAPROBO' END AS estado
                FROM examen
                WHERE instancia ILIKE 'parcial-01%'
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%%-----------
# a2.- Modificar la consulta anterior para que informe cuántos estudiantes aprobaron/reprobaron en cada instancia.
    
consultaSQL = """
                SELECT instancia, CASE WHEN nota >=4 THEN 'APROBO' ELSE 'DESAPROBO' END AS Estado, count(*) AS cantidad
                FROM examen
                GROUP BY instancia, estado
              """

dataframeResultado = dd.sql(consultaSQL).df()


#%%===========================================================================
# Ejercicios SQL - Subqueries
#=============================================================================
#a.- Listar los alumnos que en cada instancia obtuvieron una nota mayor al promedio de dicha instancia

promedio_por_instancia = """
                            SELECT instancia, avg(nota) as promedio
                            FROM examen 
                            GROUP BY instancia
                        """
dataframePromedios = dd.sql(promedio_por_instancia).df()


consultaSQL =  """
                SELECT nombre, e.instancia, nota FROM dataframePromedios As dp
                INNER JOIN examen AS e
                ON e.instancia = dp.instancia
                WHERE nota > promedio
                """


dataframeResultado = dd.sql(consultaSQL).df()


#%%-----------
# b.- Listar los alumnos que en cada instancia obtuvieron la mayor nota de dicha instancia

consultaSQL = """
                SELECT e1.nombre, e1.instancia, e1.nota
                FROM examen AS e1
                WHERE e1.nota = (
                    SELECT MAX(e2.nota)
                    FROM examen AS e2
                    WHERE e2.instancia=e1.instancia
                    )
              """

# ALL: si la subquery devuelve varios registros, compara con todas 
# ANY:  si la subquery devuelve varios registros, TRUE si al menos 1 
# IN: si la subquery devuelve varios registros, TRUE si esta incluido
# EXISTS: si la subquery devuelve al menos un registro

dataframeResultado = dd.sql(consultaSQL).df()




#%%-----------
# c.- Listar el nombre, instancia y nota sólo de los estudiantes que no rindieron ningún Recuperatorio

consultaSQL = """
                SELECT e1.nombre, e1.instancia, e1.nota 
                FROM examen AS e1
                WHERE e1.nombre NOT IN (
                        SELECT nombre
                        FROM examen AS e2
                        WHERE e2.instancia ILIKE 'parcial%'
                    )
              """

query = """SELECT nombre
FROM examen AS e1
WHERE e1.instancia ILIKE 'parcial%'"""   # FIX

query2 = """SELECT nombre
FROM examen AS e2
WHERE e2.instancia ILIKE 'recu%'"""   



query1 = dd.sql(query).df()
query2 = dd.sql(query2).df()

# QUERY 3 las que estan en query1 y no estan en query 2, eso meterlo en consultaSQL


dataframeResultado = dd.sql(consultaSQL).df()

#%%===========================================================================
# Ejercicios SQL - Integrando variables de Python
#=============================================================================
# a.- Mostrar Nombre, Instancia y Nota de los alumnos cuya Nota supera el umbral indicado en la variable de Python umbralNota

umbralNota = 7

consultaSQL = f"""
                SELECT nombre, instancia, nota
                FROM examen 
                WHERE nota > {umbralNota}
                """

dataframeResultado = dd.sql(consultaSQL).df()


#%%===========================================================================
# Ejercicios SQL - Manejo de NULLs
#=============================================================================
# a.- Listar todas las tuplas de Examen03 cuyas Notas son menores a 9

consultaSQL = """
                SELECT * FROM examen03 WHERE nota < 9
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# b.- Listar todas las tuplas de Examen03 cuyas Notas son mayores o iguales a 9

consultaSQL = """
                SELECT * FROM examen03 WHERE nota >= 9
              """


dataframeResultado = dd.sql(consultaSQL).df()


#%%-----------
# c.- Listar el UNION de todas las tuplas de Examen03 cuyas Notas son menores a 9 y las que son mayores o iguales a 9

consultaSQL = """
                SELECT * FROM examen03 WHERE nota < 9
                UNION
                SELECT * FROM examen03 WHERE nota >= 9
              """

# no se incluyeron los NULLS

dataframeResultado = dd.sql(consultaSQL).df()


#%%-----------
# d1.- Obtener el promedio de notas

consultaSQL = """
                SELECT AVG(CASE WHEN nota IS NULL THEN 0 ELSE nota END) AS promedio 
                FROM examen03
              """
# NULLS = 0

dataframeResultado = dd.sql(consultaSQL).df()


#%%-----------
# d2.- Obtener el promedio de notas (tomando a NULL==0)

consultaSQL = """
                SELECT AVG(CASE WHEN nota IS NULL THEN 0 ELSE nota END) AS promedio 
                FROM examen03
              """


dataframeResultado = dd.sql(consultaSQL).df()

#%%===========================================================================
# Ejercicios SQL - Mayúsculas/Minúsculas
#=============================================================================
# a.- Consigna: Transformar todos los caracteres de las descripciones de los roles a mayúscula

consultaSQL = """
                SELECT empleado, UPPER(rol) as rol
                FROM empleadoRol
              """

dataframeResultado = dd.sql(consultaSQL).df()

#%%-----------
# b.- Consigna: Transformar todos los caracteres de las descripciones de los roles a minúscula

consultaSQL = """
                SELECT empleado, LOWER(rol) as ROL
                FROM empleadoRol
              """

dataframeResultado = dd.sql(consultaSQL).df()




#%%===========================================================================
# Ejercicios SQL - Reemplazos
#=============================================================================
# a.- Consigna: En la descripción de los roles de los empleados reemplazar las ñ por ni

consultaSQL = """
                  SELECT empleado, REPLACE(rol, 'ñ', 'ni') AS rol  
                  FROM empleadoRol
              """

# REPLACE(campo, ex_valor, new_valor)

dataframeResultado = dd.sql(consultaSQL).df()

#%%---------------

# CONSTANTES

query = """SELECT *, 'valor constante' AS constante FROM examen"""
 
dataframeResultado = dd.sql(query).df()


#%%===========================================================================
# Ejercicios SQL - Desafío
#=============================================================================
# a.- Mostrar para cada estudiante las siguientes columnas con sus datos: Nombre, Sexo, Edad, Nota-Parcial-01, Nota-Parcial-02, Recuperatorio-01 y , Recuperatorio-02

# ... Paso 1: Obtenemos los datos de los estudiantes


# falta considerar los NULLS -> tabla estudiantes, left join por nombre estudiantes-notas

estudiantes = dd.sql('SELECT DISTINCT nombre FROM examen').df()

  # P1
queryNotasParcial1 = """
                SELECT es.nombre, ex.nota AS notaParcial01
                FROM estudiantes es
                LEFT JOIN examen ex ON es.nombre = ex.nombre 
                WHERE instancia = 'Parcial-01'
              """
notasParcial1 = dd.sql(queryNotasParcial1).df()

  # P2              
queryNotasParcial2 = """
                SELECT es.nombre, ex.nota AS notaParcial02
                FROM estudiantes es
                LEFT JOIN examen ex ON es.nombre = ex.nombre 
                WHERE instancia = 'Parcial-02'
              """
notasParcial2 = dd.sql(queryNotasParcial2).df()

  # R1
queryNotasRecu1 = """
                SELECT es.nombre, ex.nota AS notaRecu01
                FROM estudiantes es
                LEFT JOIN examen ex ON es.nombre = ex.nombre 
                WHERE instancia = 'Recuperatorio-01'
              """
notasRecu1 = dd.sql(queryNotasRecu1).df()

  #R2
queryNotasRecu2 = """
                SELECT es.nombre, ex.nota AS notaRecu02
                FROM estudiantes es
                LEFT JOIN examen ex ON es.nombre = ex.nombre 
                WHERE instancia = 'Recuperatorio-02'
              """
notasRecu2 = dd.sql(queryNotasRecu2).df()

  # RESULT
queryResult = """
                SELECT DISTINCT e.nombre, e.sexo, e.edad, np1.notaParcial01, np2.notaParcial02, nr1.notaRecu01, nr2.notaRecu02
                FROM examen e
                LEFT JOIN notasParcial1 np1 ON e.nombre = np1.nombre
                LEFT JOIN notasParcial2 np2 ON e.nombre = np2.nombre
                LEFT JOIN notasRecu1 nr1 ON e.nombre = nr1.nombre
                LEFT JOIN notasRecu2 nr2 ON e.nombre = nr2.nombre
                ORDER BY nombre
              """
result = dd.sql(queryResult).df()





#%% -----------
# b.- Agregar al ejercicio anterior la columna Estado, que informa si el alumno aprobó la cursada (APROBÓ/NO APROBÓ). Se aprueba con 4.

consultaSQL = """
                 SELECT *, CASE WHEN (notaParcial01 >= 4 OR notaRecu01 >= 4) AND (notaParcial02 >= 4 OR notaRecu02 >= 4) THEN 'APROBO' ELSE 'DESAPROBO' END AS estado
                 FROM result
              """

desafio_02 = dd.sql(consultaSQL).df()



#%% -----------
# c.- Generar la tabla Examen a partir de la tabla obtenida en el desafío anterior.


# usando las tablas nombre-instancia, crear col instancia con valor constante con un inner join




p1 = """
                SELECT d2.nombre, d2.sexo, d2.edad, 'Parcial-01' AS instancia, d2.notaParcial01 AS nota
                FROM desafio_02 d2
                INNER JOIN desafio_02 d22 ON d2.nombre = d22.nombre
              """

notasP1 = dd.sql(p1).df()


p2 = """
                SELECT d2.nombre, d2.sexo, d2.edad, 'Parcial-02' AS instancia, d2.notaParcial02 NOT NULL AS nota
                FROM desafio_02 d2
                INNER JOIN desafio_02 d22 ON d2.nombre = d22.nombre
                
              """

notasP2 = dd.sql(p2).df()


R1 = """
                SELECT d2.nombre, d2.sexo, d2.edad, 'Recuperatorio-01' AS instancia, d2.notaRecu01 NOT NULL AS nota
                FROM desafio_02 d2
                INNER JOIN desafio_02 d22 ON d2.nombre = d22.nombre
                
              """

notasR1 = dd.sql(R1).df()



R2 = """
                SELECT d2.nombre, d2.sexo, d2.edad, 'Recuperatorio-02' AS instancia, d2.notaRecu02 NOT NULL AS nota
                FROM desafio_02 d2
                INNER JOIN desafio_02 d22 ON d2.nombre = d22.nombre
                
              """

notasR2 = dd.sql(R2).df()


query_result_final = """
                      SELECT * FROM (SELECT * FROM (SELECT * FROM notasP1 UNION SELECT * FROM notasP2) UNION (SELECT * FROM notasR1)) UNION SELECT * FROM notasR2
                      """ 
                      
result_final = dd.sql(query_result_final).df()