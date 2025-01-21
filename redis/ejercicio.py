import json
import redis

conexionRedis = redis.ConnectionPool(host='localhost', port=6379, db=0,decode_responses=True)

baseDatosRedis = redis.Redis(connection_pool=conexionRedis)

# Crear registros clave-valor(0.5 puntos)

baseDatosRedis.set('paciente:1:nombre', 'Juan Perez')
baseDatosRedis.set('paciente:1:edad', 30)
baseDatosRedis.set('paciente:1:diagnostico', 'Hipertensión')

baseDatosRedis.set('paciente:2:nombre', 'Maria Gomez')
baseDatosRedis.set('paciente:2:edad', 25)
baseDatosRedis.set('paciente:2:diagnostico', 'Diabetes')

baseDatosRedis.set('paciente:3:nombre', 'Carlos Ruiz')
baseDatosRedis.set('paciente:3:edad', 40)
baseDatosRedis.set('paciente:3:diagnostico', 'Asma')
print('Ejercicio 1 - Registros creados')

# 2 - Obtener y mostrar el número de claves registradas (0.5 puntos)
num_claves = len(baseDatosRedis.keys('*'))
print(f'Ejercicio 2 - Número de claves registradas: {num_claves}')

# 3 - Obtener y mostrar un registro en base a una clave (0.5 puntos)
clave = 'paciente:1:nombre'
valor = baseDatosRedis.get(clave)
print(f'Ejercicio 3 - Valor de la clave "{clave}": {valor}')

# 4 - Actualizar el valor de una clave y mostrar el nuevo valor(0.5 puntos)
baseDatosRedis.set('paciente:1:edad', 31)
nuevo_valor = baseDatosRedis.get('paciente:1:edad')
print(f'Ejercicio 4 - Nuevo valor de la clave "paciente:1:edad": {nuevo_valor}')

# 5 - Eliminar una clave-valor y mostrar la clave y el valor eliminado(0.5 puntos)
clave_a_eliminar = 'paciente:2:diagnostico'
valor_eliminado = baseDatosRedis.get(clave_a_eliminar)
baseDatosRedis.delete(clave_a_eliminar)
print(f'Ejercicio 5 - Clave eliminada: {clave_a_eliminar}, Valor eliminado: {valor_eliminado}')

#6- Obtener y mostrar todas las claves guardadas (0.5 puntos)
todas_las_claves = baseDatosRedis.keys('*')
print('Ejercicio 6 - Todas las claves guardadas:', todas_las_claves)

#7 - Obtener y mostrar todos los valores guardados(0.5 puntos)
todos_los_valores = [baseDatosRedis.get(clave) for clave in todas_las_claves]
print('Ejercicio 7 - Todos los valores guardados:', todos_los_valores)

#8 - Obtener y mostrar varios registros con una clave con un patrón en común usando * (0.5 puntos)
patron = 'paciente:*:nombre'
registros_con_patron = baseDatosRedis.keys(patron)
print(f'Ejercicio 8 - Registros con el patrón "{patron}": {registros_con_patron}')

#9 - Obtener y mostrar varios registros con una clave con un patrón en común usando [] (0.5 puntos)
patron_con_corchetes = 'paciente:[1-3]:nombre'
registros_con_patron_corchetes = baseDatosRedis.keys(patron_con_corchetes)
print(f'Ejercicio 9 - Registros con el patrón "{patron_con_corchetes}": {registros_con_patron_corchetes}')

#10 - Obtener y mostrar varios registros con una clave con un patrón en común usando ? (0.5 puntos)
patron_con_interrogacion = 'paciente:?:nombre'
registros_con_patron_interrogacion = baseDatosRedis.keys(patron_con_interrogacion)
print(f'Ejercicio 10 - Registros con el patrón "{patron_con_interrogacion}": {registros_con_patron_interrogacion}')

#11 - Obtener y mostrar varios registros y filtrarlos por un valor en concreto. (0.5 puntos)
valor_a_filtrar = 'Hipertensión'
claves_filtradas = [clave for clave in baseDatosRedis.keys('paciente:*:diagnostico') if baseDatosRedis.get(clave) == valor_a_filtrar]
print(f'Ejercicio 11 - Claves con el valor "{valor_a_filtrar}": {claves_filtradas}')

#12 - Actualizar una serie de registros en base a un filtro (por ejemplo aumentar su valor en 1 )(0.5 puntos)
claves_a_actualizar = baseDatosRedis.keys('paciente:*:edad')
for clave in claves_a_actualizar:
    valor_actual = int(baseDatosRedis.get(clave))
    nuevo_valor = valor_actual + 1
    baseDatosRedis.set(clave, nuevo_valor)
    print(f'Ejercicio 12 - Clave actualizada: {clave}, Antiguo valor: {valor_actual}, Nuevo valor: {nuevo_valor}')
#13 - Eliminar una serie de registros en base a un filtro (0.5 puntos)
claves_a_eliminar = [clave for clave in baseDatosRedis.keys('paciente:*:diagnostico') if baseDatosRedis.get(clave) == 'Asma']
for clave in claves_a_eliminar:
    baseDatosRedis.delete(clave)
    print(f'Ejercicio 13 - Clave eliminada: {clave}')
#14 - Crear una estructura en JSON de array de los datos que vayais a almacenar(0.5 puntos)
pacientes = [
    {
        "id": 1,
        "nombre": "Juan Perez",
        "edad": 31,
        "diagnostico": "Hipertensión"
    },
    {
        "id": 2,
        "nombre": "Maria Gomez",
        "edad": 25,
        "diagnostico": "Diabetes"
    },
    {
        "id": 3,
        "nombre": "Carlos Ruiz",
        "edad": 40,
        "diagnostico": "Asma"
    }
]

json_pacientes = json.dumps(pacientes)
baseDatosRedis.set('pacientes', json_pacientes)
print('Ejercicio 14 - Estructura JSON creada y almacenada')
#15 - Realizar un filtro por cada atributo de la estructura JSON anterior (0.5 puntos)
# Filtrar por nombre
nombre_a_filtrar = 'Juan Perez'
pacientes_filtrados_por_nombre = [paciente for paciente in pacientes if paciente['nombre'] == nombre_a_filtrar]
print(f'Ejercicio 15 - Pacientes con el nombre "{nombre_a_filtrar}": {pacientes_filtrados_por_nombre}')

# Filtrar por edad
edad_a_filtrar = 25
pacientes_filtrados_por_edad = [paciente for paciente in pacientes if paciente['edad'] == edad_a_filtrar]
print(f'Ejercicio 15 - Pacientes con la edad "{edad_a_filtrar}": {pacientes_filtrados_por_edad}')

# Filtrar por diagnóstico
diagnostico_a_filtrar = 'Diabetes'
pacientes_filtrados_por_diagnostico = [paciente for paciente in pacientes if paciente['diagnostico'] == diagnostico_a_filtrar]
print(f'Ejercicio 15 - Pacientes con el diagnóstico "{diagnostico_a_filtrar}": {pacientes_filtrados_por_diagnostico}')
#16 - Crear una lista en Redis (0.5 puntos)
pacientes_lista = ['Juan Perez', 'Maria Gomez', 'Carlos Ruiz']
for paciente in pacientes_lista:
    baseDatosRedis.rpush('lista_pacientes', paciente)
print('Ejercicio 16 - Lista creada y almacenada en Redis')
#17 - Obtener elementos de una lista con un filtro en concreto(0.5 puntos)
# Obtener elementos de la lista que contienen la letra 'a'
filtro = 'a'
elementos_filtrados = [paciente for paciente in baseDatosRedis.lrange('lista_pacientes', 0, -1) if filtro in paciente]
print(f'Ejercicio 17 - Elementos de la lista que contienen "{filtro}": {elementos_filtrados}')
# 18 - En Redis hay otras formas de almacenar datos: Set, Hashes, SortedSet,Streams, Geopatial, Bitmaps, Bitfields,Probabilistic y Time Series. Elige dos de estos tipos, y crea una función que los guarde en la base de datos y otra que los obtenga. (1.5 puntos)
print('Ejercicio 18 - Funciones para guardar y obtener Set y SortedSet en Redis')
# Funciones para Set
def guardar_set(redis_db, nombre_set, valores):
    for valor in valores:
        redis_db.sadd(nombre_set, valor)
    print(f'Set "{nombre_set}" guardado en Redis')

def obtener_set(redis_db, nombre_set):
    valores = redis_db.smembers(nombre_set)
    print(f'Valores del set "{nombre_set}": {valores}')
    return valores

# Funciones para SortedSet
def guardar_sorted_set(redis_db, nombre_sorted_set, valores_con_puntuacion):
    for valor, puntuacion in valores_con_puntuacion.items():
        redis_db.zadd(nombre_sorted_set, {valor: puntuacion})
    print(f'SortedSet "{nombre_sorted_set}" guardado en Redis')

def obtener_sorted_set(redis_db, nombre_sorted_set):
    valores = redis_db.zrange(nombre_sorted_set, 0, -1, withscores=True)
    print(f'Valores del SortedSet "{nombre_sorted_set}": {valores}')
    return valores

# Ejemplo de uso de Set con nombres de pacientes
valores_set = {'Juan Perez', 'Maria Gomez', 'Carlos Ruiz'}
guardar_set(baseDatosRedis, 'nombres_pacientes_set', valores_set)
obtener_set(baseDatosRedis, 'nombres_pacientes_set')

# Ejemplo de uso de SortedSet con edades de pacientes
valores_sorted_set = {'Juan Perez': 31, 'Maria Gomez': 25, 'Carlos Ruiz': 40}
guardar_sorted_set(baseDatosRedis, 'edades_pacientes_sorted_set', valores_sorted_set)
obtener_sorted_set(baseDatosRedis, 'edades_pacientes_sorted_set')
