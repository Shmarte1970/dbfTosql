from dbfread import DBF
import os

# Listar archivos DBF en el directorio
dbf_files = [f for f in os.listdir('.') if f.lower().endswith('.dbf')]
print("\nArchivos DBF disponibles:")
for file in dbf_files:
    print(f"- {file}")

# Solicitamos el nombre de la tabla al usuario
nombre_tabla = input("\nPor favor, introduce el nombre de la tabla DBF (sin la extensión .dbf): ")

try:
    # Abrimos la tabla añadiendo la extensión .dbf
    tabla = DBF(f"{nombre_tabla}.dbf")
    
    # Obtenemos la estructura básica
    estructura = []
    for field in tabla.fields:
        # Simplemente guardamos nombre, tipo y longitud
        estructura.append(f"'{field.name} {field.type}({field.length})'")
    
    # Creamos el nombre del archivo de salida
    nombre_archivo = f"{nombre_tabla}.txt"
    
    # Guardamos la estructura en un archivo txt
    with open(nombre_archivo, 'w') as archivo:
        archivo.write(f"FieldnameList([{', '.join(estructura)}])")
    
    print(f"La estructura se ha guardado exitosamente en el archivo: {nombre_archivo}")
    
    # Mostrar la estructura por consola para verificar
    print("\nEstructura de la tabla:")
    for campo in estructura:
        print(campo)

except Exception as e:
    print(f"Error inesperado: {e}")
    import traceback
    traceback.print_exc()