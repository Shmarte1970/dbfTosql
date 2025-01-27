import dbf

# Solicitamos el nombre de la tabla al usuario
nombre_tabla = input("Por favor, introduce el nombre de la tabla DBF (sin la extensión .dbf): ")




try:
    # Abrimos la tabla añadiendo la extensión .dbf
    tabla = dbf.Table(f"{nombre_tabla}.dbf")
    tabla.open()
    
    # Obtenemos la estructura
    estructura = tabla.structure()
    
    # Creamos el nombre del archivo de salida
    nombre_archivo = f"{nombre_tabla}.txt"
    
    # Guardamos la estructura en un archivo txt
    with open(nombre_archivo, 'w') as archivo:        
        archivo.write(str(estructura))
    
    print(f"La estructura se ha guardado exitosamente en el archivo: {nombre_archivo}")
    
    # Cerramos la tabla
    tabla.close()

except dbf.DbfError as e:
    print(f"Error al abrir la tabla: {e}")
except Exception as e:
    print(f"Error inesperado: {e}")