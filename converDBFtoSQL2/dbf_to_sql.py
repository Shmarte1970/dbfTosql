import os
from datetime import datetime
import ast  # Para convertir el string de la estructura a lista
from dbfread import DBF  # Cambiamos dbf por dbfread

def get_structure_from_txt(filename):
    """Lee la estructura desde el archivo txt"""
    try:
        with open(filename, 'r') as f:
            content = f.read()
            # Limpiamos el string de la estructura
            content = content.replace('FieldnameList([', '').replace('])', '')
            fields = content.split(', ')
            
            # Convertimos cada campo a una tupla (nombre, tipo, longitud, decimal)
            field_specs = []
            for field in fields:
                field = field.strip("'")
                parts = field.split()
                name = parts[0]
                type_info = parts[1]
                
                # Procesar tipo y longitud
                if '(' in type_info:
                    type_char = type_info[0]
                    length_info = type_info[2:-1]  # Removemos los paréntesis
                    if ',' in length_info:
                        length, decimal = map(int, length_info.split(','))
                    else:
                        length = int(length_info)
                        decimal = 0
                else:
                    type_char = type_info
                    length = 8 if type_info == 'D' else 1  # Por defecto para fechas y otros tipos
                    decimal = 0
                
                field_specs.append((name, type_char, length, decimal))
            
            print("\nEstructura procesada:")
            for field in field_specs[:5]:  # Mostramos los primeros 5 campos como ejemplo
                print(field)
                
            return field_specs
            
    except Exception as e:
        print(f"Error al leer la estructura: {str(e)}")
        raise

def get_mysql_type(type_char, length, decimal):
    """Determina el tipo MySQL basado en el tipo DBF"""
    if type_char == 'C':  # Character
        if length > 255:
            return "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        return f"VARCHAR({length}) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    elif type_char == 'N':  # Numeric
        if decimal > 0:
            return f"DECIMAL({length},{decimal})"
        return "INT"
    elif type_char == 'D':  # Date
        return "DATE"
    elif type_char == 'L':  # Logical
        return "TINYINT(1)"
    elif type_char == 'M':  # Memo
        return "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    elif type_char == 'I':  # Integer
        return "INT"
    elif type_char == 'V':  # Variable character
        return f"VARCHAR({length}) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    else:
        return "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"



def sanitize_value(value, type_char):
    """Sanitiza los valores según su tipo"""
    if value is None:
        return 'NULL'
    
    # Convertir bytes a string si es necesario
    if isinstance(value, bytes):
        try:
            value = value.decode('utf-8')
        except:
            return 'NULL'
    
    # Para campos lógicos/booleanos
    if type_char == 'L':
        if isinstance(value, bool):
            return '1' if value else '0'
        if isinstance(value, str):
            return '1' if value.upper() in ['T', 'Y', 'S', '1', 'TRUE'] else '0'
        return '1' if value else '0'
    
    # Para campos numéricos
    if type_char in ['N', 'I']:
        if str(value).strip() == '':
            return 'NULL'
        try:
            clean_num = str(value).replace(',', '.').strip()
            num_value = float(clean_num)
            if num_value.is_integer():
                return str(int(num_value))
            return str(num_value)
        except:
            return '0'
    
    # Para campos fecha/hora
    if type_char in ['D', 'T']:
        if str(value).strip() == '':
            return 'NULL'
        try:
            return f"'{str(value)}'"
        except:
            return 'NULL'
    
    # Para todos los demás campos (texto)
    try:
        if not value or str(value).strip() == '':
            return 'NULL'
        
        clean_value = str(value)
        
        # Limpiar el valor
        clean_value = clean_value.replace('\\', '')      # Eliminar barras invertidas
        clean_value = clean_value.replace("'", "''")     # Escapar comillas simples
        clean_value = clean_value.replace('\x00', '')    # Eliminar caracteres nulos
        clean_value = clean_value.replace('\r', ' ')     # Reemplazar retornos de carro
        clean_value = clean_value.replace('\n', ' ')     # Reemplazar saltos de línea
        clean_value = ' '.join(clean_value.split())      # Normalizar espacios
        clean_value = clean_value.strip()
        
        # Eliminar cualquier comilla simple o barra invertida al final
        clean_value = clean_value.rstrip("'\\")
        
        if not clean_value:
            return 'NULL'
        
        # Para campos de texto, asegurarnos de que no excedan la longitud máxima
        if type_char == 'C' and len(clean_value) > 255:
            clean_value = clean_value[:255]
        
        return f"'{clean_value}'"
    except Exception as e:
        print(f"Error sanitizando valor: {str(e)}")
        return 'NULL'

def dbf_to_sql(dbf_path, output_path, table_name, field_specs):
    try:
        # Abrir archivo DBF usando dbfread con codificación específica
        table = DBF(dbf_path, encoding='latin1', char_decode_errors='replace')
        
        with open(output_path, 'w', encoding='utf8') as f:
            # Escribir encabezado SQL
            f.write("SET NAMES utf8mb4;\n")
            f.write("SET FOREIGN_KEY_CHECKS = 0;\n")
            f.write("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';\n")
            f.write("SET time_zone = '+00:00';\n\n")
            
            # Obtener nombres de campos
            field_names = [name for name, _, _, _ in field_specs]
            
            # Crear la estructura de la tabla
            fields = []
            for name, type_char, length, decimal in field_specs:
                mysql_type = get_mysql_type(type_char, length, decimal)
                fields.append(f"`{name}` {mysql_type} DEFAULT NULL")
            
            # Crear tabla
            f.write(f"DROP TABLE IF EXISTS `{table_name}`;\n")
            f.write(f"CREATE TABLE `{table_name}` (\n")
            f.write("  " + ",\n  ".join(fields))
            f.write("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n\n")
            
            # Procesar registros uno por uno
            count = 0
            for record in table:
                try:
                    values = []
                    for name, type_char, _, _ in field_specs:
                        try:
                            value = record.get(name, None)
                            sanitized_value = sanitize_value(value, type_char)
                            values.append(sanitized_value)
                        except Exception as e:
                            print(f"Error en campo {name} del registro {count + 1}: {str(e)}")
                            values.append('NULL')
                    
                    # Generar INSERT individual
                    if len(values) == len(field_specs):
                        fields_str = f"`{'`, `'.join(field_names)}`"
                        values_str = ', '.join(values)
                        sql = f"INSERT INTO `{table_name}` ({fields_str}) VALUES ({values_str});\n"
                        f.write(sql)
                        count += 1
                        
                        if count % 100 == 0:
                            print(f"Procesados {count} registros...")
                    else:
                        print(f"Error: Número incorrecto de valores en registro {count + 1}")
                        
                except Exception as e:
                    print(f"Error procesando registro {count + 1}: {str(e)}")
                    continue
            
            # Escribir pie SQL
            f.write("\nSET FOREIGN_KEY_CHECKS = 1;\n")
        
        print(f"\nArchivo SQL generado exitosamente: {output_path}")
        print(f"Total de registros procesados: {count}")
        
    except Exception as e:
        print("Error durante la conversión:", str(e))
        import traceback
        traceback.print_exc()
        raise

def main():
    try:
        # Listar archivos .txt en el directorio
        txt_files = [f for f in os.listdir('.') if f.endswith('.txt')]
        print("\nArchivos .txt disponibles:")
        for file in txt_files:
            print(f"- {file}")
        
        # Preguntar qué archivo de estructura usar
        structure_file = input("\nIntroduce el nombre del archivo de estructura a usar: Ejemplo Nombre.txt ")
        if not os.path.exists(structure_file):
            print(f"Error: No se encontró el archivo: {structure_file}")
            return
        
        # El nombre base será el mismo que el archivo txt (sin extensión)
        base_name = os.path.splitext(structure_file)[0]
        dbf_file = f"{base_name}.dbf"
        output_sql = f"{base_name}.sql"
        table_name = base_name
        
        if not os.path.exists(dbf_file):
            print(f"Error: No se encontró el archivo: {dbf_file}")
            return
            
        # Intentamos leer la estructura y mostrarla para debug
        print("\nLeyendo estructura del archivo...")
        field_specs = get_structure_from_txt(structure_file)
        print("Estructura leída:")
        print(field_specs)
        
        print(f"\nIniciando conversión de {dbf_file}")
        dbf_to_sql(dbf_file, output_sql, table_name, field_specs)
        print("Conversión completada con éxito")
        
    except Exception as e:
        print(f"Error en la conversión: {str(e)}")
        import traceback
        traceback.print_exc()  # Esto mostrará el stack trace completo

if __name__ == "__main__":
    main()
