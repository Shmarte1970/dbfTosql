import dbf
import os
from datetime import datetime
import re

def create_field_specs():
    """Crea las especificaciones de campos manualmente basado en la estructura conocida"""
    return [
        ('ORDRE', 'N', 6, 0),
        ('CONT', 'C', 13, 0),
        ('CLASE', 'C', 3, 0),
        ('TIPO', 'C', 5, 0),
        ('LONG', 'N', 3, 0),
        ('ALT', 'N', 4, 2),
        ('CARGA', 'N', 6, 0),
        ('TARA', 'N', 6, 0),
        ('OK', 'C', 2, 0),
        ('ACEP', 'C', 1, 0),
        ('ECLI', 'N', 6, 0),
        ('FENT', 'D', 8, 0),
        ('FSAL', 'D', 8, 0),
        ('ES', 'C', 1, 0),
        ('ITRANS', 'N', 8, 2),
        ('ITRANSSAL', 'N', 8, 2),
        ('HORA', 'C', 5, 0),
        ('HORASAL', 'C', 5, 0),
        ('TRANS', 'C', 15, 0),
        ('TRANSAL', 'C', 15, 0),
        ('MAT', 'C', 11, 0),
        ('MATSAL', 'C', 11, 0),
        ('CIFCOND', 'C', 20, 0),
        ('CIFCONDSAL', 'C', 20, 0),
        ('CONDUCTOR', 'C', 31, 0),
        ('CONDSAL', 'C', 31, 0),
        ('RESERVA', 'C', 17, 0),
        ('RESAL', 'C', 25, 0),
        ('OBS1', 'C', 61, 0),
        ('OBS2', 'C', 61, 0),
        ('OBSAL1', 'C', 61, 0),
        ('OBSAL2', 'C', 61, 0),
        ('ENOM', 'C', 41, 0),
        ('EXISTE', 'C', 1, 0),
        ('REPARAT', 'D', 8, 0),
        ('MATRICULA', 'C', 13, 0),
        ('TFA', 'C', 3, 0),
        ('OBS_TIPUS', 'C', 4, 0),
        ('OBS_DPP', 'C', 13, 0),
        ('OBS_DATA', 'D', 8, 0),
        ('OBS_CLIENT', 'C', 29, 0),
        ('OBS_REF', 'C', 15, 0),
        ('CAI', 'C', 1, 0),
        ('OBS_REFS', 'C', 15, 0),
        ('FABR', 'C', 7, 0),
        ('FRA_A', 'N', 6, 0),
        ('DEPOT_SORT', 'C', 11, 0),
        ('REVISIO', 'N', 6, 0),
        ('M_OBRA', 'N', 6, 0),
        ('VCONT', 'C', 13, 0),
        ('ENVIAR', 'C', 9, 0),
        ('LESSOR', 'C', 9, 0),
        ('DEPOT', 'C', 9, 0),
        ('OKM', 'C', 2, 0),
        ('REP_CAIXA', 'D', 8, 0),
        ('REP_MAQ', 'D', 8, 0),
        ('QAUTORIZ', 'C', 10, 0),
        ('CAI_CAIXA', 'N', 6, 0),
        ('CAI_MAQ', 'N', 6, 0),
        ('TOTCAI', 'N', 7, 2),
        ('TOTCAIM', 'N', 7, 2),
        ('PCLI1', 'N', 6, 0),
        ('PCLI2', 'N', 6, 0),
        ('NAUTORIZ', 'C', 10, 0),
        ('GRADO', 'C', 12, 0),
        ('PRI', 'D', 8, 0),
        ('AUTHLESSEE', 'D', 8, 0),
        ('AUTHLESOR', 'D', 8, 0),
        ('RENTAT', 'D', 8, 0),
        ('VENDA', 'D', 8, 0),
        ('SURVLESSEE', 'D', 8, 0),
        ('SURVLESOR', 'D', 8, 0),
        ('REPARATM', 'D', 8, 0),
        ('ENTR_NUM', 'C', 5, 0),
        ('SORT_NUM', 'C', 5, 0),
        ('CONDICIO', 'C', 5, 0),
        ('MODIFICAT', 'D', 8, 0),
        ('AMMO', 'C', 2, 0),
        ('VTALLER', 'C', 20, 0),
        ('PORTIC', 'C', 18, 0),
        ('TEMP_MAX', 'N', 3, 0),
        ('TEMP_MIN', 'N', 3, 0),
        ('NUM_REF', 'N', 6, 0),
        ('ASSIGNAR', 'L', 1, 0),
        ('NO_ASSIGN', 'D', 8, 0),
        ('INSPECCIO', 'D', 8, 0),
        ('ENTREGUESE', 'C', 20, 0),
        ('PRECINTEE', 'C', 15, 0),
        ('ADMITASE', 'C', 20, 0),
        ('PRECINTES', 'C', 15, 0),
        ('OBSWEB', 'C', 60, 0)
    ]

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
    else:
        return "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"

def sanitize_value(value, type_char):
    """Sanitiza los valores según su tipo"""
    if value is None:
        return 'NULL'
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, datetime):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    elif isinstance(value, dbf.Date):
        if str(value).strip() == '':
            return 'NULL'
        return f"'{value.strftime('%Y-%m-%d')}'"
    elif type_char == 'L':  # Logical/Boolean
        return '1' if str(value).upper() in ['T', 'Y', 'S', '1'] else '0'
    else:
        # Sanitizar string
        clean_value = str(value)
        clean_value = clean_value.replace("'", "''")  # Escapar comillas simples
        clean_value = clean_value.replace('\x00', '')  # Eliminar caracteres nulos
        clean_value = clean_value.strip()
        return f"'{clean_value}'" if clean_value else 'NULL'

def dbf_to_sql(dbf_path, output_path, table_name):
    try:
        # Abrir archivo DBF
        table = dbf.Table(dbf_path)
        table.open()
        
        # Obtener las especificaciones de campos
        field_specs = create_field_specs()
        
        with open(output_path, 'w', encoding='utf8') as f:
            # Escribir encabezado SQL
            f.write("SET NAMES utf8mb4;\n")
            f.write("SET FOREIGN_KEY_CHECKS = 0;\n")
            f.write("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';\n")
            f.write("SET time_zone = '+00:00';\n\n")
            
            # Crear la estructura de la tabla
            fields = []
            for name, type_char, length, decimal in field_specs:
                mysql_type = get_mysql_type(type_char, length, decimal)
                fields.append(f"`{name}` {mysql_type} DEFAULT NULL")
                print(f"Campo agregado: {name}, Tipo MySQL: {mysql_type}")
            
            # Crear tabla
            f.write(f"DROP TABLE IF EXISTS `{table_name}`;\n")
            f.write(f"CREATE TABLE `{table_name}` (\n")
            f.write("  " + ",\n  ".join(fields))
            f.write("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n\n")
            
            # Insertar datos
            batch_size = 100
            records = []
            count = 0
            
            for record in table:
                values = []
                for name, type_char, _, _ in field_specs:
                    value = record[name]
                    values.append(sanitize_value(value, type_char))
                
                records.append(f"({', '.join(values)})")
                count += 1
                
                if len(records) >= batch_size:
                    f.write(f"INSERT INTO `{table_name}` VALUES {', '.join(records)};\n")
                    print(f"Procesados {count} registros...")
                    records = []
            
            # Escribir registros restantes
            if records:
                f.write(f"INSERT INTO `{table_name}` VALUES {', '.join(records)};\n")
            
            # Escribir pie SQL
            f.write("\nSET FOREIGN_KEY_CHECKS = 1;\n")
        
        table.close()
        print(f"\nArchivo SQL generado exitosamente: {output_path}")
        print(f"Total de registros procesados: {count}")
        
    except Exception as e:
        print("Error durante la conversión:", str(e))
        import traceback
        traceback.print_exc()
        if 'table' in locals():
            table.close()
        raise

def main():
    # Configuración
    dbf_file = "contenid.dbf"
    output_sql = "contenid.sql"
    table_name = "contenid"
    
    if not os.path.exists(dbf_file):
        print(f"Error: No se encontró el archivo: {dbf_file}")
        return
    
    try:
        print(f"Iniciando conversión de {dbf_file}")
        dbf_to_sql(dbf_file, output_sql, table_name)
        print("Conversión completada con éxito")
    except Exception as e:
        print(f"Error en la conversión: {str(e)}")

if __name__ == "__main__":
    main()