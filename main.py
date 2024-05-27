import sys
from EBase import EBase, prettyPrint
import pandas as pd

def show_menu():
    print("\n" + "="*100)
    print("{:^100}".format("MENU DE OPERACIONES EBASE"))
    print("="*100)
    print("{:<50} {:<50}".format("DDL (Definición de Datos)", "DML (Manipulación de Datos)"))
    print("="*100)
    print("{:<50} {:<50}".format("1. Crear tabla", "10. Insertar datos"))
    print("{:<50} {:<50}".format("2. Listar tablas", "11. Obtener datos"))
    print("{:<50} {:<50}".format("3. Deshabilitar / Habilitar tabla", "12. Escanear tabla"))
    print("{:<50} {:<50}".format("4. Verificar si tabla está habilitada", "13. Eliminar dato"))
    print("{:<50} {:<50}".format("5. Alterar tabla", "14. Eliminar todos los datos de una fila"))
    print("{:<50} {:<50}".format("6. Eliminar tabla", "15. Contar filas en tabla"))
    print("{:<50} {:<50}".format("7. Eliminar todas las tablas", "16. Truncar tabla"))
    print("{:<50} {:<50}".format("8. Describir tabla", "17. Alterar muchos"))
    print("{:<50} {:<50}".format("9. Salir", "18. Insertar muchos datos"))
    print("="*100)
    return input("\nSeleccione una opcion: ")

def validate_input(prompt, required=True, type_=str):
    while True:
        value = input(prompt)
        if not value.strip() and not required:
            return None
        try:
            return type_(value)
        except ValueError:
            print(f"\nError: por favor ingrese un valor valido para {prompt}")

def validate_output(output):
    if output['success']:
        return True
    else:
        print(f"\nError: {output['message']}")

def main():
    db = EBase()
    
    while True:
        option = show_menu()
        
        try:
            if option == '1':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                if not table_name.strip():
                    print("\nError: El nombre de la tabla no puede estar vacío.")
                    continue
                
                column_families = validate_input("Ingrese las familias de columnas separadas por coma: ").split(',')
                if any(cf.strip() == "" for cf in column_families):
                    print("\nError: El nombre de una familia de columnas no puede estar vacío.")
                    continue
                number_timestamps = validate_input("Ingrese la cantidad de TimeStamps que desea usar (presione enter si no desea cambiarlo): ", True,  int)
                data = db.create(table_name.strip(), [cf.strip() for cf in column_families], number_timestamps)
                print("\n"+data["message"])
            
            elif option == '2':
                tables = db.list_tables()
                if tables['data']['tables'] == []:
                    print("No hay tablas para listar.")
                elif tables['data']:
                    print(f"Total de tablas: {len(tables['data']['tables'])}")
                    print(f"Nombres de las tablas: {', '.join(tables['data']['tables'])}")
                
            elif option == '3':
                table_name = validate_input("Ingrese el nombre de la tabla a deshabilitar/habilitar: ")
                print("Seleccione una opcion:\n1. Deshabilitar tabla\n2. Habilitar tabla")
                option = input("Opcion: ")
                
                if option == '1':
                    output = db.disable(table_name.strip())
                    if validate_output(output):
                        print(f"Tabla {table_name} deshabilitada correctamente.")
                        
                elif option == '2':
                    output = db.enable(table_name.strip())
                    if validate_output(output):
                        print(f"Tabla {table_name} habilitada correctamente.")
            
            elif option == '4':
                table_name = validate_input("Ingrese el nombre de la tabla a verificar: ")
                output = db.is_enabled(table_name.strip())

                if validate_output(output):
                    print(f"La tabla {table_name} esta habilitada." if output['data']['is_enabled'] else f"La tabla {table_name} no esta habilitada.")
                
            elif option == '5':
                table_name = validate_input("Ingrese el nombre de la tabla a alterar: ")
                new_name = validate_input("Nuevo nombre de la tabla (presione enter si no desea cambiarlo): ", required=False)
                new_cf = validate_input("Nueva familia de columnas a aniadir (presione enter si no desea aniadir): ", required=False)
                if new_cf is None and new_name is None:
                    print("Error: debe ingresar al menos un nuevo nombre o una nueva familia de columnas.")
                else:
                    data = db.alter(table_name.strip(), new_name or None, new_cf or None)
                    print("\n" + data["message"])
            
            elif option == '6':
                table_name = validate_input("Ingrese el nombre de la tabla a eliminar: ")
                output = db.drop(table_name.strip())
                
                if validate_output(output):
                    print(f"Tabla {table_name} eliminada correctamente.")
            
            # REVISAR
            elif option == '7':
                output = db.drop_all()
                if output['success']:
                    print(f"Todas las tablas eliminadas correctamente.")
                else:
                    print('Error:', output['message'])
            
            elif option == '8':
                table_name = validate_input("Ingrese el nombre de la tabla a describir: ")
                descripcion = db.describe(table_name.strip())
                
                if validate_output(descripcion):
                    metadata = descripcion['data']
                    print("\n" + "-"*100)
                    print("{:^100}".format("DESCRIPCIÓN DE LA TABLA"))
                    print("-"*100)
                    print(f"Nombre de la tabla: {metadata['table_metadata']['table_name']}")
                    print(f"Familias de columnas: {', '.join(metadata['table_metadata']['column_families'])}")
                    print(f"ID de la tabla: {metadata['table_metadata']['table_id']}")
                    print(f"Tabla deshabilitada: {'Sí' if metadata['table_metadata']['disabled'] else 'No'}")
                    print(f"Creada en: {metadata['table_metadata']['created_at']}")
                    print(f"Actualizada en: {metadata['table_metadata']['updated_at']}")
                    print(f"Total de filas: {metadata['table_metadata']['rows']}")
                    print(f"Timestamp máximo: {metadata['table_metadata']['max_timestamp']}")
                    print("-"*100+"\n")
                
            elif option == '9':
                print("Saliendo del programa...")
                break
            
            elif option =='10':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                column_family = validate_input("Ingrese la familia de columna: ")
                column = validate_input("Ingrese la columna: ")
                value = validate_input("Ingrese el valor: ")
                row_key = validate_input("Ingrese la clave de la fila (opcional, presione enter para generar una nueva): ")
                output = db.put(table_name.strip(), column_family.strip(), column.strip(), value.strip(), row_key or None)
                print(output)
            
            elif option == '11':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                row_key = validate_input("Ingrese la clave de la fila: ")
                output = db.get(table_name.strip(), row_key.strip())
                
                if validate_output(output):
                    print("\n" + "-"*100)
                    print(f"Table: {table_name}")
                    print(f"Row: {row_key}")
                    print("-"*100)
                    print("{:<32} {:<32} {:<36}".format("Column Family", "Column", "Value"))
                    print("-"*100)

                    data = output['data']['data']
                    for family, columns in data.items():
                        for column, value in columns.items():
                            print("{:<32} {:<32} {:<36}".format(family, column, value))

                    print("-"*100+"\n")

            
            elif option == '12':
                table_name = validate_input("Ingrese el nombre de la tabla a escanear: ")
                output = db.scan(table_name.strip())
                
                if validate_output(output):
                    data = output['data']['data']
                    print("\n" + "-"*100)
                    print("{:^100}".format("Contenido de la tabla: " + table_name))
                    print("-"*100)
                    print("{:<40} {:<60}".format("Row", "Column+Cell"))
                    print("-"*100)

                    for row_key, families in data.items():
                        for family, columns in families.items():
                            for column, timestamps in columns.items():
                                for timestamp, value in timestamps.items():
                                    cell_info = f'column={family}:{column}, ts={timestamp}, value={value}'
                                    print("{:<40} {:<60}".format(row_key, cell_info))

                    print("-"*100+"\n")
                    
            elif option == '13':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                row_key = validate_input("Ingrese la clave de la fila: ")
                column_family = validate_input("Ingrese la familia de columna: ")
                column = validate_input("Ingrese la columna: ")
                output = db.delete(table_name.strip(), row_key.strip(), column_family.strip(), column.strip())
                
                if validate_output(output):
                    print(f"Dato eliminado correctamente de la tabla {table_name}.")
            
            elif option == '14':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                row_key = validate_input("Ingrese la clave de la fila: ")
                output = db.delete_all(table_name.strip(), row_key.strip())
                
                if validate_output(output):
                    print(f"Todos los datos de la fila con clave {row_key} en la tabla {table_name} eliminados correctamente.")
                            
            elif option == '15':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                output = db.count(table_name.strip())
                
                if validate_output(output):
                    print(f"Total de filas en la tabla {table_name}: {output['data']['rows']}")
            
            elif option == '16':
                table_name = validate_input("Ingrese el nombre de la tabla a truncar: ")
                output = db.truncate(table_name.strip())
                
                if validate_output(output):
                    print(f"Tabla {table_name} truncada correctamente.")
            
            elif option == '17':
                path = validate_input("Ingrese el nombre de su csv con los datos a modificar: ")
                table_name = validate_input("Ingrese el nombre de la tabla a insertar los datos: ")
                df = pd.read_csv(path + '.csv')
                updates = df.to_dict(orient='records')
                result = db.update_many(table_name, updates)
                if validate_output(result):
                    rows = result['data']['number_of_rows_updated']
                    data = result['data']['updated_cells']
                    print("\n" + "-"*100)
                    print("{:^100}".format("Datos insertados correctamente en la tabla " + table_name))
                    print("{:^100}".format("La cantidad de filas actualizadas " + str(rows)))
                    print("-"*100)
                    print("{:<40} {:<60}".format("Row", "Column+Cell"))
                    print("-"*100)
                    # Iterar directamente sobre la lista de diccionarios
                    for cell in data:
                        row_key = cell['row_key']
                        family = cell['column_family']
                        column = cell['column']
                        value = cell['value']
                        cell_value = cell['value'] # Obtiene el valor insertado
                        cell_info = f'column={family}:{column}, value={cell_value}'
                        print("{:<40} {:<60}".format(row_key, cell_info))

                    print("-"*100+"\n")
            
            elif option == '18':
                path = validate_input("Ingrese el nombre de su csv con los datos a ingresar: ")
                df = pd.read_csv(path + '.csv')
                table_name = validate_input("Ingrese el nombre de la tabla a insertar los datos: ")
                column_family = validate_input("Ingrese la familia de columna: ")
                column = validate_input("Ingrese la columna: ")
                data_to_insert = df[column].tolist()
                result = db.insert_many(table_name, column_family, column, data_to_insert)
                if validate_output(result):
                    data = result['data']['inserted_cells']
                    print("\n" + "-"*100)
                    print("{:^100}".format("Contenido insertado en la tabla: " + table_name))
                    print("-"*100)
                    print("{:<40} {:<60}".format("Row", "Column+Cell"))
                    print("-"*100)
                # Iterar directamente sobre la lista de diccionarios
                    for cell in data:
                        row_key = cell['row_key']
                        family = cell['column_family']
                        column = cell['column']
                        value = cell['value']
                        cell_value = cell['value'] # Obtiene el valor insertado
                        cell_info = f'column={family}:{column}, value={cell_value}'
                        print("{:<40} {:<60}".format(row_key, cell_info))

                    print("-"*100+"\n")
            
            else:
                print("Opcion no válida. Intente de nuevo.")
        
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == '__main__':
    main()
