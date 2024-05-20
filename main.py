import sys
from EBase import EBase, prettyPrint

def show_menu():
    print("\n" + "-"*80)
    print("{:^80}".format("MENU DE OPERACIONES EBASE"))
    print("-"*80)
    print("{:<40} {:<40}".format("DDL (Definición de Datos)", "DML (Manipulación de Datos)"))
    print("-"*80)
    print("{:<40} {:<40}".format("1. Crear tabla", "9. Insertar datos"))
    print("{:<40} {:<40}".format("2. Listar tablas", "10. Obtener datos"))
    print("{:<40} {:<40}".format("3. Deshabilitar / Habilitar tabla", "11. Escanear tabla"))
    print("{:<40} {:<40}".format("4. Verificar si tabla está habilitada", "12. Eliminar dato"))
    print("{:<40} {:<40}".format("5. Alterar tabla", "13. Eliminar todos los datos de una fila"))
    print("{:<40} {:<40}".format("6. Eliminar tabla", "14. Contar filas en tabla"))
    print("{:<40} {:<40}".format("7. Eliminar todas las tablas", "15. Truncar tabla"))
    print("{:<40} {:<40}".format("8. Describir tabla", "16. Salir"))
    print("-"*80)
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
                
                prettyPrint(db.create(table_name.strip(), [cf.strip() for cf in column_families]))
            
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
                prettyPrint(db.alter(table_name.strip(), new_name.strip() or None, new_cf.strip() or None))
            
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
                description = db.describe(table_name.strip())
                
                if validate_output(description):
                    print(f"Descripcion de la tabla {table_name}:")
                    prettyPrint(description['data'])
            
            elif option == '9':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                column_family = validate_input("Ingrese la familia de columna: ")
                column = validate_input("Ingrese la columna: ")
                value = validate_input("Ingrese el valor: ")
                row_key = validate_input("Ingrese la clave de la fila (opcional, presione enter para generar una nueva): ", required=False)
                output = db.put(table_name.strip(), column_family.strip(), column.strip(), value.strip(), row_key.strip() or None)
                
                if validate_output(output):
                    print(f"Datos insertados correctamente en la tabla {table_name}.")
            
            # REVISAR
            elif option == '10':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                row_key = validate_input("Ingrese la clave de la fila: ")
                output = db.get(table_name.strip(), row_key.strip())
                
                print(output)
                
                if validate_output(output):
                    print(f"Datos de la fila con clave {row_key} en la tabla {table_name}:")
                    print(output['data'])
            
            elif option == '11':
                table_name = validate_input("Ingrese el nombre de la tabla a escanear: ")
                output = db.scan(table_name.strip())
                
                if validate_output(output):
                    print(output)
            
            elif option == '12':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                row_key = validate_input("Ingrese la clave de la fila: ")
                column_family = validate_input("Ingrese la familia de columna: ")
                column = validate_input("Ingrese la columna: ")
                output = db.delete(table_name.strip(), row_key.strip(), column_family.strip(), column.strip())
                
                if validate_output(output):
                    print(f"Dato eliminado correctamente de la tabla {table_name}.")
            
            elif option == '13':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                row_key = validate_input("Ingrese la clave de la fila: ")
                output = db.delete_all(table_name.strip(), row_key.strip())
                
                if validate_output(output):
                    print(f"Todos los datos de la fila con clave {row_key} en la tabla {table_name} eliminados correctamente.")
                            
            elif option == '14':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                output = db.count(table_name.strip())
                
                if validate_output(output):
                    print(f"Total de filas en la tabla {table_name}: {output['data']['rows']}")
            
            elif option == '15':
                table_name = validate_input("Ingrese el nombre de la tabla a truncar: ")
                output = db.truncate(table_name.strip())
                
                if validate_output(output):
                    print(f"Tabla {table_name} truncada correctamente.")
            
            elif option == '16':
                print("Saliendo del programa...")
                break
            
            else:
                print("Opcion no válida. Intente de nuevo.")
        
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == '__main__':
    main()
