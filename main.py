import sys
from EBase import EBase, prettyPrint

def show_menu():
    print("\n----- MENU DE OPERACIONES EBASE -----")
    print("1. Crear tabla")
    print("2. Listar tablas")
    print("3. Deshabilitar tabla")
    print("4. Verificar si tabla esta habilitada")
    print("5. Alterar tabla")
    print("6. Eliminar tabla")
    print("7. Eliminar todas las tablas")
    print("8. Describir tabla")
    print("9. Insertar datos")
    print("10. Obtener datos")
    print("11. Escanear tabla")
    print("12. Eliminar dato")
    print("13. Eliminar todos los datos de una fila")
    print("14. Contar filas en tabla")
    print("15. Truncar tabla")
    print("16. Salir")
    return input("Seleccione una opcion: ")

def validate_input(prompt, required=True, type_=str):
    while True:
        value = input(prompt)
        if not value.strip() and not required:
            return None
        try:
            return type_(value)
        except ValueError:
            print(f"Error: por favor ingrese un valor valido para {prompt}")

def main():
    db = EBase()
    
    while True:
        option = show_menu()
        
        try:
            if option == '1':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                column_families = validate_input("Ingrese las familias de columnas separadas por coma: ").split(',')
                prettyPrint(db.create(table_name.strip(), [cf.strip() for cf in column_families]))
            
            elif option == '2':
                tables = db.list_tables()
                if tables['data']:
                    prettyPrint(tables)
                else:
                    print("No hay tablas para listar.")
            
            elif option == '3':
                table_name = validate_input("Ingrese el nombre de la tabla a deshabilitar: ")
                prettyPrint(db.disable(table_name.strip()))
            
            elif option == '4':
                table_name = validate_input("Ingrese el nombre de la tabla a verificar: ")
                prettyPrint(db.is_enabled(table_name.strip()))
            
            elif option == '5':
                table_name = validate_input("Ingrese el nombre de la tabla a alterar: ")
                new_name = validate_input("Nuevo nombre de la tabla (presione enter si no desea cambiarlo): ", required=False)
                new_cf = validate_input("Nueva familia de columnas a aniadir (presione enter si no desea aniadir): ", required=False)
                prettyPrint(db.alter(table_name.strip(), new_name.strip() or None, new_cf.strip() or None))
            
            elif option == '6':
                table_name = validate_input("Ingrese el nombre de la tabla a eliminar: ")
                prettyPrint(db.drop(table_name.strip()))
            
            elif option == '7':
                prettyPrint(db.drop_all())
            
            elif option == '8':
                table_name = validate_input("Ingrese el nombre de la tabla a describir: ")
                description = db.describe(table_name.strip())
                if description['success']:
                    prettyPrint(description)
                else:
                    print(description['message'])
            
            elif option == '9':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                column_family = validate_input("Ingrese la familia de columna: ")
                column = validate_input("Ingrese la columna: ")
                value = validate_input("Ingrese el valor: ")
                row_key = validate_input("Ingrese la clave de la fila (opcional, presione enter para generar una nueva): ", required=False)
                prettyPrint(db.put(table_name.strip(), column_family.strip(), column.strip(), value.strip(), row_key.strip() or None))
            
            elif option == '10':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                row_key = validate_input("Ingrese la clave de la fila: ")
                prettyPrint(db.get(table_name.strip(), row_key.strip()))
            
            elif option == '11':
                table_name = validate_input("Ingrese el nombre de la tabla a escanear: ")
                prettyPrint(db.scan(table_name.strip()))
            
            elif option == '12':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                row_key = validate_input("Ingrese la clave de la fila: ")
                column_family = validate_input("Ingrese la familia de columna: ")
                column = validate_input("Ingrese la columna: ")
                prettyPrint(db.delete(table_name.strip(), row_key.strip(), column_family.strip(), column.strip()))
            
            elif option == '13':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                row_key = validate_input("Ingrese la clave de la fila: ")
                prettyPrint(db.delete_all(table_name.strip(), row_key.strip()))
            
            elif option == '14':
                table_name = validate_input("Ingrese el nombre de la tabla: ")
                prettyPrint(db.count(table_name.strip()))
            
            elif option == '15':
                table_name = validate_input("Ingrese el nombre de la tabla a truncar: ")
                prettyPrint(db.truncate(table_name.strip()))
            
            elif option == '16':
                print("Saliendo del programa...")
                break
            
            else:
                print("Opcion no válida. Intente de nuevo.")
        
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == '__main__':
    main()
