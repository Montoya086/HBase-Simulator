import uuid
import os
import json
from typing import Dict, Union
import datetime

def prettyPrint(data:dict) -> str:
        print(json.dumps(data, indent=4))

class EBase:
    def __init__(self, db:str = 'root') -> None:
        self.db = db
        self.relative_path = os.path.join(os.path.dirname(__file__), 'storage', db)
        if not os.path.exists(self.relative_path):
            os.makedirs(self.relative_path)
    
    def table_exists(self, table_name:str) -> Dict[str, Union[bool, str, dict]]:
        """
        Check if a table exists
        @param table_name: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            table_name = table_name.replace(' ', '_')
            if table_name+'.json' in os.listdir(self.relative_path):
                res = {
                    "exists": True
                }
                return {'success': True, 'message': 'Table exists', "data": res}
            else:
                res = {
                    "exists": False
                }
                return {'success': False, 'message': 'Table does not exist', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}

    def create(self, table_name:str, column_families: list[str], max_timestamp: int = 1) -> Dict[str, Union[bool, str, dict]]:
        """
        Create a table
        @param table_name: str
        @param column_families: list[str]
        @param max_timestamp: int (optional) - 1 as default
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try: 
            if self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table already exists', "data": {}}
            else:
                if len(column_families) == 0:
                    return {'success': False, 'message': 'Column families cannot be empty', "data": {}}
                if max_timestamp < 1:
                    return {'success': False, 'message': 'Max timestamp should be greater than 0', "data": {}}
                
                table_name = table_name.replace(' ', '_')
                table_path = os.path.join(self.relative_path, table_name+'.json')
                with open(table_path, 'w') as f:
                    f.write(
                        json.dumps(
                            {
                                "table_metadata": {
                                    "table_name": table_name,
                                    "column_families": column_families,
                                    "table_id": str(uuid.uuid4()),
                                    "disabled": False,
                                    "created_at": str(datetime.datetime.now()),
                                    "updated_at": str(datetime.datetime.now()),
                                    "rows": 0,
                                    "max_timestamp": max_timestamp
                                },
                                "data": {}
                            },
                            indent=4
                        )
                    )
                return {'success': True, 'message': 'Table created successfully', "data": {}}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
        
    def list_tables(self) -> Dict[str, Union[bool, str, dict]]:
        """
        List all tables
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            files = os.listdir(self.relative_path)
            tables = []
            for i in range(len(files)):
                tables.append(files[i].replace('.json', ''))
            res = {
                "tables": tables
            }
            return {'success': True, 'message': 'Tables listed successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
    
    def disable(self, table_name:str) -> Dict[str, Union[bool, str, dict]]:
        """
        Disable a table
        @param table_name: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}

            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            data['table_metadata']['disabled'] = True
            data['table_metadata']['updated_at'] = str(datetime.datetime.now())
            with open(table_path, 'w') as f:
                f.write(json.dumps(data, indent=4))
            return {'success': True, 'message': 'Table disabled successfully', "data": {}}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
    
    def is_enabled(self, table_name:str) -> Dict[str, Union[bool, str, dict]]:
        """
        Check if a table is enabled
        @param table_name: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}

            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            res = {
                "is_enabled": not data['table_metadata']['disabled']
            }
            return {'success': True, 'message': 'Table status fetched successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
    
    def enable(self, table_name:str) -> Dict[str, Union[bool, str, dict]]:
        """
        Enable a table
        @param table_name: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}

            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            data['table_metadata']['disabled'] = False
            data['table_metadata']['updated_at'] = str(datetime.datetime.now())
            with open(table_path, 'w') as f:
                f.write(json.dumps(data, indent=4))
            return {'success': True, 'message': 'Table enabled successfully', "data": {}}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
        
    def alter(self, table_name:str, new_name:str = None, new_column_family:str = None) -> Dict[str, Union[bool, str, dict]]:
        """
        Alter a table
        @param table_name: str
        @param new_name: str (optional)
        @param new_column_family: str (optional)
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            if self.is_enabled(table_name)['data']['is_enabled']:
                return {'success': False, 'message': 'Table is enabled, please disable it first', "data": {}}
            else:
                table_name = table_name.replace(' ', '_')
                table_path = os.path.join(self.relative_path, table_name+'.json')
                with open(table_path, 'r') as f:
                    data = json.load(f)
                if new_name:
                    data['table_metadata']['table_name'] = new_name
                    data['table_metadata']['updated_at'] = str(datetime.datetime.now())
                if new_column_family:
                    data['table_metadata']['column_families'].append(new_column_family)
                    data['table_metadata']['updated_at'] = str(datetime.datetime.now())
                with open(table_path, 'w') as f:
                    f.write(json.dumps(data, indent=4))
                if new_name:
                    os.rename(table_path, os.path.join(self.relative_path, new_name+'.json'))

                return {'success': True, 'message': 'Table altered successfully', "data": {}}

        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
    
    def drop(self, table_name:str) -> Dict[str, Union[bool, str, dict]]:
        """
        Drop a table
        @param table_name: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': f'Table {table_name} does not exist', "data": {}}
            
            if self.is_enabled(table_name)['data']['is_enabled']:
                return {'success': False, 'message': f'Table {table_name} is enabled, please disable it first', "data": {}}
            else:
                table_name = table_name.replace(' ', '_')
                table_path = os.path.join(self.relative_path, table_name+'.json')
                os.remove(table_path)
                return {'success': True, 'message': f'Table {table_name} dropped successfully', "data": {}}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
    
    def drop_all(self) -> Dict[str, Union[bool, str, dict]]:
        """
        Drop all tables
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            tables = self.list_tables()['data']['tables']
            errors = []
            success = []
            for table in tables:
                response = self.drop(table)
                if not response['success']:
                    errors.append(response['message'])
                else: 
                    success.append(response['message'])
            
            res = {
                "success": success,
                "errors": errors
            }
            if len(errors) > 0:
                return {'success': False, 'message': 'Some tables could not be dropped', "data": res}
            
            return {'success': True, 'message': 'All tables dropped successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
    
    def describe(self, table_name:str) -> Dict[str, Union[bool, str, dict]]:
        """
        Describe a table
        @param table_name: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            res = {
                "table_metadata": data['table_metadata']
            }
            return {'success': True, 'message': 'Table described successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}

    def put(self, table_name: str, column_family: str, column: str, value: str, row_key: str = None) -> Dict[str, Union[bool, str, dict]]:
        """
        Insert data into a table
        @param table_name: str
        @param column_family: str
        @param column: str
        @param value: str
        @param row_key: str (optional) - if not provided, a new row key will be generated
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            if not self.is_enabled(table_name)['data']['is_enabled']:
                return {'success': False, 'message': 'Table is disabled, please enable it first', "data": {}}
            
            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            if column_family not in data['table_metadata']['column_families']:
                return {'success': False, 'message': 'Column family does not exist', "data": {}}
            if row_key:
                if row_key is None:
                    row_key = str(uuid.uuid4())
                else:
                    if row_key not in data['data']:
                        return {'success': False, 'message': 'Row key does not exist', "data": {}}
                
                if column_family not in data['data'][row_key]:
                    data['data'][row_key][column_family] = {}

                if column not in data['data'][row_key][column_family]:
                    data['data'][row_key][column_family][column] = {}
                
                old_timestamps = list(data['data'][row_key][column_family][column].keys())
                if len(old_timestamps) >= data['table_metadata']['max_timestamp']:
                    old_timestamps.sort()
                    del data['data'][row_key][column_family][column][old_timestamps[0]]
                data['data'][row_key][column_family][column][str(datetime.datetime.now())] = value
            else:
                row_key = str(uuid.uuid4())
                data['data'][row_key] = {
                    column_family: {
                        column: {
                            str(datetime.datetime.now()): value
                        }
                    }
                }
                data['table_metadata']['rows'] = data['table_metadata']['rows'] + 1
            
            with open(table_path, 'w') as f:
                f.write(json.dumps(data, indent=4))

            res = {
                "row_key": row_key
            }
            return {'success': True, 'message': 'Data inserted successfully', "data": res}

        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}

    def get(self, table_name: str, row_key: str) -> Dict[str, Union[bool, str, dict]]:
        """
        Get data from a table
        @param table_name: str
        @param row_key: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            if row_key not in data['data']:
                return {'success': False, 'message': 'Row key does not exist', "data": {}}
            row_families = data['data'][row_key].keys()
            for family in row_families:
                columns = data['data'][row_key][family].keys()
                for column in columns:
                    timestamps = list(data['data'][row_key][family][column].keys())
                    timestamps.sort()
                    data['data'][row_key][family][column] = data['data'][row_key][family][column][timestamps[-1]]
            res = {
                "data": data['data'][row_key]
            }
            return {'success': True, 'message': 'Data fetched successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
        
    def scan(self, table_name) -> Dict[str, Union[bool, str, dict]]:
        """
        Scan all data from a table
        @param table_name: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            res = {
                "data": data['data']
            }
            return {'success': True, 'message': 'Data scanned successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}

    def delete(self, table_name: str, row_key: str, column_family: str, column: str) -> Dict[str, Union[bool, str, dict]]:
        """
        Delete a cell from a table
        @param table_name: str
        @param row_key: str
        @param column_family: str
        @param column: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """

        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            if not self.is_enabled(table_name)['data']['is_enabled']:
                return {'success': False, 'message': 'Table is disabled, please enable it first', "data": {}}
            
            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            if row_key not in data['data']:
                return {'success': False, 'message': 'Row key does not exist', "data": {}}
            if column_family not in data['data'][row_key]:
                return {'success': False, 'message': 'Column family does not exist', "data": {}}
            if column not in data['data'][row_key][column_family]:
                return {'success': False, 'message': 'Column does not exist', "data": {}}

            data['data'][row_key][column_family][column] = {}
            with open(table_path, 'w') as f:
                f.write(json.dumps(data, indent=4))
            return {'success': True, 'message': 'Data deleted successfully', "data": {}}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}

    def delete_all(self, table_name: str, row_key: str) -> Dict[str, Union[bool, str, dict]]:
        """
        Delete a row from a table
        @param table_name: str
        @param row_key: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            if not self.is_enabled(table_name)['data']['is_enabled']:
                return {'success': False, 'message': 'Table is disabled, please enable it first', "data": {}}
            
            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            if row_key not in data['data']:
                return {'success': False, 'message': 'Row key does not exist', "data": {}}

            del data['data'][row_key]
            data['table_metadata']['rows'] = data['table_metadata']['rows'] - 1
            with open(table_path, 'w') as f:
                f.write(json.dumps(data, indent=4))
            return {'success': True, 'message': 'Row deleted successfully', "data": {}}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
        
    def count(self, table_name: str) -> Dict[str, Union[bool, str, dict]]:
        """
        Count the number of rows in a table
        @param table_name: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            res = {
                "rows": data['table_metadata']['rows']
            }
            return {'success': True, 'message': 'Rows counted successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
        
    def truncate(self, table_name: str) -> Dict[str, Union[bool, str, dict]]:
        """
        Truncate a table
        @param table_name: str
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            start_time = datetime.datetime.now()
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            print("- Disabling table...")
            disable_res = self.disable(table_name)
            if not disable_res['success']:
                return disable_res
            
            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            
            print("- Truncating table...")
            data['data'] = {}
            row_num = data['table_metadata']['rows']
            data['table_metadata']['rows'] = 0
            data['table_metadata']['updated_at'] = str(datetime.datetime.now())
            with open(table_path, 'w') as f:
                f.write(json.dumps(data, indent=4))

            print("- Enabling table...")
            enable_res = self.enable(table_name)
            if not enable_res['success']:
                return enable_res
            res = {
                "time_taken": str(datetime.datetime.now() - start_time),

                "number_of_rows_deleted": row_num
            }
            return {'success': True, 'message': 'Table truncated successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
    
    def insert_many(self, table_name: str, column_family: str, column: str, data:list) -> Dict[str, Union[bool, str, dict]]:
        """
        Insert many rows into a table
        @param table_name: str
        @param column_family: str
        @param column: str
        @param data: list
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            start_time = datetime.datetime.now()
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            if not self.is_enabled(table_name)['data']['is_enabled']:
                return {'success': False, 'message': 'Table is disabled, please enable it first', "data": {}}
            
            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                table_data = json.load(f)

            inserted_cells = []
            for i in range(len(data)):
                row_key = str(uuid.uuid4())
                table_data['data'][row_key] = {
                    column_family: {
                        column: {
                            str(datetime.datetime.now()): data[i]
                        }
                    }
                }
                inserted_cells.append({
                    "row_key": row_key,
                    "column_family": column_family,
                    "column": column,
                    "value": data[i]
                })
                table_data['table_metadata']['rows'] = table_data['table_metadata']['rows'] + 1
            with open(table_path, 'w') as f:
                f.write(json.dumps(table_data, indent=4))
            res = {
                "time_taken": str(datetime.datetime.now() - start_time),
                "number_of_rows_inserted": len(data),
                "inserted_cells": inserted_cells
            }
            return {'success': True, 'message': 'Data inserted successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
    
    def update_many(self, table_name: str, data:list[dict]) -> Dict[str, Union[bool, str, dict]]:
        """
        Update many rows in a table
        @param table_name: str
        @param data: list[dict] - [{'row_key': str, 'column_family': str, 'column': str, 'value': str}]
        @return: dict - {'success': bool, 'message': str, 'data': dict}
        """
        try:
            start_time = datetime.datetime.now()
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}
            
            if not self.is_enabled(table_name)['data']['is_enabled']:
                return {'success': False, 'message': 'Table is disabled, please enable it first', "data": {}}
            
            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                table_data = json.load(f)

            updated_cells = []
            for i in range(len(data)):
                if data[i]['row_key'] not in table_data['data']:
                    return {'success': False, 'message': 'Row key does not exist', "data": {}}
                if data[i]['column_family'] not in table_data['data'][data[i]['row_key']]:
                    return {'success': False, 'message': 'Column family does not exist', "data": {}}
                if data[i]['column'] not in table_data['data'][data[i]['row_key']][data[i]['column_family']]:
                    return {'success': False, 'message': 'Column does not exist', "data": {}}
                
                old_timestamps = list(table_data['data'][data[i]['row_key']][data[i]['column_family']][data[i]['column']].keys())
                if len(old_timestamps) >= table_data['table_metadata']['max_timestamp']:
                    old_timestamps.sort()
                    del table_data['data'][data[i]['row_key']][data[i]['column_family']][data[i]['column']][old_timestamps[0]]
                table_data['data'][data[i]['row_key']][data[i]['column_family']][data[i]['column']][str(datetime.datetime.now())] = data[i]['value']
                updated_cells.append({
                    "row_key": data[i]['row_key'],
                    "column_family": data[i]['column_family'],
                    "column": data[i]['column'],
                    "value": data[i]['value']
                })
            with open(table_path, 'w') as f:
                f.write(json.dumps(table_data, indent=4))
            res = {
                "time_taken": str(datetime.datetime.now() - start_time),
                "number_of_rows_updated": len(data),
                "updated_cells": updated_cells
            }
            return {'success': True, 'message': 'Data updated successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}

db = EBase()

#prettyPrint(db.list_tables())
#prettyPrint(db.create('users', ['personal', 'contact']))
#prettyPrint(db.disable('users'))
#prettyPrint(db.alter('users', new_name='userss', new_column_family='phone'))
#prettyPrint(db.drop('userss'))
#prettyPrint(db.drop_all())
#prettyPrint(db.describe('userss'))
#prettyPrint(db.list_tables())
#prettyPrint(db.enable('userss'))
#prettyPrint(db.is_enabled('userss'))
#prettyPrint(db.put('userss', 'contact', 'phone', '213567123', None))
#prettyPrint(db.get('users', '7939b322-5cee-4d55-83a4-77c59cdfbc78'))
#prettyPrint(db.scan('users'))
#prettyPrint(db.delete('users', '96562231-90e3-4f7f-b472-44de8d81b737', 'personal', 'last_name'))
#prettyPrint(db.delete_all('users', '860e68d0-3083-4740-ac5c-1a3d83280d17'))
#prettyPrint(db.count('users'))
#prettyPrint(db.truncate('users'))
#prettyPrint(db.insert_many('users', 'personal', 'last_name', ['Edison', 'Tesla', 'Newton']))
#prettyPrint(db.update_many('users', [{'row_key': '7939b322-5cee-4d55-83a4-77c59cdfbc78', 'column_family': 'personal', 'column': 'last_name', 'value': 'Einstein'}, {'row_key': '2e7f7ef2-75e5-435b-8e48-72089724799e', 'column_family': 'personal', 'column': 'last_name', 'value': 'Oppenheimer'}]))