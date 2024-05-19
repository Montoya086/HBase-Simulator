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
                            }
                        )
                    )
                return {'success': True, 'message': 'Table created successfully', "data": {}}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
        
    def list(self) -> Dict[str, Union[bool, str, dict]]:
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
                f.write(json.dumps(data))
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
                f.write(json.dumps(data))
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
                    f.write(json.dumps(data))
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
            tables = self.list()['data']['tables']
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
                if row_key not in data['data']:
                    return {'success': False, 'message': 'Row key does not exist', "data": {}}
                if column_family not in data['data'][row_key]:
                    data['data'][row_key][column_family] = {}
                
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
                f.write(json.dumps(data))

            res = {
                "row_key": row_key
            }
            return {'success': True, 'message': 'Data inserted successfully', "data": res}

        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}

    def get(self, table_name: str, row_key: str, column_family: str, column: str) -> Dict[str, Union[bool, str, dict]]:
        """
        Get data from a table
        @param table_name: str
        @param row_key: str (optional)
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

            timestamps = list(data['data'][row_key][column_family][column].keys())
            timestamps.sort()
            last_timestamp = timestamps[-1]
            res = {
                "value": data['data'][row_key][column_family][column][last_timestamp]
            }
            return {'success': True, 'message': 'Data fetched successfully', "data": res}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
        

db = EBase()
#prettyPrint(db.create('users', ['personal', 'contact']))
#prettyPrint(db.disable('users'))
#prettyPrint(db.enable('users'))
#prettyPrint(db.is_enabled('users'))
#prettyPrint(db.alter('users', new_name='userss', new_column_family='phone'))
#prettyPrint(db.drop('userss'))
#prettyPrint(db.drop_all())
#prettyPrint(db.describe('userss'))
#prettyPrint(db.list())

#prettyPrint(db.put('users', 'personal', 'last_name', 'Edison', row_key='860e68d0-3083-4740-ac5c-1a3d83280d17'))
#prettyPrint(db.get('users', '860e68d0-3083-4740-ac5c-1a3d83280d17', 'personal', 'last_name'))