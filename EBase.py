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

    def create(self, table_name:str, column_families: list[str]) -> Dict[str, Union[bool, str, dict]]:
        try: 
            if self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table already exists', "data": {}}
            else:
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
                                },
                                "data": {}
                            }
                        )
                    )
                return {'success': True, 'message': 'Table created successfully', "data": {}}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
        
    def list(self) -> Dict[str, Union[bool, str, dict]]:
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
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}

            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            data['table_metadata']['disabled'] = True
            with open(table_path, 'w') as f:
                f.write(json.dumps(data))
            return {'success': True, 'message': 'Table disabled successfully', "data": {}}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
    
    def is_enabled(self, table_name:str) -> Dict[str, Union[bool, str, dict]]:
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
        try:
            if not self.table_exists(table_name)['data']['exists']:
                return {'success': False, 'message': 'Table does not exist', "data": {}}

            table_name = table_name.replace(' ', '_')
            table_path = os.path.join(self.relative_path, table_name+'.json')
            with open(table_path, 'r') as f:
                data = json.load(f)
            data['table_metadata']['disabled'] = False
            with open(table_path, 'w') as f:
                f.write(json.dumps(data))
            return {'success': True, 'message': 'Table enabled successfully', "data": {}}
        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
        
    def alter(self, table_name:str, new_name:str = None, new_column_family:str = None) -> Dict[str, Union[bool, str, dict]]:
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
                if new_column_family:
                    data['table_metadata']['column_families'].append(new_column_family)
                with open(table_path, 'w') as f:
                    f.write(json.dumps(data))
                if new_name:
                    os.rename(table_path, os.path.join(self.relative_path, new_name+'.json'))

                return {'success': True, 'message': 'Table altered successfully', "data": {}}

        except Exception as e:
            return {'success': False, 'message': str(e), "data": {}}
    
    def drop(self, table_name:str) -> Dict[str, Union[bool, str, dict]]:
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
    

db = EBase()
#prettyPrint(db.create('userss', ['name', 'age', 'email']))
#prettyPrint(db.disable('users'))
#prettyPrint(db.enable('users'))
#prettyPrint(db.is_enabled('users'))
#prettyPrint(db.alter('users', new_name='userss', new_column_family='phone'))
#prettyPrint(db.drop('userss'))
#prettyPrint(db.drop_all())