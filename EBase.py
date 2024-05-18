import uuid
import os
import json
from typing import Dict, Union
import datetime

class EBase:
    def __init__(self, db:str = 'root') -> None:
        self.db = db
        self.relative_path = os.path.join(os.path.dirname(__file__), 'storage', db)
        if not os.path.exists(self.relative_path):
            os.makedirs(self.relative_path)

    def create(self, table_name:str, column_families: list[str]) -> Dict[str, Union[bool, str, dict]]:
        try: 
            if table_name in os.listdir(self.relative_path):
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
    