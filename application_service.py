# application_service.py
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from domain_models import (
    User, UserRole,
    Device, DeviceStatus, DeviceType,
    DataSource, DataSourceType,
    IoTNetwork, AnalysisResult
)
from gateways import (
    UserFinder, UserGateway,
    DeviceFinder, DeviceGateway,
    IoTNetworkFinder, IoTNetworkGateway,
    DataSourceFinder, DataSourceGateway,
    AnalysisFinder, AnalysisGateway
)


class IoTNetworkApplicationService:
    """
    Сервисный слой для координации между Domain Model и Row Data Gateway
    Реализует прецеденты использования
    """
    
    def __init__(self, db_path: str = 'iot_network.db'):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_database()
    
    def _init_database(self):
        """Инициализировать структуру БД"""
        cursor = self.conn.cursor()
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                login TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin', 'analyst'))
            )
        ''')
        
        # Таблица IoT сетей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS iot_networks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                description TEXT,
                network_name TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''')
        
        # Таблица устройств
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_name TEXT NOT NULL,
                status TEXT NOT NULL,
                type TEXT NOT NULL,
                network_id INTEGER NOT NULL,
                FOREIGN KEY (network_id) REFERENCES iot_networks (id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица связей между устройствами
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS device_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id INTEGER NOT NULL,
                connected_device_id INTEGER NOT NULL,
                FOREIGN KEY (device_id) REFERENCES devices (id) ON DELETE CASCADE,
                UNIQUE(device_id, connected_device_id)
            )
        ''')
        
        # Таблица источников данных
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                datasource_name TEXT NOT NULL,
                last_update TEXT NOT NULL,
                type TEXT NOT NULL,
                network_id INTEGER NOT NULL,
                FOREIGN KEY (network_id) REFERENCES iot_networks (id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица результатов анализа
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                centrality_score REAL NOT NULL,
                date TEXT NOT NULL,
                network_id INTEGER NOT NULL,
                FOREIGN KEY (network_id) REFERENCES iot_networks (id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица изолированных узлов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS isolated_nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id INTEGER NOT NULL,
                device_id INTEGER NOT NULL,
                FOREIGN KEY (analysis_id) REFERENCES analysis (id) ON DELETE CASCADE,
                FOREIGN KEY (device_id) REFERENCES devices (id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица избыточных связей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS redundant_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id INTEGER NOT NULL,
                device_id1 INTEGER NOT NULL,
                device_id2 INTEGER NOT NULL,
                FOREIGN KEY (analysis_id) REFERENCES analysis (id) ON DELETE CASCADE,
                FOREIGN KEY (device_id1) REFERENCES devices (id) ON DELETE CASCADE,
                FOREIGN KEY (device_id2) REFERENCES devices (id) ON DELETE CASCADE
            )
        ''')
        
        # Создаем тестовых пользователей, если нет
        cursor.execute("SELECT COUNT(*) FROM users WHERE login='admin'")
        if cursor.fetchone()[0] == 0:
            cursor.execute('''
                INSERT INTO users (name, login, password, role)
                VALUES (?, ?, ?, ?)
            ''', ('Администратор', 'admin', 'admin123', 'admin'))
            
            cursor.execute('''
                INSERT INTO users (name, login, password, role)
                VALUES (?, ?, ?, ?)
            ''', ('Аналитик', 'analyst', 'analyst123', 'analyst'))
        
        self.conn.commit()
    
    # ===== МЕТОДЫ ДЛЯ УСТРОЙСТВ =====
    
    def add_device(self, network_id: int, device_data: Dict[str, Any]) -> Dict[str, Any]:
        """Добавить устройство вручную"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute('''
                INSERT INTO devices (device_name, status, type, network_id)
                VALUES (?, ?, ?, ?)
            ''', (
                device_data['name'],
                device_data.get('status', 'active'),
                device_data['type'],
                network_id
            ))
            
            device_id = cursor.lastrowid
            
            # Сохранить связи
            connections = device_data.get('connections', [])
            for connected_id in connections:
                cursor.execute('''
                    INSERT INTO device_connections (device_id, connected_device_id)
                    VALUES (?, ?)
                ''', (device_id, connected_id))
            
            self.conn.commit()
            
            return {
                'success': True,
                'device_id': device_id,
                'message': f'Устройство "{device_data["name"]}" успешно добавлено'
            }
            
        except Exception as e:
            self.conn.rollback()
            return {'success': False, 'error': str(e)}
    
    def remove_device(self, device_id: int) -> Dict[str, Any]:
        """Удалить устройство из сети"""
        try:
            cursor = self.conn.cursor()
            
            # Получить имя устройства перед удалением
            cursor.execute('SELECT device_name FROM devices WHERE id = ?', (device_id,))
            row = cursor.fetchone()
            
            if not row:
                return {'success': False, 'error': f'Устройство с ID {device_id} не найдено'}
            
            device_name = row[0]
            
            # Удалить устройство (каскадно удалит связи)
            cursor.execute('DELETE FROM devices WHERE id = ?', (device_id,))
            
            self.conn.commit()
            
            return {
                'success': True,
                'message': f'Устройство "{device_name}" удалено'
            }
            
        except Exception as e:
            self.conn.rollback()
            return {'success': False, 'error': str(e)}
    
    # ===== МЕТОДЫ ДЛЯ ИСТОЧНИКОВ ДАННЫХ =====
    
    def add_data_source(self, network_id: int, source_data: Dict[str, Any]) -> Dict[str, Any]:
        """Добавить источник данных"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute('''
                INSERT INTO data_sources (datasource_name, last_update, type, network_id)
                VALUES (?, ?, ?, ?)
            ''', (
                source_data['name'],
                source_data.get('last_update', datetime.now().isoformat()),
                source_data['type'],
                network_id
            ))
            
            source_id = cursor.lastrowid
            
            self.conn.commit()
            
            return {
                'success': True,
                'source_id': source_id,
                'message': f'Источник данных "{source_data["name"]}" успешно добавлен'
            }
            
        except Exception as e:
            self.conn.rollback()
            return {'success': False, 'error': str(e)}
    
    def remove_data_source(self, source_id: int) -> Dict[str, Any]:
        """Удалить источник данных"""
        try:
            cursor = self.conn.cursor()
            
            # Получить имя источника перед удалением
            cursor.execute('SELECT datasource_name FROM data_sources WHERE id = ?', (source_id,))
            row = cursor.fetchone()
            
            if not row:
                return {'success': False, 'error': f'Источник данных с ID {source_id} не найден'}
            
            source_name = row[0]
            
            # Удалить источник
            cursor.execute('DELETE FROM data_sources WHERE id = ?', (source_id,))
            
            self.conn.commit()
            
            return {
                'success': True,
                'message': f'Источник данных "{source_name}" удален'
            }
            
        except Exception as e:
            self.conn.rollback()
            return {'success': False, 'error': str(e)}
    
    # ===== НАБОРЫ ПРИМЕРНЫХ ДАННЫХ =====
    
    def get_sample_datasets(self) -> Dict[str, Dict[str, Any]]:
        """Получить наборы примерных данных"""
        return {
            'smart_home': {
                'name': 'Умный дом',
                'description': 'Базовая сеть умного дома с датчиками и исполнительными устройствами',
                'devices': [
                    {'original_id': 101, 'name': 'Датчик температуры', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 102, 'name': 'Датчик влажности', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 103, 'name': 'Датчик движения', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 104, 'name': 'Умная лампа', 'type': 'actuator', 'status': 'active'},
                    {'original_id': 105, 'name': 'Кондиционер', 'type': 'actuator', 'status': 'active'},
                    {'original_id': 106, 'name': 'Шлюз Zigbee', 'type': 'gateway', 'status': 'active'},
                    {'original_id': 107, 'name': 'Розетка умная', 'type': 'actuator', 'status': 'inactive'},
                ],
                'connections': [
                    (101, 106), (102, 106), (103, 106), (106, 104), 
                    (106, 105), (101, 105), (104, 105)
                ],
                'data_sources': [
                    {'name': 'Home Assistant API', 'type': 'api', 'last_update': (datetime.now() - timedelta(hours=1)).isoformat()},
                    {'name': 'Zigbee Gateway', 'type': 'stream', 'last_update': datetime.now().isoformat()}
                ]
            },
            'industrial_iot': {
                'name': 'Промышленный IoT',
                'description': 'Сеть промышленных датчиков и контроллеров',
                'devices': [
                    {'original_id': 201, 'name': 'Датчик вибрации', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 202, 'name': 'Датчик давления', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 203, 'name': 'Датчик температуры', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 204, 'name': 'Контроллер PLC', 'type': 'controller', 'status': 'active'},
                    {'original_id': 205, 'name': 'Шлюз OPC UA', 'type': 'gateway', 'status': 'active'},
                    {'original_id': 206, 'name': 'Аварийный стоп', 'type': 'actuator', 'status': 'active'},
                    {'original_id': 207, 'name': 'Клапан регулирующий', 'type': 'actuator', 'status': 'active'},
                    {'original_id': 208, 'name': 'Резервный датчик', 'type': 'sensor', 'status': 'inactive'},
                ],
                'connections': [
                    (201, 205), (202, 205), (203, 205), (205, 204),
                    (204, 206), (204, 207), (201, 204), (202, 204),
                    (203, 204), (205, 206), (205, 207)
                ],
                'data_sources': [
                    {'name': 'OPC UA Server', 'type': 'stream', 'last_update': datetime.now().isoformat()},
                    {'name': 'SCADA Database', 'type': 'database', 'last_update': (datetime.now() - timedelta(hours=3)).isoformat()},
                    {'name': 'MES System API', 'type': 'api', 'last_update': (datetime.now() - timedelta(hours=2)).isoformat()}
                ]
            },
            'smart_city': {
                'name': 'Умный город',
                'description': 'Сеть устройств для управления городской инфраструктурой',
                'devices': [
                    {'original_id': 301, 'name': 'Умный фонарь', 'type': 'actuator', 'status': 'active'},
                    {'original_id': 302, 'name': 'Датчик освещенности', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 303, 'name': 'Датчик шума', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 304, 'name': 'Камера наблюдения', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 305, 'name': 'Контроллер района', 'type': 'controller', 'status': 'active'},
                    {'original_id': 306, 'name': 'Шлюз LoRaWAN', 'type': 'gateway', 'status': 'active'},
                    {'original_id': 307, 'name': 'Метеостанция', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 308, 'name': 'Табло информации', 'type': 'actuator', 'status': 'active'},
                ],
                'connections': [
                    (301, 306), (302, 306), (303, 306), (304, 306),
                    (307, 306), (306, 305), (305, 301), (305, 308),
                    (302, 305), (307, 305), (301, 302), (305, 306)
                ],
                'data_sources': [
                    {'name': 'LoRaWAN Network Server', 'type': 'stream', 'last_update': datetime.now().isoformat()},
                    {'name': 'City Management API', 'type': 'api', 'last_update': (datetime.now() - timedelta(hours=1)).isoformat()},
                    {'name': 'Weather Service', 'type': 'api', 'last_update': (datetime.now() - timedelta(minutes=30)).isoformat()}
                ]
            },
            'simple_test': {
                'name': 'Простой тест',
                'description': 'Минимальная сеть для тестирования',
                'devices': [
                    {'original_id': 401, 'name': 'Датчик 1', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 402, 'name': 'Датчик 2', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 403, 'name': 'Контроллер', 'type': 'controller', 'status': 'active'},
                ],
                'connections': [
                    (401, 403), (402, 403)
                ],
                'data_sources': [
                    {'name': 'Test API', 'type': 'api', 'last_update': datetime.now().isoformat()}
                ]
            }
        }
    
    # ===== ПРЕЦЕДЕНТ 1: ЗАГРУЗКА ДАННЫХ ОБ УСТРОЙСТВАХ И СВЯЗЯХ =====
    
    def load_iot_data(
        self,
        network_id: int,
        dataset_name: str = None
    ) -> Dict[str, Any]:
        """
        Реализация прецедента "Загрузить данные об IoT-устройствах и -связях"
        """
        try:
            # Проверить существование сети
            cursor = self.conn.cursor()
            cursor.execute('SELECT id FROM iot_networks WHERE id = ?', (network_id,))
            if not cursor.fetchone():
                return {'success': False, 'error': f"Сеть с ID {network_id} не найдена"}
            
            # Выбрать источник данных
            if not dataset_name:
                return {'success': False, 'error': "Не указан набор данных"}
            
            datasets = self.get_sample_datasets()
            if dataset_name not in datasets:
                return {'success': False, 'error': f"Набор данных '{dataset_name}' не найден"}
            
            dataset = datasets[dataset_name]
            devices_data = dataset['devices']
            connections_data = dataset['connections']
            data_sources_data = dataset['data_sources']
            
            # Начать транзакцию
            cursor = self.conn.cursor()
            
            # Удалить существующие данные (если есть)
            cursor.execute('DELETE FROM device_connections WHERE device_id IN (SELECT id FROM devices WHERE network_id = ?)', (network_id,))
            cursor.execute('DELETE FROM devices WHERE network_id = ?', (network_id,))
            cursor.execute('DELETE FROM data_sources WHERE network_id = ?', (network_id,))
            
            # Загрузить устройства
            device_id_map = {}
            
            for device_data in devices_data:
                cursor.execute('''
                    INSERT INTO devices (device_name, status, type, network_id)
                    VALUES (?, ?, ?, ?)
                ''', (
                    device_data['name'],
                    device_data.get('status', 'active'),
                    device_data['type'],
                    network_id
                ))
                
                device_id = cursor.lastrowid
                device_id_map[device_data['original_id']] = device_id
            
            # Загрузить связи между устройствами
            connection_count = 0
            
            for source_orig_id, target_orig_id in connections_data:
                if source_orig_id in device_id_map and target_orig_id in device_id_map:
                    source_id = device_id_map[source_orig_id]
                    target_id = device_id_map[target_orig_id]
                    
                    try:
                        cursor.execute('''
                            INSERT INTO device_connections (device_id, connected_device_id)
                            VALUES (?, ?)
                        ''', (source_id, target_id))
                        connection_count += 1
                    except sqlite3.IntegrityError:
                        # Игнорируем дублирующиеся связи
                        pass
            
            # Загрузить источники данных
            for ds_data in data_sources_data:
                cursor.execute('''
                    INSERT INTO data_sources (datasource_name, last_update, type, network_id)
                    VALUES (?, ?, ?, ?)
                ''', (
                    ds_data['name'],
                    ds_data['last_update'],
                    ds_data['type'],
                    network_id
                ))
            
            self.conn.commit()
            
            return {
                'success': True,
                'message': f"Успешно загружено из '{dataset['name']}': {len(devices_data)} устройств, "
                          f"{connection_count} связей, {len(data_sources_data)} источников данных",
                'devices_loaded': len(devices_data),
                'connections_loaded': connection_count,
                'data_sources_loaded': len(data_sources_data),
                'dataset_name': dataset['name']
            }
            
        except Exception as e:
            self.conn.rollback()
            return {'success': False, 'error': str(e)}
    
    # ===== ПРЕЦЕДЕНТ 2: АНАЛИЗ ТОПОЛОГИИ И СВЯЗЕЙ =====
    
    def analyze_topology_and_connections(self, network_id: int) -> Dict[str, Any]:
        """
        Реализация прецедента "Проанализировать топологию и связи"
        """
        try:
            # 1. Получить данные сети
            network_finder = IoTNetworkFinder(self.conn)
            network_gateway = network_finder.find_by_id(network_id)
            
            if not network_gateway:
                return {'success': False, 'error': f"Сеть с ID {network_id} не найдена"}
            
            # 2. Получить доменную модель сети
            network = network_gateway.to_domain_model()
            if not network:
                return {'success': False, 'error': "Ошибка загрузки сети"}
            
            # 3. Получить устройства сети
            device_finder = DeviceFinder(self.conn)
            device_gateways = device_finder.find_by_network(network_id)
            
            if not device_gateways:
                return {'success': False, 'error': "В сети нет устройств для анализа"}
            
            # 4. Преобразовать RDG в доменные модели
            devices = []
            for device_gateway in device_gateways:
                device = device_gateway.to_domain_model()
                if device:
                    devices.append(device)
            
            # 5. Валидация сети
            if not network.validate_connections(devices):
                return {'success': False, 'error': "Сеть содержит невалидные связи"}
            
            # 6. Выполнить анализ топологии
            analysis_result = network.analyze_topology(devices)
            
            # 7. Сохранить результат анализа
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO analysis (centrality_score, date, network_id)
                VALUES (?, ?, ?)
            ''', (
                analysis_result.centrality_score,
                analysis_result.date.isoformat(),
                network_id
            ))
            
            analysis_id = cursor.lastrowid
            
            # 8. Сохранить детали анализа
            for node_id in analysis_result.isolated_nodes:
                cursor.execute('''
                    INSERT INTO isolated_nodes (analysis_id, device_id)
                    VALUES (?, ?)
                ''', (analysis_id, node_id))
            
            for link in analysis_result.redundant_links:
                cursor.execute('''
                    INSERT INTO redundant_links (analysis_id, device_id1, device_id2)
                    VALUES (?, ?, ?)
                ''', (analysis_id, link[0], link[1]))
            
            self.conn.commit()
            
            # 9. Подготовить детализированную информацию
            isolated_devices_info = []
            for node_id in analysis_result.isolated_nodes:
                device_finder = DeviceFinder(self.conn)
                device_gateway = device_finder.find_by_id(node_id)
                if device_gateway:
                    device = device_gateway.to_domain_model()
                    if device:
                        isolated_devices_info.append({
                            'id': device.id,
                            'name': device.device_name,
                            'type': device.type.value,
                            'status': device.status.value,
                            'is_active': device.is_active()
                        })
            
            redundant_links_info = []
            for link in analysis_result.redundant_links:
                device1_gateway = DeviceGateway(self.conn)
                device2_gateway = DeviceGateway(self.conn)
                
                device1_name = "Неизвестно"
                device2_name = "Неизвестно"
                
                if device1_gateway.load(link[0]):
                    device1 = device1_gateway.to_domain_model()
                    if device1:
                        device1_name = device1.device_name
                
                if device2_gateway.load(link[1]):
                    device2 = device2_gateway.to_domain_model()
                    if device2:
                        device2_name = device2.device_name
                
                redundant_links_info.append({
                    'device1_id': link[0],
                    'device2_id': link[1],
                    'device1_name': device1_name,
                    'device2_name': device2_name
                })
            
            # Получить источники данных
            data_source_finder = DataSourceFinder(self.conn)
            data_source_gateways = data_source_finder.find_by_network(network_id)
            data_sources = [ds.to_domain_model() for ds in data_source_gateways if ds.to_domain_model()]
            
            # Получить сводку по сети
            network_summary = network.get_network_summary(devices, data_sources)
            
            return {
                'success': True,
                'analysis_id': analysis_id,
                'network_name': network.network_name,
                'network_summary': network_summary,
                'analysis_date': analysis_result.date.strftime('%Y-%m-%d %H:%M:%S'),
                'centrality_score': round(analysis_result.centrality_score, 4),
                'isolated_nodes_count': len(analysis_result.isolated_nodes),
                'isolated_nodes': isolated_devices_info,
                'redundant_links_count': len(analysis_result.redundant_links),
                'redundant_links': redundant_links_info,
                'has_issues': analysis_result.has_issues(),
                'total_issues': analysis_result.get_issue_count(),
                'recommendations': analysis_result.get_recommendations()
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    # ===== ДОПОЛНИТЕЛЬНЫЕ МЕТОДЫ =====
    
    def authenticate_user(self, login: str, password: str) -> Optional[User]:
        """Аутентификация пользователя"""
        user_finder = UserFinder(self.conn)
        user_gateway = user_finder.find_by_login(login)
        
        if user_gateway and user_gateway.password == password:
            return user_gateway.to_domain_model()
        return None
    
    def create_network(self, name: str, description: str = "", user_id: Optional[int] = None) -> Dict[str, Any]:
        """Создать новую IoT сеть"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute('''
                INSERT INTO iot_networks (network_name, description, user_id)
                VALUES (?, ?, ?)
            ''', (name, description, user_id))
            
            network_id = cursor.lastrowid
            
            self.conn.commit()
            
            return {
                'success': True,
                'network_id': network_id,
                'network_name': name
            }
            
        except Exception as e:
            self.conn.rollback()
            return {'success': False, 'error': str(e)}
    
    def get_user_networks(self, user_id: int) -> List[Dict[str, Any]]:
        """Получить все сети пользователя"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, network_name, description, created_at 
            FROM iot_networks 
            WHERE user_id = ? 
            ORDER BY created_at DESC
        ''', (user_id,))
        
        networks = []
        for row in cursor.fetchall():
            networks.append({
                'id': row['id'],
                'name': row['network_name'],
                'description': row['description'],
                'created_at': row['created_at']
            })
        
        return networks
    
    def get_network_details(self, network_id: int) -> Dict[str, Any]:
        """Получить детальную информацию о сети"""
        cursor = self.conn.cursor()
        
        # Получить сеть
        cursor.execute('''
            SELECT id, network_name, description, created_at 
            FROM iot_networks 
            WHERE id = ?
        ''', (network_id,))
        
        network_row = cursor.fetchone()
        if not network_row:
            return {}
        
        network = {
            'id': network_row['id'],
            'name': network_row['network_name'],
            'description': network_row['description'],
            'created_at': network_row['created_at']
        }
        
        # Получить устройства
        cursor.execute('''
            SELECT d.id, d.device_name, d.type, d.status, 
                   COUNT(dc.connected_device_id) as connections_count
            FROM devices d
            LEFT JOIN device_connections dc ON d.id = dc.device_id
            WHERE d.network_id = ?
            GROUP BY d.id
            ORDER BY d.type, d.device_name
        ''', (network_id,))
        
        devices = []
        for row in cursor.fetchall():
            devices.append({
                'id': row['id'],
                'name': row['device_name'],
                'type': row['type'],
                'status': row['status'],
                'connections_count': row['connections_count'] or 0
            })
        
        # Получить источники данных
        cursor.execute('''
            SELECT id, datasource_name, type, last_update 
            FROM data_sources 
            WHERE network_id = ?
            ORDER BY datasource_name
        ''', (network_id,))
        
        data_sources = []
        for row in cursor.fetchall():
            data_sources.append({
                'id': row['id'],
                'name': row['datasource_name'],
                'type': row['type'],
                'last_update': row['last_update']
            })
        
        # Получить историю анализов
        cursor.execute('''
            SELECT id, centrality_score, date 
            FROM analysis 
            WHERE network_id = ? 
            ORDER BY date DESC 
            LIMIT 5
        ''', (network_id,))
        
        analyses = []
        for row in cursor.fetchall():
            analyses.append({
                'id': row['id'],
                'date': row['date'],
                'centrality_score': row['centrality_score'],
                'has_issues': False  # Это нужно будет вычислить
            })
        
        # Статистика связей
        cursor.execute('''
            SELECT COUNT(*) as total_connections 
            FROM device_connections 
            WHERE device_id IN (SELECT id FROM devices WHERE network_id = ?)
        ''', (network_id,))
        
        connections_stat = cursor.fetchone()
        
        active_devices = len([d for d in devices if d['status'] == 'active'])
        
        return {
            'network': network,
            'devices': devices,
            'data_sources': data_sources,
            'analyses': analyses,
            'stats': {
                'total_devices': len(devices),
                'active_devices': active_devices,
                'total_data_sources': len(data_sources),
                'total_analyses': len(analyses),
                'total_connections': connections_stat['total_connections'] if connections_stat else 0
            }
        }
    
    def close(self):
        """Закрыть соединение с БД"""
        self.conn.close()