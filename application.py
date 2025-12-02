# application.py
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from domain_models import (
    Device, DeviceStatus, DeviceType,
    DataSource, DataSourceType,
    IoTNetwork, AnalysisResult
)
from gateways import (
    DeviceGateway, DataSourceGateway,
    AnalysisGateway, IoTNetworkGateway
)


class IoTNetworkService:
    """Сервисный слой для работы с IoT сетями"""
    
    def __init__(self, db_path: str = 'iot_network.db'):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Для работы со строками как словарями
        self._init_database()
        
        # Инициализация gateways
        self.device_gateway = DeviceGateway(self.conn)
        self.data_source_gateway = DataSourceGateway(self.conn)
        self.analysis_gateway = AnalysisGateway(self.conn)
        self.network_gateway = IoTNetworkGateway(self.conn)
    
    def _init_database(self):
        """Инициализировать структуру БД"""
        cursor = self.conn.cursor()
        
        # Таблица пользователей (для авторизации)
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
                FOREIGN KEY (network_id) REFERENCES iot_networks (id)
            )
        ''')
        
        # Таблица связей между устройствами
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS device_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id INTEGER NOT NULL,
                connected_device_id INTEGER NOT NULL,
                FOREIGN KEY (device_id) REFERENCES devices (id)
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
                FOREIGN KEY (network_id) REFERENCES iot_networks (id)
            )
        ''')
        
        # Таблица результатов анализа
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                centrality_score REAL NOT NULL,
                date TEXT NOT NULL,
                network_id INTEGER NOT NULL,
                FOREIGN KEY (network_id) REFERENCES iot_networks (id)
            )
        ''')
        
        # Таблица изолированных узлов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS isolated_nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id INTEGER NOT NULL,
                device_id INTEGER NOT NULL,
                FOREIGN KEY (analysis_id) REFERENCES analysis (id)
            )
        ''')
        
        # Таблица избыточных связей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS redundant_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id INTEGER NOT NULL,
                device_id1 INTEGER NOT NULL,
                device_id2 INTEGER NOT NULL,
                FOREIGN KEY (analysis_id) REFERENCES analysis (id)
            )
        ''')
        
        # Создаем тестового пользователя, если нет
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
    
    def authenticate_user(self, login: str, password: str) -> Optional[Dict]:
        """Аутентификация пользователя"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, name, login, role 
            FROM users 
            WHERE login = ? AND password = ?
        ''', (login, password))
        
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    
    def get_all_networks(self, user_id: Optional[int] = None) -> List[Dict]:
        """Получить все сети пользователя"""
        cursor = self.conn.cursor()
        
        if user_id:
            cursor.execute('''
                SELECT id, network_name, description, created_at 
                FROM iot_networks 
                WHERE user_id = ? 
                ORDER BY created_at DESC
            ''', (user_id,))
        else:
            cursor.execute('''
                SELECT id, network_name, description, created_at 
                FROM iot_networks 
                ORDER BY created_at DESC
            ''')
        
        networks = []
        for row in cursor.fetchall():
            networks.append(dict(row))
        
        return networks
    
    def create_network(self, name: str, description: str = "", user_id: Optional[int] = None) -> Dict:
        """Создать новую IoT сеть"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            INSERT INTO iot_networks (network_name, description, user_id)
            VALUES (?, ?, ?)
        ''', (name, description, user_id))
        
        network_id = cursor.lastrowid
        
        self.conn.commit()
        
        return {
            'id': network_id,
            'network_name': name,
            'description': description,
            'user_id': user_id
        }
    
    def load_iot_data(
        self,
        network_id: int,
        devices_data: List[Dict[str, Any]],
        connections_data: List[Tuple[int, int]],
        data_sources_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Загрузить данные об IoT-устройствах и связях
        """
        try:
            # Получить сеть
            network = self.network_gateway.find_by_id(network_id)
            if not network:
                return {'success': False, 'error': f"Сеть с ID {network_id} не найдена"}
            
            # Загрузить устройства
            devices_dict = {}
            device_ids_map = {}  # Соответствие оригинальных ID и реальных ID
            
            for device_data in devices_data:
                device = Device(
                    id=0,
                    device_name=device_data['name'],
                    status=DeviceStatus(device_data.get('status', 'active')),
                    type=DeviceType(device_data['type']),
                    connections=[]
                )
                
                # Вставить в БД
                device_id = self.device_gateway.insert(device, network_id)
                devices_dict[device_id] = device
                device_ids_map[device_data['original_id']] = device_id
                
                # Добавить в доменную модель
                network.add_device(device)
            
            # Загрузить связи
            connection_count = 0
            for source_orig_id, target_orig_id in connections_data:
                if source_orig_id in device_ids_map and target_orig_id in device_ids_map:
                    source_id = device_ids_map[source_orig_id]
                    target_id = device_ids_map[target_orig_id]
                    
                    # Добавить связь в доменную модель
                    if source_device := network.get_device(source_id):
                        source_device.add_connection(target_id)
                    
                    # Добавить связь в БД
                    cursor = self.conn.cursor()
                    cursor.execute('''
                        INSERT INTO device_connections (device_id, connected_device_id)
                        VALUES (?, ?)
                    ''', (source_id, target_id))
                    
                    connection_count += 1
            
            # Загрузить источники данных
            for ds_data in data_sources_data:
                data_source = DataSource(
                    id=0,
                    datasource_name=ds_data['name'],
                    last_update=datetime.fromisoformat(ds_data['last_update']),
                    type=DataSourceType(ds_data['type'])
                )
                
                self.data_source_gateway.insert(data_source, network_id)
                network.add_data_source(data_source)
            
            self.conn.commit()
            
            return {
                'success': True,
                'network_id': network_id,
                'devices_loaded': len(devices_data),
                'connections_loaded': connection_count,
                'data_sources_loaded': len(data_sources_data),
                'message': f"Успешно загружено {len(devices_data)} устройств, {connection_count} связей и {len(data_sources_data)} источников данных"
            }
            
        except Exception as e:
            self.conn.rollback()
            return {'success': False, 'error': str(e)}
    
    def analyze_topology_and_connections(self, network_id: int) -> Dict[str, Any]:
        """
        Проанализировать топологию и связи сети
        """
        try:
            # Получить сеть с данными из БД
            network = self.network_gateway.find_by_id(network_id)
            if not network:
                return {'success': False, 'error': f"Сеть с ID {network_id} не найдена"}
            
            # Загрузить устройства в доменную модель
            devices = self.device_gateway.find_by_network(network_id)
            if not devices:
                return {'success': False, 'error': "В сети нет устройств для анализа"}
            
            for device in devices:
                network.add_device(device)
            
            # Проверить валидность сети
            if not network.validate_network():
                return {'success': False, 'error': "Сеть содержит невалидные связи"}
            
            # Выполнить анализ (доменная логика)
            analysis_result = network.analyze_topology()
            
            # Сохранить результат анализа
            analysis_id = self.analysis_gateway.insert(analysis_result, network_id)
            
            # Получить детальную информацию об устройствах для изолированных узлов
            isolated_devices_info = []
            for node_id in analysis_result.isolated_nodes:
                cursor = self.conn.cursor()
                cursor.execute('''
                    SELECT device_name, type, status 
                    FROM devices 
                    WHERE id = ?
                ''', (node_id,))
                device_info = cursor.fetchone()
                if device_info:
                    isolated_devices_info.append({
                        'id': node_id,
                        'name': device_info['device_name'],
                        'type': device_info['type'],
                        'status': device_info['status']
                    })
            
            # Получить информацию об избыточных связях
            redundant_links_info = []
            for link in analysis_result.redundant_links:
                cursor = self.conn.cursor()
                cursor.execute('''
                    SELECT d1.device_name as device1_name, d2.device_name as device2_name
                    FROM devices d1, devices d2
                    WHERE d1.id = ? AND d2.id = ?
                ''', (link[0], link[1]))
                link_info = cursor.fetchone()
                if link_info:
                    redundant_links_info.append({
                        'device1_id': link[0],
                        'device2_id': link[1],
                        'device1_name': link_info['device1_name'],
                        'device2_name': link_info['device2_name']
                    })
            
            return {
                'success': True,
                'analysis_id': analysis_id,
                'network_name': network.network_name,
                'total_devices': len(devices),
                'analysis_date': analysis_result.date.strftime('%Y-%m-%d %H:%M:%S'),
                'centrality_score': round(analysis_result.centrality_score, 4),
                'isolated_nodes_count': len(analysis_result.isolated_nodes),
                'isolated_nodes': isolated_devices_info,
                'redundant_links_count': len(analysis_result.redundant_links),
                'redundant_links': redundant_links_info,
                'has_issues': analysis_result.has_issues(),
                'total_issues': analysis_result.get_issue_count(),
                'recommendations': self._generate_recommendations(analysis_result)
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _generate_recommendations(self, analysis: AnalysisResult) -> List[str]:
        """Сгенерировать рекомендации на основе анализа"""
        recommendations = []
        
        if analysis.isolated_nodes:
            recommendations.append(
                f"Обнаружено {len(analysis.isolated_nodes)} изолированных устройств. "
                "Рекомендуется проверить их подключение к сети или удалить, если они не используются."
            )
        
        if analysis.redundant_links:
            recommendations.append(
                f"Обнаружено {len(analysis.redundant_links)} избыточных связей. "
                "Рекомендуется удалить дублирующиеся связи для оптимизации сети."
            )
        
        if analysis.centrality_score < 0.3:
            recommendations.append(
                f"Низкая центральность сети ({analysis.centrality_score:.2f}). "
                "Рекомендуется добавить больше связей между ключевыми устройствами."
            )
        elif analysis.centrality_score > 0.7:
            recommendations.append(
                f"Высокая центральность сети ({analysis.centrality_score:.2f}). "
                "Сеть может быть перегружена - рассмотрите возможность распределения нагрузки."
            )
        
        if not recommendations:
            recommendations.append("Сеть в хорошем состоянии. Серьезных проблем не обнаружено.")
        
        return recommendations
    
    def get_network_details(self, network_id: int) -> Dict[str, Any]:
        """Получить детальную информацию о сети"""
        cursor = self.conn.cursor()
        
        # Основная информация о сети
        cursor.execute('''
            SELECT n.*, u.name as user_name 
            FROM iot_networks n 
            LEFT JOIN users u ON n.user_id = u.id 
            WHERE n.id = ?
        ''', (network_id,))
        
        network_row = cursor.fetchone()
        if not network_row:
            return {}
        
        network_info = dict(network_row)
        
        # Устройства сети
        cursor.execute('''
            SELECT id, device_name, type, status 
            FROM devices 
            WHERE network_id = ? 
            ORDER BY type, device_name
        ''', (network_id,))
        
        devices = [dict(row) for row in cursor.fetchall()]
        
        # Источники данных
        cursor.execute('''
            SELECT id, datasource_name, type, last_update 
            FROM data_sources 
            WHERE network_id = ?
        ''', (network_id,))
        
        data_sources = [dict(row) for row in cursor.fetchall()]
        
        # История анализов
        cursor.execute('''
            SELECT id, centrality_score, date 
            FROM analysis 
            WHERE network_id = ? 
            ORDER BY date DESC 
            LIMIT 5
        ''', (network_id,))
        
        analyses = [dict(row) for row in cursor.fetchall()]
        
        # Статистика связей
        cursor.execute('''
            SELECT COUNT(*) as total_connections 
            FROM device_connections 
            WHERE device_id IN (SELECT id FROM devices WHERE network_id = ?)
        ''', (network_id,))
        
        connections_stat = cursor.fetchone()
        
        return {
            'network': network_info,
            'devices': devices,
            'data_sources': data_sources,
            'analyses': analyses,
            'stats': {
                'total_devices': len(devices),
                'total_data_sources': len(data_sources),
                'total_analyses': len(analyses),
                'total_connections': connections_stat['total_connections'] if connections_stat else 0
            }
        }
    
    def close(self):
        """Закрыть соединение с БД"""
        self.conn.close()