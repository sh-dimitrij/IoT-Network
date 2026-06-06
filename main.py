# main.py
from datetime import datetime, timedelta
from app import IoTNetworkService


def main():
    service = IoTNetworkService()
    
    try:
        print("=== Создание IoT сети ===")
        network = service.create_network(
            name="Умный дом",
            description="Сеть устройств умного дома"
        )
        print(f"Создана сеть: {network.network_name} (ID: {network.id})")

        print("\n=== Загрузка данных IoT ===")

        devices_data = [
            {'original_id': 101, 'name': 'Датчик температуры', 'type': 'sensor', 'status': 'active'},
            {'original_id': 102, 'name': 'Датчик влажности', 'type': 'sensor', 'status': 'active'},
            {'original_id': 103, 'name': 'Умная лампа', 'type': 'actuator', 'status': 'active'},
            {'original_id': 104, 'name': 'Кондиционер', 'type': 'actuator', 'status': 'active'},
            {'original_id': 105, 'name': 'Шлюз Zigbee', 'type': 'gateway', 'status': 'active'},
            {'original_id': 106, 'name': 'Резервный датчик', 'type': 'sensor', 'status': 'inactive'}
        ]

        connections_data = [
            (101, 105),  # Датчик температуры -> Шлюз
            (102, 105),  # Датчик влажности -> Шлюз
            (105, 103),  # Шлюз -> Умная лампа
            (105, 104),  # Шлюз -> Кондиционер
            (101, 104),  # Датчик температуры -> Кондиционер (прямая связь)
            (101, 104),  # Дублирующаяся связь
            (103, 104)   # Умная лампа -> Кондиционер
        ]

        data_sources_data = [
            {
                'name': 'Home Assistant API',
                'type': 'api',
                'last_update': (datetime.now() - timedelta(hours=2)).isoformat()
            },
            {
                'name': 'Local MQTT Broker',
                'type': 'stream',
                'last_update': datetime.now().isoformat()
            }
        ]

        loaded_network = service.load_iot_data(
            network_id=network.id,
            devices_data=devices_data,
            connections_data=connections_data,
            data_sources_data=data_sources_data
        )
        
        print(f"Загружено устройств: {len(loaded_network.get_all_devices())}")
        print(f"Загружено источников данных: {len(data_sources_data)}")

        print("\n=== Анализ топологии и связей ===")

        analysis_result = service.analyze_topology_and_connections(network.id)

        print(f"Сеть: {analysis_result['network_name']}")
        print(f"Всего устройств: {analysis_result['total_devices']}")
        print(f"Дата анализа: {analysis_result['analysis_date']}")
        print(f"Средняя центральность: {analysis_result['centrality_score']}")
        print(f"Изолированных узлов: {analysis_result['isolated_nodes_count']}")
        if analysis_result['isolated_nodes']:
            print(f"  ID узлов: {analysis_result['isolated_nodes']}")
        
        print(f"Избыточных связей: {analysis_result['redundant_links_count']}")
        if analysis_result['redundant_links']:
            print(f"  Связи: {analysis_result['redundant_links']}")
        
        print(f"Есть проблемы: {'Да' if analysis_result['has_issues'] else 'Нет'}")
        print(f"Всего проблем: {analysis_result['total_issues']}")

        print("\n=== Информация о сети ===")
        network_info = service.get_network_info(network.id)
        print(f"Имя сети: {network_info['network']['name']}")
        print(f"Устройств в сети: {network_info['devices_count']}")
        print(f"Источников данных: {network_info['data_sources_count']}")
        
        if network_info['last_analysis']:
            print(f"Последний анализ: {network_info['last_analysis']['date']}")
            print(f"Центральность: {network_info['last_analysis']['centrality_score']}")
        
    finally:
        service.close()


if __name__ == "__main__":
    main()