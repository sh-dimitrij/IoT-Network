# app.py
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from datetime import datetime, timedelta
from typing import List, Tuple
import json
from application import IoTNetworkService

app = Flask(__name__)
app.secret_key = 'iot-network-analysis-secret-key'
app.config['SESSION_TYPE'] = 'filesystem'

# Инициализация сервиса
iot_service = IoTNetworkService()

@app.route('/')
def index():
    """Главная страница с авторизацией"""
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return render_template('index.html')

@app.route('/login', methods=['POST'])
def login():
    """Авторизация пользователя"""
    login = request.form.get('login')
    password = request.form.get('password')
    
    user = iot_service.authenticate_user(login, password)
    
    if user:
        session['user_id'] = user['id']
        session['user_name'] = user['name']
        session['user_role'] = user['role']
        flash(f'Добро пожаловать, {user["name"]}!', 'success')
        return redirect(url_for('dashboard'))
    else:
        flash('Неверный логин или пароль', 'error')
        return redirect(url_for('index'))

@app.route('/logout')
def logout():
    """Выход из системы"""
    session.clear()
    flash('Вы успешно вышли из системы', 'info')
    return redirect(url_for('index'))

@app.route('/dashboard')
def dashboard():
    """Панель управления"""
    if 'user_id' not in session:
        return redirect(url_for('index'))
    
    # Получить сети пользователя
    networks = iot_service.get_all_networks(session['user_id'])
    
    return render_template('network_info.html', 
                         networks=networks,
                         user_role=session.get('user_role'))

@app.route('/create_network', methods=['POST'])
def create_network():
    """Создать новую IoT сеть"""
    if 'user_id' not in session:
        return redirect(url_for('index'))
    
    name = request.form.get('network_name')
    description = request.form.get('description', '')
    
    if name:
        iot_service.create_network(name, description, session['user_id'])
        flash(f'Сеть "{name}" успешно создана', 'success')
    
    return redirect(url_for('dashboard'))

@app.route('/network/<int:network_id>')
def network_details(network_id):
    """Детальная информация о сети"""
    if 'user_id' not in session:
        return redirect(url_for('index'))
    
    network_info = iot_service.get_network_details(network_id)
    
    if not network_info:
        flash('Сеть не найдена', 'error')
        return redirect(url_for('dashboard'))
    
    return render_template('network_info.html',
                         network_info=network_info,
                         user_role=session.get('user_role'))

@app.route('/load_data/<int:network_id>', methods=['GET', 'POST'])
def load_data(network_id):
    """Страница загрузки данных IoT (прецедент 1)"""
    if 'user_id' not in session:
        return redirect(url_for('index'))
    
    if request.method == 'POST':
        # Получение данных из формы
        devices_data = []
        connections_data = []
        data_sources_data = []
        
        try:
            # Пример данных устройств (в реальном приложении будет загрузка файла)
            sample_devices = [
                {'original_id': 101, 'name': 'Температурный датчик', 'type': 'sensor', 'status': 'active'},
                {'original_id': 102, 'name': 'Датчик влажности', 'type': 'sensor', 'status': 'active'},
                {'original_id': 103, 'name': 'Умный светильник', 'type': 'actuator', 'status': 'active'},
                {'original_id': 104, 'name': 'Кондиционер', 'type': 'actuator', 'status': 'active'},
                {'original_id': 105, 'name': 'Шлюз Zigbee', 'type': 'gateway', 'status': 'active'},
                {'original_id': 106, 'name': 'Датчик движения', 'type': 'sensor', 'status': 'inactive'}
            ]
            
            # Пример связей
            sample_connections: List[Tuple[int, int]] = [
                (101, 105), (102, 105), (105, 103), (105, 104),
                (101, 104), (101, 104), (103, 104), (106, 105)
            ]
            
            # Пример источников данных
            sample_data_sources = [
                {
                    'name': 'Home Assistant API',
                    'type': 'api',
                    'last_update': (datetime.now() - timedelta(hours=2)).isoformat()
                },
                {
                    'name': 'MQTT Broker',
                    'type': 'stream',
                    'last_update': datetime.now().isoformat()
                }
            ]
            
            # Вызов прецедента загрузки данных
            result = iot_service.load_iot_data(
                network_id=network_id,
                devices_data=sample_devices,
                connections_data=sample_connections,
                data_sources_data=sample_data_sources
            )
            
            if result['success']:
                flash(result['message'], 'success')
            else:
                flash(f'Ошибка загрузки: {result["error"]}', 'error')
            
            return redirect(url_for('network_details', network_id=network_id))
            
        except Exception as e:
            flash(f'Ошибка: {str(e)}', 'error')
    
    # Получить информацию о сети для отображения
    network_info = iot_service.get_network_details(network_id)
    
    return render_template('load_data.html',
                         network_info=network_info,
                         user_role=session.get('user_role'))

@app.route('/analyze/<int:network_id>', methods=['GET', 'POST'])
def analyze_network(network_id):
    """Страница анализа топологии (прецедент 2)"""
    if 'user_id' not in session:
        return redirect(url_for('index'))
    
    network_info = iot_service.get_network_details(network_id)
    
    if request.method == 'POST':
        # Вызов прецедента анализа
        result = iot_service.analyze_topology_and_connections(network_id)
        
        if result['success']:
            return render_template('analyze.html',
                                 network_info=network_info,
                                 analysis_result=result,
                                 user_role=session.get('user_role'))
        else:
            flash(f'Ошибка анализа: {result["error"]}', 'error')
            return redirect(url_for('network_details', network_id=network_id))
    
    return render_template('analyze.html',
                         network_info=network_info,
                         analysis_result=None,
                         user_role=session.get('user_role'))

@app.route('/api/get_sample_data')
def get_sample_data():
    """API для получения примеров данных"""
    sample_data = {
        'devices': [
            {'id': 1, 'name': 'Датчик температуры', 'type': 'sensor', 'status': 'active'},
            {'id': 2, 'name': 'Датчик влажности', 'type': 'sensor', 'status': 'active'},
            {'id': 3, 'name': 'Умная лампа', 'type': 'actuator', 'status': 'active'},
            {'id': 4, 'name': 'Кондиционер', 'type': 'actuator', 'status': 'active'},
            {'id': 5, 'name': 'Шлюз', 'type': 'gateway', 'status': 'active'}
        ],
        'connections': [
            {'from': 1, 'to': 5},
            {'from': 2, 'to': 5},
            {'from': 5, 'to': 3},
            {'from': 5, 'to': 4},
            {'from': 1, 'to': 4}
        ]
    }
    return jsonify(sample_data)

if __name__ == '__main__':
    app.run(debug=True, port=5000)