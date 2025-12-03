# app.py
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from datetime import datetime, timedelta
from typing import List, Tuple
from application_service import IoTNetworkApplicationService

app = Flask(__name__)
app.secret_key = 'iot-network-analysis-secret-key'
app.config['SESSION_TYPE'] = 'filesystem'

iot_service = IoTNetworkApplicationService()

@app.template_filter('to_datetime')
def to_datetime_filter(value):
    """Конвертировать строку в datetime"""
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except:
            try:
                return datetime.strptime(value[:19], '%Y-%m-%d %H:%M:%S')
            except:
                return datetime.now()
    return value

@app.context_processor
def utility_processor():
    """Добавить утилиты в контекст шаблонов"""
    def now():
        return datetime.now()
    return dict(now=now)

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
        session['user_id'] = user.id
        session['user_name'] = user.name
        session['user_role'] = user.role.value
        flash(f'Добро пожаловать, {user.name}!', 'success')
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

    networks = iot_service.get_user_networks(session['user_id'])
    
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
        result = iot_service.create_network(name, description, session['user_id'])
        
        if result['success']:
            flash(f'Сеть "{name}" успешно создана', 'success')
        else:
            flash(f'Ошибка: {result["error"]}', 'error')
    
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
    
    network_info = iot_service.get_network_details(network_id)

    if not network_info or 'network' not in network_info or not network_info['network'] or 'id' not in network_info['network']:
        flash('Сеть не найдена', 'error')
        return redirect(url_for('dashboard'))
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'load_sample':
            dataset_name = request.form.get('dataset')
            
            result = iot_service.load_iot_data(
                network_id=network_id,
                dataset_name=dataset_name
            )
            
            if result['success']:
                flash(result['message'], 'success')
            else:
                flash(f'Ошибка загрузки: {result["error"]}', 'error')
            
            return redirect(url_for('network_details', network_id=network_id))
        
        elif action == 'add_device':
            device_name = request.form.get('device_name')
            device_type = request.form.get('device_type')
            device_status = request.form.get('device_status', 'active')
            
            if device_name and device_type:
                device_data = {
                    'name': device_name,
                    'type': device_type,
                    'status': device_status,
                    'connections': []
                }
                
                result = iot_service.add_device(network_id, device_data)
                
                if result['success']:
                    flash(result['message'], 'success')
                else:
                    flash(f'Ошибка: {result["error"]}', 'error')
            
            return redirect(url_for('load_data', network_id=network_id))
        
        elif action == 'add_source':
            source_name = request.form.get('source_name')
            source_type = request.form.get('source_type')
            
            if source_name and source_type:
                source_data = {
                    'name': source_name,
                    'type': source_type,
                    'last_update': datetime.now().isoformat()
                }
                
                result = iot_service.add_data_source(network_id, source_data)
                
                if result['success']:
                    flash(result['message'], 'success')
                else:
                    flash(f'Ошибка: {result["error"]}', 'error')
            
            return redirect(url_for('load_data', network_id=network_id))

    datasets = iot_service.get_sample_datasets()

    if datasets is None:
        datasets = {}
    
    return render_template('load_data.html',
                         network_info=network_info,
                         datasets=datasets,
                         user_role=session.get('user_role'))

@app.route('/manage_device/<int:device_id>', methods=['POST'])
def manage_device(device_id):
    """Управление устройством (удаление)"""
    if 'user_id' not in session:
        return redirect(url_for('index'))
    
    action = request.form.get('action')
    
    if action == 'delete':
        result = iot_service.remove_device(device_id)
        
        if result['success']:
            flash(result['message'], 'success')
        else:
            flash(f'Ошибка: {result["error"]}', 'error')

    network_info = iot_service.get_network_details(
        request.form.get('network_id', type=int)
    )
    
    if network_info:
        return redirect(url_for('network_details', network_id=network_info['network']['id']))
    
    return redirect(url_for('dashboard'))

@app.route('/manage_source/<int:source_id>', methods=['POST'])
def manage_source(source_id):
    """Управление источником данных"""
    if 'user_id' not in session:
        return redirect(url_for('index'))
    
    action = request.form.get('action')
    
    if action == 'delete':
        result = iot_service.remove_data_source(source_id)
        
        if result['success']:
            flash(result['message'], 'success')
        else:
            flash(f'Ошибка: {result["error"]}', 'error')
    
    elif action == 'update':
        result = iot_service.update_data_source(source_id)
        
        if result['success']:
            flash(result['message'], 'success')
        else:
            flash(f'Ошибка: {result["error"]}', 'error')

    network_info = iot_service.get_network_details(
        request.form.get('network_id', type=int)
    )
    
    if network_info:
        return redirect(url_for('network_details', network_id=network_info['network']['id']))
    
    return redirect(url_for('dashboard'))

@app.route('/analyze/<int:network_id>', methods=['GET', 'POST'])
def analyze_network(network_id):
    """Страница анализа топологии (прецедент 2)"""
    if 'user_id' not in session:
        return redirect(url_for('index'))
    
    network_info = iot_service.get_network_details(network_id)
    
    if request.method == 'POST':

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

@app.route('/delete_network/<int:network_id>', methods=['POST'])
def delete_network(network_id):
    """Удалить сеть"""
    if 'user_id' not in session:
        return redirect(url_for('index'))

    networks = iot_service.get_user_networks(session['user_id'])
    network_exists = any(network['id'] == network_id for network in networks)
    
    if not network_exists:
        flash('Сеть не найдена или у вас нет прав для ее удаления', 'error')
        return redirect(url_for('dashboard'))
    
    result = iot_service.delete_network(network_id)
    
    if result['success']:
        flash(result['message'], 'success')
    else:
        flash(f'Ошибка: {result["error"]}', 'error')
    
    return redirect(url_for('dashboard'))

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')