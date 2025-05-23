# Telegram Bot для управления заказами и экстренными вызовами

Этот Telegram бот предназначен для администраторов сервисного центра, позволяя управлять заказами услуг и экстренными вызовами через удобный интерфейс.

## Основные функции

- 📌 **Автоматические уведомления** о новых заказах и экстренных вызовах
- 📋 **Просмотр заказов** в разных статусах (новые, в работе, завершенные)
- 🔄 **Управление статусами** заказов (взятие в работу, завершение)
- 📊 **Статистика** по всем типам заказов
- 📅 **Фильтрация завершенных заказов** по датам
- ⏰ **Автоматическая проверка** новых заказов каждые 5 минут

## Технологии

- Python 3.8+
- aiogram (асинхронный фреймворк для Telegram ботов)
- MySQL (хранение данных о заказах)
- APScheduler (планировщик для периодических задач)
- pytz (работа с часовыми поясами)

## Установка и настройка

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/yourusername/telegram-orders-bot.git
   cd telegram-orders-bot
   ```

2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

3. Настройте конфигурацию:
   - В файле `config.py` укажите:
     ```python
     DB_CONFIG = {
         'host': 'your_mysql_host',
         'user': 'your_mysql_user',
         'password': 'your_mysql_password',
         'database': 'your_database_name'
     }
     
     TOKEN = 'your_telegram_bot_token'
     ADMIN_CHAT_ID = 'your_admin_chat_id'
     ```

4. Запустите бота:
   ```bash
   python bot.py
   ```

## Структура базы данных

Бот работает с двумя основными таблицами:

1. **emergency_calls** - экстренные вызовы:
   ```sql
   CREATE TABLE `emergency_calls` (
     `id` INT AUTO_INCREMENT PRIMARY KEY,
     `phone` VARCHAR(20) NOT NULL,
     `address` TEXT NOT NULL,
     `problem_description` TEXT NOT NULL,
     `call_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
     `status` ENUM('new', 'notified', 'in_progress', 'completed') DEFAULT 'new',
     `notes` TEXT
   );
   ```

2. **service_orders** - заказы услуг:
   ```sql
   CREATE TABLE `service_orders` (
     `id` INT AUTO_INCREMENT PRIMARY KEY,
     `client_name` VARCHAR(100) NOT NULL,
     `phone` VARCHAR(20) NOT NULL,
     `address` TEXT NOT NULL,
     `service_id` INT,
     `additional_info` TEXT,
     `order_date` DATETIME DEFAULT CURRENT_TIMESTAMP,
     `status` ENUM('new', 'notified', 'in_progress', 'completed') DEFAULT 'new',
     FOREIGN KEY (`service_id`) REFERENCES `services`(`id`)
   );
   ```

## Команды и интерфейс

После запуска бота администратор получает меню с кнопками:

- **Новые заказы** - просмотр новых заказов и экстренных вызовов
- **Заказы в работе** - просмотр заказов в статусе "в работе"
- **Завершенные заказы** - просмотр завершенных заказов за выбранный период
- **Статистика** - общая статистика по всем заказам

Для каждого заказа доступны кнопки действий:
- "Взять в работу" - меняет статус заказа на "in_progress"
- "Завершить" - меняет статус заказа на "completed"

## Лицензия

Этот проект распространяется под лицензией MIT.

