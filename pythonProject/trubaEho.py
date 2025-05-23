import logging
from datetime import datetime
import pytz
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
import mysql.connector
from mysql.connector import Error
from apscheduler.schedulers.asyncio import AsyncIOScheduler
 # Драйвер MySQL для Python
                 # Работа с часовыми поясами

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация базы данных
DB_CONFIG = {
    'host': 'your_host',
    'user': 'your_user',
    'password': 'your_password',
    'database': 'your_database'
}

# Конфигурация бота
TOKEN = 'your_telegram_bot_token'
ADMIN_CHAT_ID = 'your_admin_chat_id'  # ID администратора для уведомлений

# Инициализация бота и диспетчера
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Инициализация планировщика
scheduler = AsyncIOScheduler()


# Состояния для FSM
class OrderStates(StatesGroup):
    waiting_for_date_start = State()
    waiting_for_date_end = State()


# Подключение к базе данных
def get_db_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        logger.error(f"Ошибка подключения к MySQL: {e}")
        return None


# Функция для проверки новых заказов
async def check_new_orders():
    connection = get_db_connection()
    if connection is None:
        return

    try:
        cursor = connection.cursor(dictionary=True)

        # Проверяем emergency_calls
        cursor.execute("""
            SELECT `id`, `phone`, `address`, `problem_description`, `call_time`, `status`, `notes` 
            FROM `emergency_calls` 
            WHERE `status` = 'new'
        """)
        emergency_calls = cursor.fetchall()

        for call in emergency_calls:
            message = (
                f"📞 Новый экстренный вызов!\n"
                f"ID: {call['id']}\n"
                f"Телефон: {call['phone']}\n"
                f"Адрес: {call['address']}\n"
                f"Описание проблемы: {call['problem_description']}\n"
                f"Время вызова: {call['call_time']}\n"
                f"Заметки: {call['notes']}"
            )
            await bot.send_message(ADMIN_CHAT_ID, message)
            # Обновляем статус
            cursor.execute("UPDATE `emergency_calls` SET `status` = 'notified' WHERE `id` = %s", (call['id'],))

        # Проверяем service_orders
        cursor.execute("""
            SELECT so.`id`, so.`client_name`, so.`phone`, so.`address`, 
                   so.`service_id`, s.`name` as service_name, 
                   so.`additional_info`, so.`order_date`, so.`status` 
            FROM `service_orders` so
            LEFT JOIN `services` s ON so.`service_id` = s.`id`
            WHERE so.`status` = 'new'
        """)
        service_orders = cursor.fetchall()

        for order in service_orders:
            message = (
                f"🛎 Новый заказ услуги!\n"
                f"ID: {order['id']}\n"
                f"Клиент: {order['client_name']}\n"
                f"Телефон: {order['phone']}\n"
                f"Адрес: {order['address']}\n"
                f"Услуга: {order['service_name']} (ID: {order['service_id']})\n"
                f"Доп. информация: {order['additional_info']}\n"
                f"Дата заказа: {order['order_date']}"
            )
            await bot.send_message(ADMIN_CHAT_ID, message)
            # Обновляем статус
            cursor.execute("UPDATE `service_orders` SET `status` = 'notified' WHERE `id` = %s", (order['id'],))

        connection.commit()
    except Error as e:
        logger.error(f"Ошибка при проверке новых заказов: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Команда /start
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    if str(message.from_user.id) == ADMIN_CHAT_ID:
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        buttons = ["Новые заказы", "Заказы в работе", "Завершенные заказы", "Статистика"]
        keyboard.add(*buttons)
        await message.answer("Добро пожаловать в панель управления заказами!", reply_markup=keyboard)
    else:
        await message.answer("У вас нет доступа к этому боту.")


# Обработка кнопки "Новые заказы"
@dp.message_handler(lambda message: message.text == "Новые заказы")
async def show_new_orders(message: types.Message):
    if str(message.from_user.id) != ADMIN_CHAT_ID:
        await message.answer("У вас нет доступа к этой функции.")
        return

    connection = get_db_connection()
    if connection is None:
        await message.answer("Ошибка подключения к базе данных.")
        return

    try:
        cursor = connection.cursor(dictionary=True)

        # Получаем новые emergency_calls
        cursor.execute("""
            SELECT `id`, `phone`, `address`, `problem_description`, `call_time`, `status`, `notes` 
            FROM `emergency_calls` 
            WHERE `status` IN ('new', 'notified')
            ORDER BY `call_time` DESC
        """)
        emergency_calls = cursor.fetchall()

        if not emergency_calls:
            await message.answer("Нет новых экстренных вызовов.")
        else:
            for call in emergency_calls:
                keyboard = types.InlineKeyboardMarkup()
                keyboard.add(types.InlineKeyboardButton(
                    text="Взять в работу",
                    callback_data=f"emergency_work_{call['id']}"
                ))

                message_text = (
                    f"📞 Экстренный вызов (Статус: {call['status']})\n"
                    f"ID: {call['id']}\n"
                    f"Телефон: {call['phone']}\n"
                    f"Адрес: {call['address']}\n"
                    f"Описание проблемы: {call['problem_description']}\n"
                    f"Время вызова: {call['call_time']}\n"
                    f"Заметки: {call['notes']}"
                )
                await message.answer(message_text, reply_markup=keyboard)

        # Получаем новые service_orders
        cursor.execute("""
            SELECT so.`id`, so.`client_name`, so.`phone`, so.`address`, 
                   so.`service_id`, s.`name` as service_name, 
                   so.`additional_info`, so.`order_date`, so.`status` 
            FROM `service_orders` so
            LEFT JOIN `services` s ON so.`service_id` = s.`id`
            WHERE so.`status` IN ('new', 'notified')
            ORDER BY so.`order_date` DESC
        """)
        service_orders = cursor.fetchall()

        if not service_orders:
            await message.answer("Нет новых заказов услуг.")
        else:
            for order in service_orders:
                keyboard = types.InlineKeyboardMarkup()
                keyboard.add(types.InlineKeyboardButton(
                    text="Взять в работу",
                    callback_data=f"order_work_{order['id']}"
                ))

                message_text = (
                    f"🛎 Заказ услуги (Статус: {order['status']})\n"
                    f"ID: {order['id']}\n"
                    f"Клиент: {order['client_name']}\n"
                    f"Телефон: {order['phone']}\n"
                    f"Адрес: {order['address']}\n"
                    f"Услуга: {order['service_name']} (ID: {order['service_id']})\n"
                    f"Доп. информация: {order['additional_info']}\n"
                    f"Дата заказа: {order['order_date']}"
                )
                await message.answer(message_text, reply_markup=keyboard)

    except Error as e:
        logger.error(f"Ошибка при получении новых заказов: {e}")
        await message.answer("Произошла ошибка при получении данных.")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Обработка кнопки "Заказы в работе"
@dp.message_handler(lambda message: message.text == "Заказы в работе")
async def show_in_progress_orders(message: types.Message):
    if str(message.from_user.id) != ADMIN_CHAT_ID:
        await message.answer("У вас нет доступа к этой функции.")
        return

    connection = get_db_connection()
    if connection is None:
        await message.answer("Ошибка подключения к базе данных.")
        return

    try:
        cursor = connection.cursor(dictionary=True)

        # Получаем emergency_calls в работе
        cursor.execute("""
            SELECT `id`, `phone`, `address`, `problem_description`, `call_time`, `status`, `notes` 
            FROM `emergency_calls` 
            WHERE `status` = 'in_progress'
            ORDER BY `call_time` DESC
        """)
        emergency_calls = cursor.fetchall()

        if not emergency_calls:
            await message.answer("Нет экстренных вызовов в работе.")
        else:
            for call in emergency_calls:
                keyboard = types.InlineKeyboardMarkup()
                keyboard.add(types.InlineKeyboardButton(
                    text="Завершить",
                    callback_data=f"emergency_complete_{call['id']}"
                ))

                message_text = (
                    f"📞 Экстренный вызов (В работе)\n"
                    f"ID: {call['id']}\n"
                    f"Телефон: {call['phone']}\n"
                    f"Адрес: {call['address']}\n"
                    f"Описание проблемы: {call['problem_description']}\n"
                    f"Время вызова: {call['call_time']}\n"
                    f"Заметки: {call['notes']}"
                )
                await message.answer(message_text, reply_markup=keyboard)

        # Получаем service_orders в работе
        cursor.execute("""
            SELECT so.`id`, so.`client_name`, so.`phone`, so.`address`, 
                   so.`service_id`, s.`name` as service_name, 
                   so.`additional_info`, so.`order_date`, so.`status` 
            FROM `service_orders` so
            LEFT JOIN `services` s ON so.`service_id` = s.`id`
            WHERE so.`status` = 'in_progress'
            ORDER BY so.`order_date` DESC
        """)
        service_orders = cursor.fetchall()

        if not service_orders:
            await message.answer("Нет заказов услуг в работе.")
        else:
            for order in service_orders:
                keyboard = types.InlineKeyboardMarkup()
                keyboard.add(types.InlineKeyboardButton(
                    text="Завершить",
                    callback_data=f"order_complete_{order['id']}"
                ))

                message_text = (
                    f"🛎 Заказ услуги (В работе)\n"
                    f"ID: {order['id']}\n"
                    f"Клиент: {order['client_name']}\n"
                    f"Телефон: {order['phone']}\n"
                    f"Адрес: {order['address']}\n"
                    f"Услуга: {order['service_name']} (ID: {order['service_id']})\n"
                    f"Доп. информация: {order['additional_info']}\n"
                    f"Дата заказа: {order['order_date']}"
                )
                await message.answer(message_text, reply_markup=keyboard)

    except Error as e:
        logger.error(f"Ошибка при получении заказов в работе: {e}")
        await message.answer("Произошла ошибка при получении данных.")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Обработка кнопки "Завершенные заказы"
@dp.message_handler(lambda message: message.text == "Завершенные заказы")
async def ask_for_completed_orders_period(message: types.Message):
    if str(message.from_user.id) != ADMIN_CHAT_ID:
        await message.answer("У вас нет доступа к этой функции.")
        return

    await message.answer("Введите начальную дату для отчета (в формате ГГГГ-ММ-ДД):")
    await OrderStates.waiting_for_date_start.set()


# Обработка ввода начальной даты
@dp.message_handler(state=OrderStates.waiting_for_date_start)
async def process_date_start(message: types.Message, state: FSMContext):
    try:
        date_start = datetime.strptime(message.text, "%Y-%m-%d")
        await state.update_data(date_start=date_start)
        await message.answer("Введите конечную дату для отчета (в формате ГГГГ-ММ-ДД):")
        await OrderStates.waiting_for_date_end.set()
    except ValueError:
        await message.answer("Неверный формат даты. Пожалуйста, введите дату в формате ГГГГ-ММ-ДД.")


# Обработка ввода конечной даты и вывод завершенных заказов
@dp.message_handler(state=OrderStates.waiting_for_date_end)
async def process_date_end_and_show_orders(message: types.Message, state: FSMContext):
    try:
        date_end = datetime.strptime(message.text, "%Y-%m-%d")
        data = await state.get_data()
        date_start = data['date_start']

        if date_start > date_end:
            await message.answer("Начальная дата не может быть позже конечной. Попробуйте снова.")
            await state.finish()
            return

        connection = get_db_connection()
        if connection is None:
            await message.answer("Ошибка подключения к базе данных.")
            await state.finish()
            return

        try:
            cursor = connection.cursor(dictionary=True)

            # Получаем завершенные emergency_calls
            cursor.execute("""
                SELECT `id`, `phone`, `address`, `problem_description`, `call_time`, `status`, `notes` 
                FROM `emergency_calls` 
                WHERE `status` = 'completed' 
                AND DATE(`call_time`) BETWEEN %s AND %s
                ORDER BY `call_time` DESC
            """, (date_start.date(), date_end.date()))
            emergency_calls = cursor.fetchall()

            if not emergency_calls:
                await message.answer("Нет завершенных экстренных вызовов за указанный период.")
            else:
                await message.answer(f"📊 Завершенные экстренные вызовы с {date_start.date()} по {date_end.date()}:")
                for call in emergency_calls:
                    message_text = (
                        f"📞 Экстренный вызов (Завершен)\n"
                        f"ID: {call['id']}\n"
                        f"Телефон: {call['phone']}\n"
                        f"Адрес: {call['address']}\n"
                        f"Описание проблемы: {call['problem_description']}\n"
                        f"Время вызова: {call['call_time']}\n"
                        f"Заметки: {call['notes']}"
                    )
                    await message.answer(message_text)

            # Получаем завершенные service_orders
            cursor.execute("""
                SELECT so.`id`, so.`client_name`, so.`phone`, so.`address`, 
                       so.`service_id`, s.`name` as service_name, 
                       so.`additional_info`, so.`order_date`, so.`status` 
                FROM `service_orders` so
                LEFT JOIN `services` s ON so.`service_id` = s.`id`
                WHERE so.`status` = 'completed'
                AND DATE(so.`order_date`) BETWEEN %s AND %s
                ORDER BY so.`order_date` DESC
            """, (date_start.date(), date_end.date()))
            service_orders = cursor.fetchall()

            if not service_orders:
                await message.answer("Нет завершенных заказов услуг за указанный период.")
            else:
                await message.answer(f"📊 Завершенные заказы услуг с {date_start.date()} по {date_end.date()}:")
                for order in service_orders:
                    message_text = (
                        f"🛎 Заказ услуги (Завершен)\n"
                        f"ID: {order['id']}\n"
                        f"Клиент: {order['client_name']}\n"
                        f"Телефон: {order['phone']}\n"
                        f"Адрес: {order['address']}\n"
                        f"Услуга: {order['service_name']} (ID: {order['service_id']})\n"
                        f"Доп. информация: {order['additional_info']}\n"
                        f"Дата заказа: {order['order_date']}"
                    )
                    await message.answer(message_text)

        except Error as e:
            logger.error(f"Ошибка при получении завершенных заказов: {e}")
            await message.answer("Произошла ошибка при получении данных.")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

        await state.finish()
    except ValueError:
        await message.answer("Неверный формат даты. Пожалуйста, введите дату в формате ГГГГ-ММ-ДД.")


# Обработка callback для взятия в работу emergency_call
@dp.callback_query_handler(lambda c: c.data.startswith('emergency_work_'))
async def process_emergency_work(callback_query: types.CallbackQuery):
    if str(callback_query.from_user.id) != ADMIN_CHAT_ID:
        await callback_query.answer("У вас нет доступа к этой функции.")
        return

    call_id = callback_query.data.split('_')[2]
    connection = get_db_connection()
    if connection is None:
        await callback_query.answer("Ошибка подключения к базе данных.")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("""
            UPDATE `emergency_calls` 
            SET `status` = 'in_progress' 
            WHERE `id` = %s AND `status` IN ('new', 'notified')
        """, (call_id,))

        if cursor.rowcount == 0:
            await callback_query.answer("Не удалось обновить статус. Возможно, заказ уже обработан.")
        else:
            connection.commit()
            await callback_query.answer("Статус обновлен: В работе")
            await bot.edit_message_reply_markup(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                reply_markup=None
            )
            await bot.send_message(
                callback_query.message.chat.id,
                f"Экстренный вызов ID {call_id} взят в работу."
            )
    except Error as e:
        logger.error(f"Ошибка при обновлении статуса emergency_call: {e}")
        await callback_query.answer("Произошла ошибка при обновлении статуса.")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Обработка callback для завершения emergency_call
@dp.callback_query_handler(lambda c: c.data.startswith('emergency_complete_'))
async def process_emergency_complete(callback_query: types.CallbackQuery):
    if str(callback_query.from_user.id) != ADMIN_CHAT_ID:
        await callback_query.answer("У вас нет доступа к этой функции.")
        return

    call_id = callback_query.data.split('_')[2]
    connection = get_db_connection()
    if connection is None:
        await callback_query.answer("Ошибка подключения к базе данных.")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("""
            UPDATE `emergency_calls` 
            SET `status` = 'completed' 
            WHERE `id` = %s AND `status` = 'in_progress'
        """, (call_id,))

        if cursor.rowcount == 0:
            await callback_query.answer("Не удалось обновить статус. Возможно, заказ уже обработан.")
        else:
            connection.commit()
            await callback_query.answer("Статус обновлен: Завершен")
            await bot.edit_message_reply_markup(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                reply_markup=None
            )
            await bot.send_message(
                callback_query.message.chat.id,
                f"Экстренный вызов ID {call_id} завершен."
            )
    except Error as e:
        logger.error(f"Ошибка при обновлении статуса emergency_call: {e}")
        await callback_query.answer("Произошла ошибка при обновлении статуса.")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Обработка callback для взятия в работу service_order
@dp.callback_query_handler(lambda c: c.data.startswith('order_work_'))
async def process_order_work(callback_query: types.CallbackQuery):
    if str(callback_query.from_user.id) != ADMIN_CHAT_ID:
        await callback_query.answer("У вас нет доступа к этой функции.")
        return

    order_id = callback_query.data.split('_')[2]
    connection = get_db_connection()
    if connection is None:
        await callback_query.answer("Ошибка подключения к базе данных.")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("""
            UPDATE `service_orders` 
            SET `status` = 'in_progress' 
            WHERE `id` = %s AND `status` IN ('new', 'notified')
        """, (order_id,))

        if cursor.rowcount == 0:
            await callback_query.answer("Не удалось обновить статус. Возможно, заказ уже обработан.")
        else:
            connection.commit()
            await callback_query.answer("Статус обновлен: В работе")
            await bot.edit_message_reply_markup(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                reply_markup=None
            )
            await bot.send_message(
                callback_query.message.chat.id,
                f"Заказ услуги ID {order_id} взят в работу."
            )
    except Error as e:
        logger.error(f"Ошибка при обновлении статуса service_order: {e}")
        await callback_query.answer("Произошла ошибка при обновлении статуса.")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Обработка callback для завершения service_order
@dp.callback_query_handler(lambda c: c.data.startswith('order_complete_'))
async def process_order_complete(callback_query: types.CallbackQuery):
    if str(callback_query.from_user.id) != ADMIN_CHAT_ID:
        await callback_query.answer("У вас нет доступа к этой функции.")
        return

    order_id = callback_query.data.split('_')[2]
    connection = get_db_connection()
    if connection is None:
        await callback_query.answer("Ошибка подключения к базе данных.")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("""
            UPDATE `service_orders` 
            SET `status` = 'completed' 
            WHERE `id` = %s AND `status` = 'in_progress'
        """, (order_id,))

        if cursor.rowcount == 0:
            await callback_query.answer("Не удалось обновить статус. Возможно, заказ уже обработан.")
        else:
            connection.commit()
            await callback_query.answer("Статус обновлен: Завершен")
            await bot.edit_message_reply_markup(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                reply_markup=None
            )
            await bot.send_message(
                callback_query.message.chat.id,
                f"Заказ услуги ID {order_id} завершен."
            )
    except Error as e:
        logger.error(f"Ошибка при обновлении статуса service_order: {e}")
        await callback_query.answer("Произошла ошибка при обновлении статуса.")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Обработка кнопки "Статистика"
@dp.message_handler(lambda message: message.text == "Статистика")
async def show_statistics(message: types.Message):
    if str(message.from_user.id) != ADMIN_CHAT_ID:
        await message.answer("У вас нет доступа к этой функции.")
        return

    connection = get_db_connection()
    if connection is None:
        await message.answer("Ошибка подключения к базе данных.")
        return

    try:
        cursor = connection.cursor(dictionary=True)

        # Статистика по emergency_calls
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN `status` = 'new' THEN 1 ELSE 0 END) as new,
                SUM(CASE WHEN `status` = 'notified' THEN 1 ELSE 0 END) as notified,
                SUM(CASE WHEN `status` = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN `status` = 'completed' THEN 1 ELSE 0 END) as completed
            FROM `emergency_calls`
        """)
        emergency_stats = cursor.fetchone()

        # Статистика по service_orders
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN `status` = 'new' THEN 1 ELSE 0 END) as new,
                SUM(CASE WHEN `status` = 'notified' THEN 1 ELSE 0 END) as notified,
                SUM(CASE WHEN `status` = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN `status` = 'completed' THEN 1 ELSE 0 END) as completed
            FROM `service_orders`
        """)
        order_stats = cursor.fetchone()

        message_text = (
            "📊 Статистика по заказам:\n\n"
            "📞 Экстренные вызовы:\n"
            f"Всего: {emergency_stats['total']}\n"
            f"Новых: {emergency_stats['new']}\n"
            f"Уведомленных: {emergency_stats['notified']}\n"
            f"В работе: {emergency_stats['in_progress']}\n"
            f"Завершенных: {emergency_stats['completed']}\n\n"
            "🛎 Заказы услуг:\n"
            f"Всего: {order_stats['total']}\n"
            f"Новых: {order_stats['new']}\n"
            f"Уведомленных: {order_stats['notified']}\n"
            f"В работе: {order_stats['in_progress']}\n"
            f"Завершенных: {order_stats['completed']}"
        )

        await message.answer(message_text)

    except Error as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        await message.answer("Произошла ошибка при получении статистики.")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Запуск планировщика при старте бота
async def on_startup(dp):
    scheduler.add_job(check_new_orders, 'interval', minutes=5)
    scheduler.start()


# Завершение планировщика при остановке бота
async def on_shutdown(dp):
    scheduler.shutdown()


if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)