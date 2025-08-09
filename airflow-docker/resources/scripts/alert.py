import requests
import os


TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message):
    """Fungsi untuk mengirim pesan ke Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    requests.post(url, data=payload)

def task_fail_alert(context):
    """Notifikasi saat task gagal."""
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    message = (
        f"❌ <b>Airflow Task Failed</b>\n"
        f"<b>DAG:</b> {dag_id}\n"
        f"<b>Task:</b> {task_id}\n"
        f"<b>Execution Time:</b> {execution_date}\n"
        f"<a href='{log_url}'>🔗 Lihat Log</a>"
    )
    send_telegram_message(message)