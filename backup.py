import os
import json
import sys
import subprocess

# Сначала проверяем и устанавливаем psycopg2-binary
try:
    import psycopg2
except ImportError:
    print("📦 Установка необходимых компонентов (psycopg2-binary)...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
        import psycopg2
    except Exception as e:
        print(f"❌ Не удалось автоматически установить библиотеку: {e}")
        print("Попробуйте установить вручную командой: pip install psycopg2-binary")
        sys.exit(1)

from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")

def run_backup():
    if not DATABASE_URL:
        print("❌ Ошибка: DATABASE_URL не найден в .env файле!")
        return

    print("🔌 Подключение к базе данных Railway...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return

    print("📥 Выгрузка таблицы игроков...")
    try:
        cursor.execute("SELECT user_id, elo FROM players ORDER BY elo DESC;")
        rows = cursor.fetchall()
        
        backup_data = []
        for r in rows:
            backup_data.append({
                "user_id": r[0],
                "elo": r[1]
            })
            
        output_file = "players_backup.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(backup_data, f, indent=4, ensure_ascii=False)
            
        print(f"✅ Успешно! Выгружено {len(backup_data)} игроков.")
        print(f"💾 Данные сохранены в файл: {os.path.abspath(output_file)}")
        
    except Exception as e:
        print(f"❌ Ошибка во время чтения БД: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_backup()
