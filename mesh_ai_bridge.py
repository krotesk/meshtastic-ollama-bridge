import re
import time
import queue
import threading
import requests
import meshtastic.serial_interface
import meshtastic.tcp_interface
from pubsub import pub


# ============================================================
# НАСТРОЙКИ
# ============================================================

# Если авто-поиск Meshtastic-ноды не сработает, укажи COM-порт явно:
# пример: MESHTASTIC_PORT = "COM4"
MESHTASTIC_PORT = None
MESHTASTIC_IP = "192.168.53.20"

# Твоя модель из ollama list:
MODEL = "huihui_ai/qwen3.5-abliterated:0.8b"

OLLAMA_URL = "http://localhost:11434/api/chat"

# Бот будет отвечать только на сообщения, начинающиеся с /ai
# TRIGGER = "/ai"
TRIGGER = "бот"

# Лимит на одну часть ответа.
# Кириллица занимает больше байт, поэтому не ставь слишком много.
# Было 170
MAX_REPLY_BYTES = 150

# Пауза между частями ответа, чтобы не забивать LoRa-эфир
# было 2.0
SEND_DELAY_SECONDS = 4.0

# Если direct-ответы не доходят, поставь False - тогда ответы будут broadcast в канал.
SEND_DIRECT_REPLY = True


# ============================================================
# ВНУТРЕННЯЯ ЛОГИКА
# ============================================================

work_queue = queue.Queue()
seen_packets = set()
radio = None


def split_utf8(text: str, max_bytes: int):
    """
    Делит текст на части так, чтобы не разрезать UTF-8 символы.
    """
    chunks = []
    current = ""

    for ch in text:
        candidate = current + ch
        if len(candidate.encode("utf-8")) > max_bytes:
            if current:
                chunks.append(current)
            current = ch
        else:
            current = candidate

    if current:
        chunks.append(current)

    return chunks


def clean_model_output(text: str) -> str:
    """
    Убирает возможные <think>...</think> блоки и лишние пробелы.
    Некоторые Qwen/Qwen-подобные модели могут их возвращать.
    """
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = text.replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def ask_ollama(user_text: str) -> str:
    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Ты локальный помощник, отвечающий через Meshtastic. "
                    "Отвечай только по-русски. "
                    "Отвечай очень кратко: максимум 1-2 предложения. "
                    "Не используй markdown, списки и рассуждения. "
                    "Не показывай thinking, chain-of-thought или внутренние размышления."
                )
            },
            {
                "role": "user",
                "content": user_text + "\n\n/no_think"
            }
        ],
        "stream": False,
        "think": False,
        "keep_alive": "30m",
        "options": {
            "temperature": 0.3,
            "num_predict": 80,
            "num_ctx": 4096
        }
    }
    response = requests.post(
        OLLAMA_URL,
        json=payload,
        timeout=900
    )
    response.raise_for_status()

    data = response.json()
    # Для /api/chat ответ лежит здесь:
    message = data.get("message", {})
    answer = message.get("content", "")
    # На всякий случай fallback, если Ollama вернёт формат generate:
    if not answer:
        answer = data.get("response", "")

    answer = clean_model_output(answer)
    if not answer:
        print("[DEBUG] Ollama вернула пустой ответ. Полный JSON:")
        print(data)
        return "Модель вернула пустой ответ."
    return answer


def extract_text_from_packet(packet: dict):
    """
    Достаёт текст из Meshtastic-пакета.
    Поддерживает несколько вариантов структуры packet,
    потому что в разных версиях библиотеки поля могут выглядеть чуть иначе.
    """
    decoded = packet.get("decoded") or {}

    portnum = decoded.get("portnum")
    if portnum is not None:
        portnum_str = str(portnum)
        if "TEXT_MESSAGE_APP" not in portnum_str and portnum_str != "1":
            return None

    text = decoded.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()

    data = decoded.get("data")
    if isinstance(data, dict):
        text = data.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()

        payload = data.get("payload")
        if isinstance(payload, bytes):
            try:
                return payload.decode("utf-8").strip()
            except UnicodeDecodeError:
                return None

    payload = decoded.get("payload")
    if isinstance(payload, bytes):
        try:
            return payload.decode("utf-8").strip()
        except UnicodeDecodeError:
            return None

    if isinstance(payload, str) and payload.strip():
        return payload.strip()

    return None


def get_my_node_num(interface):
    try:
        if interface and interface.myInfo:
            return interface.myInfo.my_node_num
    except Exception:
        pass
    return None


def send_mesh_reply(interface, destination, channel_index, text: str):
    outgoing_text = "AI: " + text
    parts = split_utf8(outgoing_text, MAX_REPLY_BYTES)

    for index, part in enumerate(parts, start=1):
        if len(parts) > 1:
            message = f"[{index}/{len(parts)}] {part}"
        else:
            message = part

        print(f"-> отправляю: {message}")

        if SEND_DIRECT_REPLY and destination is not None:
            interface.sendText(
                message,
                destinationId=destination,
                channelIndex=channel_index,
                wantAck=True
            )
        else:
            interface.sendText(
                message,
                channelIndex=channel_index,
                wantAck=False
            )

        time.sleep(SEND_DELAY_SECONDS)


def worker():
    while True:
        interface, destination, channel_index, user_text = work_queue.get()

        try:
            print(f"[LLM] запрос: {user_text}")
            answer = ask_ollama(user_text)
            print(f"[LLM] ответ: {answer}")
            send_mesh_reply(interface, destination, channel_index, answer)

        except Exception as error:
            print(f"[ОШИБКА] {error}")

            try:
                send_mesh_reply(
                    interface,
                    destination,
                    channel_index,
                    f"Ошибка AI-моста: {error}"
                )
            except Exception as send_error:
                print(f"[ОШИБКА ОТПРАВКИ] {send_error}")

        finally:
            work_queue.task_done()


def on_receive(packet, interface=None):
    global radio

    interface = interface or radio

    text = extract_text_from_packet(packet)
    if not text:
        return

    sender_num = packet.get("from")
    sender_id = packet.get("fromId") or sender_num
    packet_id = packet.get("id")
    channel_index = packet.get("channel", 0)

    my_num = get_my_node_num(interface)

    # Не обрабатываем собственные исходящие сообщения
    if my_num is not None and sender_num == my_num:
        return

    # Защита от повторов
    dedup_key = (sender_num, packet_id, text)
    if dedup_key in seen_packets:
        return

    seen_packets.add(dedup_key)

    if len(seen_packets) > 500:
        seen_packets.clear()

    clean_text = text.strip()

    # Бот реагирует только на /ai
    if not clean_text.lower().startswith(TRIGGER):
        print(f"[mesh] игнорирую обычное сообщение от {sender_id}: {clean_text}")
        return

    user_text = clean_text[len(TRIGGER):].strip()
    if not user_text:
        print("[mesh] пустой /ai запрос")
        return

    print(f"<- от {sender_id}: {user_text}")

    # Для direct-ответа лучше использовать числовой node number.
    destination = sender_num

    work_queue.put((interface, destination, channel_index, user_text))


def on_connection(interface, topic=None):
    print("[OK] Подключился к Meshtastic-ноде.")
    try:
        print(f"[INFO] Моя нода: {interface.myInfo}")
    except Exception:
        pass


def main():
    global radio

    print("Запускаю Meshtastic AI bridge...")
    print(f"Модель Ollama: {MODEL}")
    print(f"Триггер: {TRIGGER}")
    print("Для выхода нажми Ctrl+C.")
    print()

    threading.Thread(target=worker, daemon=True).start()

    pub.subscribe(on_receive, "meshtastic.receive")
    pub.subscribe(on_connection, "meshtastic.connection.established")

    if MESHTASTIC_PORT:
        radio = meshtastic.serial_interface.SerialInterface(devPath=MESHTASTIC_PORT)
    elif MESHTASTIC_IP:
        radio = meshtastic.tcp_interface.TCPInterface(MESHTASTIC_IP)
    else:
        radio = meshtastic.serial_interface.SerialInterface()

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nОстанавливаю...")

    finally:
        try:
            radio.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
