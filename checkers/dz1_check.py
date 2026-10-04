#!/usr/bin/env python3
# Самопроверка домашнего задания по Redis
# Можно запускать из любой папки: путь к Compose определяется относительно скрипта.

import json
import subprocess
from pathlib import Path
from shlex import quote

COMPOSE_FILE = Path(__file__).resolve().parent.parent / "infra" / "docker-compose.yml"
COMPOSE = f"docker compose -f {quote(str(COMPOSE_FILE))}"

# Имена сервисов из docker-compose.yml. Поправьте, если ваши отличаются.
MASTER = "redis-master"
SENTINEL = "sentinel-1"
SENTINEL_PORT = "26379"
MASTER_NAME = "mymaster"

PASS = 0
FAIL = 0
SKIP = 0

def run(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True,
                                text=True, timeout=15)
        return result.stdout.strip().splitlines() if result.stdout else []
    except Exception:
        return []

def run_redis(cmd):
    return run(f"{COMPOSE} exec -T {MASTER} redis-cli --raw {cmd}")

def run_sentinel(cmd):
    return run(f"{COMPOSE} exec -T {SENTINEL} redis-cli -p {SENTINEL_PORT} --raw {cmd}")

def get_first(lines):
    return lines[0].strip() if lines else None

def is_int(value):
    try:
        int(value)
        return True
    except (TypeError, ValueError):
        return False

def ok(msg):
    global PASS
    PASS += 1
    print(f"[OK] {msg}")

def fail(msg):
    global FAIL
    FAIL += 1
    print(f"[FAIL] {msg}")

def skip(msg):
    global SKIP
    SKIP += 1
    print(f"[SKIP] {msg}")


# 1. Инфраструктура Docker Compose
print("=== 1. Инфраструктура Docker Compose ===")
output = run(f"{COMPOSE} ps --format json")
running = 0
for line in output:
    try:
        obj = json.loads(line)
        if obj.get("State") == "running":
            running += 1
    except json.JSONDecodeError:
        continue
if running >= 6:
    ok(f"Запущено контейнеров: {running} (минимум 6)")
else:
    fail(f"Запущено контейнеров: {running}, ожидалось минимум 6 (мастер, 2 реплики, 3 Sentinel)")


# 2. Репликация и Sentinel
print("\n=== 2. Репликация и Sentinel ===")
lines = run_redis("INFO replication")
slaves = None
for line in lines:
    if line.startswith("connected_slaves:"):
        slaves = line.split(":", 1)[1].strip()
        break
if slaves == "2":
    ok("Репликация: подключено 2 реплики")
else:
    fail(f"Репликация: подключено реплик {slaves}, ожидалось 2")

lines = run_sentinel(f"SENTINEL get-master-addr-by-name {MASTER_NAME}")
addr = get_first(lines)
if addr:
    ok(f"Sentinel видит мастер: {addr}")
else:
    fail("Sentinel не видит мастер")


# 3. Профили, лидерборд, достижения
print("\n=== 3. Профили, лидерборд, достижения ===")
typ = get_first(run_redis("TYPE player:1001"))
if typ == "hash":
    ok("Профиль player:1001 существует и имеет тип Hash")
else:
    fail(f"Профиль player:1001 не найден или имеет тип {typ}")

size = get_first(run_redis("ZCARD tournament:main"))
if is_int(size) and int(size) > 0:
    ok(f"Лидерборд tournament:main содержит {size} игроков")
else:
    fail("Лидерборд tournament:main пуст или не найден")

size = get_first(run_redis("SCARD achievements:1001"))
if is_int(size) and int(size) > 0:
    ok(f"У игрока 1001 есть {size} достижений")
else:
    fail("У игрока 1001 нет достижений")


# 4. Кэширование
print("\n=== 4. Кэширование ===")
ttl = get_first(run_redis("TTL cache:player:1001"))
if is_int(ttl) and int(ttl) > 0:
    ok(f"Кэш cache:player:1001 существует, TTL = {ttl} сек")
elif ttl == "-2":
    skip("Кэш cache:player:1001 отсутствует (нужен GET-запрос к API для заполнения)")
else:
    fail(f"Кэш cache:player:1001 существует, но TTL = {ttl}, ожидалось больше 0")


# 5. Счетчик входов
print("\n=== 5. Счетчик входов ===")
value = get_first(run_redis("GET logins:1001"))
ttl = get_first(run_redis("TTL logins:1001"))
if is_int(value) and int(value) > 0 and is_int(ttl) and int(ttl) > 0:
    ok(f"Счетчик logins:1001 = {value}, TTL = {ttl} сек")
else:
    fail("Счетчик logins:1001 не найден, пуст или не имеет TTL")


# 6. Очередь уведомлений (Streams)
print("\n=== 6. Очередь уведомлений (Streams) ===")
xlen = get_first(run_redis("XLEN notifications"))
if is_int(xlen) and int(xlen) > 0:
    ok(f"Стрим notifications содержит {xlen} сообщений")
else:
    fail("Стрим notifications пуст или не найден")

lines = run_redis("XINFO GROUPS notifications")
if any("notifications-group" in line for line in lines):
    ok("Группа потребителей notifications-group создана")
else:
    fail("Группа потребителей notifications-group не найдена")


# 7. Конвейеризация
print("\n=== 7. Конвейеризация ===")
cursor = "0"
count = 0
while True:
    lines = run_redis(f"SCAN {cursor} MATCH 'player:*' COUNT 100")
    if not lines:
        break
    cursor = lines[0].strip()
    count += len(lines) - 1
    if cursor == "0":
        break
if count >= 10:
    ok(f"Найдено профилей player:*: {count} (минимум 10)")
else:
    skip(f"Найдено профилей player:*: {count}, проверьте массовую загрузку через pipeline")


# 8. Защита от split-brain
print("\n=== 8. Защита от split-brain ===")
lines = run_redis("CONFIG GET min-replicas-to-write")
mrw = lines[1].strip() if len(lines) >= 2 else None
if is_int(mrw) and int(mrw) >= 1:
    ok(f"min-replicas-to-write = {mrw}")
else:
    fail(f"min-replicas-to-write = {mrw}, ожидалось минимум 1")


# 9. Персистентность
print("\n=== 9. Персистентность ===")
lines = run_redis("CONFIG GET appendonly")
appendonly = lines[1].strip() if len(lines) >= 2 else None
if appendonly == "yes":
    ok("AOF включен (appendonly yes)")
else:
    fail(f"appendonly = {appendonly}, ожидалось yes")


# Итог
print("\n=== Результат ===")
print(f"Пройдено: {PASS}")
print(f"Не пройдено: {FAIL}")
print(f"Пропущено: {SKIP}")
print(f"Всего проверок: {PASS + FAIL + SKIP}")

if FAIL == 0:
    print("Все обязательные проверки пройдены. Можно записывать видео-презентацию.")
else:
    print("Есть ошибки. Исправьте их и запустите скрипт снова.")
