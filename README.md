# GameHub

REST API на Rust + Axum для управления профилями игроков, счётчиком входов, рейтингом, достижениями и уведомлениями. Redis используется как хранилище и кеш. Проект выполнен в рамках ДЗ в Центральном Университете.

## Запуск

Для запуска нужен Docker с Compose или Podman с Compose-провайдером. Для выполнения примеров и самопроверки нужны `curl` и Python 3. Приложение собирается внутри контейнера, отдельно устанавливать Rust не нужно.

Все команды выполняются из каталога `gamehub`, где находятся папки `app` и `infra`. Варианты запуска:

Podman:

```bash
podman compose -f infra/docker-compose.yml up --build -d
podman compose -f infra/docker-compose.yml ps
```

Docker:

```bash
docker compose -f infra/docker-compose.yml up --build -d
docker compose -f infra/docker-compose.yml ps
```

Далее команды приведены для Podman. Для Docker вместо `podman` используется `docker`.

После запуска сервисы доступны по следующим адресам:

| Сервис | Адрес / порт на хосте |
| --- | --- |
| API | `http://localhost:8080` |
| Redis Insight | `http://localhost:5540` |
| `redis-master` | `6379` |
| `redis-replica-1`, `redis-replica-2` | `6380`, `6381` |
| `sentinel-1`, `sentinel-2`, `sentinel-3` | `26379`, `26380`, `26381` |

Проверка API и просмотр логов после запуска контейнеров:

```bash
curl -sS http://localhost:8080/health
podman logs --tail 20 gamehub-app
```

`/health` возвращает `{"status":"running"}` и проверяет доступность HTTP-сервера. Состояние Redis проверяется отдельно.

У приложения настроен `restart: on-failure`. При первом запуске Redis может ещё синхронизировать реплики и отклонять запись с `NOREPLICAS`. В этом случае контейнер приложения автоматически перезапускается. Ответ от `/health` подтверждает готовность HTTP-сервера к обработке запросов.

## Архитектура

Compose запускает приложение, мастер Redis, две реплики, три Sentinel и Redis Insight в общей сети `gamehub`.

Адрес мастера приложение получает через Sentinel, имя группы — `mymaster`. Записи, чтение профилей и работа с кешем идут в текущий мастер. Рейтинг и достижения читаются с реплик; если подключиться к реплике не получилось, используется мастер. Репликация асинхронная, поэтому чтение с реплики может немного отставать от записи.

Проект использует гексагональную архитектуру:

```text
app/
  src/
    domain.rs          — игрок, очки, уведомление
    application.rs     — логика операций и валидация
    ports.rs           — интерфейсы хранилища, кеша и уведомлений
    adapters/
      http.rs          — роуты, JSON, HTTP-ответы
      redis.rs         — команды Redis, Sentinel, потребитель уведомлений
    config.rs          — настройки из переменных окружения
    error.rs           — ошибки приложения
    main.rs            — сборка зависимостей и запуск
  Dockerfile
infra/
  docker-compose.yml
  redis/               — конфиги мастера и реплик
  sentinel/            — конфиги трёх Sentinel
```

HTTP-обработчики вызывают `GameHub` из `application.rs`, а он работает с Redis через интерфейсы из `ports.rs`. Потребитель уведомлений запускается фоновой задачей в том же приложении.

Настройки подключения находятся в `app/src/config.rs`. По умолчанию приложение обращается к `sentinel-1:26379`, `sentinel-2:26379`, `sentinel-3:26379` внутри сети контейнеров. Эти адреса рассчитаны на запуск через Compose. Для `cargo run` на хосте нужны доступные с хоста адреса Sentinel и узлов Redis.

## Данные в Redis

| Ключ | Тип | Содержимое | Срок хранения |
| --- | --- | --- | --- |
| `player:{id}` | Hash | `name`, `level`, `region`, `created_at` | Без TTL |
| `logins:{id}` | String | Количество входов | 24 часа с первого входа |
| `cache:player:{id}` | String | Профиль в JSON | 60 секунд |
| `tournament:main` | Sorted Set | `player_id` → очки | Без TTL |
| `achievements:{id}` | Set | Названия достижений | Без TTL |
| `notifications` | Stream | `player_id`, `type`, `message`, `timestamp` | 7 дней через удаление старых записей |

Профиль записывается через `HSET`, уровень меняется через `HINCRBY`. `created_at` хранится в Unix-секундах и сохраняется при обновлении, в том числе через batch.

Для входов используется Lua: `INCR` и установка `EXPIRE 86400` при первом входе выполняются атомарно. Следующие входы увеличивают счётчик, но не продлевают TTL. Остальная работа с Redis сделана обычными командами.

Кеш работает по Cache-Aside: сначала `GET cache:player:{id}`, при промахе — `HGETALL player:{id}` и запись JSON на 60 секунд. Создание или обновление профиля, изменение уровня и batch удаляют соответствующий кеш. Сам `POST` кеш не заполняет — это делает следующий `GET`.

Изменения профиля отправляются в `notifications` через `XADD`. Группа `notifications-group` создаётся при старте через `XGROUP CREATE ... $ MKSTREAM`. Потребитель `gamehub-worker` читает записи через `XREADGROUP`, пишет их в лог и подтверждает через `XACK`. Есть обработка неподтверждённых сообщений. Примерно раз в минуту работающий потребитель удаляет записи старше семи дней через `XTRIM MINID`.

На мастере и репликах включены RDB (`save 60 1`) и AOF (`appendfsync everysec`, переписывание при росте на 100%). Данные лежат в отдельных томах. Лимит памяти — 256 МБ, политика — `volatile-lru`, то есть вытеснять можно ключи с TTL.

Для защиты от записи без реплик настроены `min-replicas-to-write 1` и `min-replicas-max-lag 10`. У Sentinel кворум 2 из 3, `down-after-milliseconds` — 5000, `failover-timeout` — 60000.

## API

В запросах с телом нужен `Content-Type: application/json`.

| Метод | Путь | Тело / результат |
| --- | --- | --- |
| POST | `/api/players/{id}` | `{"name":"Vlad","level":1,"region":"ru"}` — создание или обновление профиля |
| GET | `/api/players/{id}` | Профиль, `cache_hit` и заголовок `X-Cache: HIT` или `MISS` |
| PATCH | `/api/players/{id}/level` | `{"delta":2}` — изменение уровня на `delta` |
| POST | `/api/players/{id}/login` | Без тела; возвращает количество входов |
| POST | `/api/leaderboard/score` | `{"player_id":1001,"score":100}` — добавление очков |
| GET | `/api/leaderboard/top?limit=10` | Игроки по убыванию очков; по умолчанию 10 |
| GET | `/api/leaderboard/rank/{playerId}` | `player_id` и `rank`, либо `rank: null`, если игрока нет в рейтинге |
| POST | `/api/players/{id}/achievements` | `{"name":"first_win"}` — добавление достижения; `added` показывает, было ли оно новым |
| GET | `/api/players/{id}/achievements/{name}` | `{"exists":true}` или `{"exists":false}` |
| GET | `/api/players/{id1}/achievements/common/{id2}` | Общие достижения в массиве `achievements` |
| POST | `/api/players/batch` | `{"players":[{"id":1001,"name":"Vlad","level":1,"region":"ru"}]}`; возвращает `created` и `elapsed_ms` |

Перед входом, изменением уровня, добавлением очков или достижения профиль должен существовать. В batch можно передать от 1 до 1000 профилей с разными ID, в `limit` — от 1 до 1000.

Эндпоинт `rank` использует `ZRANK`, как указано в задании: нумерация с нуля, порядок от меньшего количества очков к большему. Топ при этом идёт в обратном порядке через `ZREVRANGE`. Прибавление очков сделано через `ZINCRBY`, поэтому повторный запрос ещё раз увеличит результат.

Ошибки валидации возвращают `400`, отсутствующий профиль — `404`, недоступность Redis — `503`.

## Проверка API

Примеры рассчитаны на последовательное выполнение в одном терминале. Настройка адреса API, определение текущего мастера и проверка репликации:

```bash
GAMEHUB_API=http://localhost:8080
GAMEHUB_MASTER=$(podman exec sentinel-1 redis-cli -p 26379 --raw \
  SENTINEL get-master-addr-by-name mymaster | head -n 1 | tr -d '\r')

podman exec "$GAMEHUB_MASTER" redis-cli INFO replication
podman exec sentinel-1 redis-cli -p 26379 SENTINEL CKQUORUM mymaster
```

В `INFO replication` должны быть `role:master`, `connected_slaves:2` и обе реплики в состоянии `online`. Sentinel должен подтвердить наличие кворума.

### Профили и кеш

Создание двух игроков и два последовательных запроса профиля первого игрока:

```bash
curl -sS -X POST "$GAMEHUB_API/api/players/1001" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Vlad","level":1,"region":"ru"}'

curl -sS -X POST "$GAMEHUB_API/api/players/1002" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Anton","level":1,"region":"ru"}'

curl -i "$GAMEHUB_API/api/players/1001"
curl -i "$GAMEHUB_API/api/players/1001"
podman exec "$GAMEHUB_MASTER" redis-cli TTL cache:player:1001
```

Первый `GET` должен вернуть `X-Cache: MISS`, второй — `HIT`. TTL должен быть больше нуля и не больше 60 секунд.

Изменение уровня и проверка удаления кеша:

```bash
curl -sS -X PATCH "$GAMEHUB_API/api/players/1001/level" \
  -H 'Content-Type: application/json' -d '{"delta":2}'

podman exec "$GAMEHUB_MASTER" redis-cli TTL cache:player:1001
curl -i "$GAMEHUB_API/api/players/1001"
```

До `GET` у кеша TTL `-2`: ключа нет. Следующий запрос снова будет `MISS`, уже с новым уровнем.

### Входы

Фиксация двух входов и проверка значения счётчика и его TTL:

```bash
curl -sS -X POST "$GAMEHUB_API/api/players/1001/login"
curl -sS -X POST "$GAMEHUB_API/api/players/1001/login"
podman exec "$GAMEHUB_MASTER" redis-cli GET logins:1001
podman exec "$GAMEHUB_MASTER" redis-cli TTL logins:1001
```

Счётчик растёт, TTL остаётся положительным. Для нового счётчика он будет около 86400 секунд.

### Рейтинг и достижения

Добавление очков, получение топа и места игрока. Добавление общего достижения двум игрокам, проверка его наличия и пересечения:

```bash
curl -sS -X POST "$GAMEHUB_API/api/leaderboard/score" \
  -H 'Content-Type: application/json' -d '{"player_id":1001,"score":100}'
curl -sS -X POST "$GAMEHUB_API/api/leaderboard/score" \
  -H 'Content-Type: application/json' -d '{"player_id":1002,"score":50}'

curl -sS "$GAMEHUB_API/api/leaderboard/top?limit=10"
curl -sS "$GAMEHUB_API/api/leaderboard/rank/1001"

curl -sS -X POST "$GAMEHUB_API/api/players/1001/achievements" \
  -H 'Content-Type: application/json' -d '{"name":"first_win"}'
curl -sS -X POST "$GAMEHUB_API/api/players/1002/achievements" \
  -H 'Content-Type: application/json' -d '{"name":"first_win"}'

curl -sS "$GAMEHUB_API/api/players/1001/achievements/first_win"
curl -sS "$GAMEHUB_API/api/players/1001/achievements/common/1002"
```

У обоих игроков есть `first_win`, поэтому оно должно попасть в пересечение. Повторное добавление этого достижения вернёт `added: false`.

### Уведомления

Проверка записей в стриме, группы потребителей и логов после создания профилей и изменения уровня:

```bash
podman exec "$GAMEHUB_MASTER" redis-cli XLEN notifications
podman exec "$GAMEHUB_MASTER" redis-cli XRANGE notifications - + COUNT 5
podman exec "$GAMEHUB_MASTER" redis-cli XINFO GROUPS notifications
podman exec "$GAMEHUB_MASTER" redis-cli XPENDING notifications notifications-group
podman logs --tail 20 gamehub-app
```

В логах должны быть `notification delivered`, а после обработки количество pending-сообщений должно стать нулём. Подтверждённые записи остаются в стриме до удаления по возрасту: `XACK` сам их не удаляет.

### Загрузка пачкой

Загрузка 20 профилей одним запросом:

```bash
python3 - <<'PY' | curl -sS -X POST "$GAMEHUB_API/api/players/batch" \
  -H 'Content-Type: application/json' --data-binary @-
import json

print(json.dumps({"players": [
    {"id": i, "name": f"Player {i}", "level": 1, "region": "ru"}
    for i in range(2001, 2021)
]}))
PY
```

В ответе должны быть `created: 20` и время обработки в `elapsed_ms`. В Redis-адаптере эта пачка отправляется через `redis::pipe()`: команды не ждут ответа по одной. Для существующих ID batch обновляет профили; поле `created` показывает количество обработанных записей.

## Самопроверка

Запуск скрипта из задания после выполнения примеров. Повторный запрос профиля заполняет кеш, если он успел истечь:

```bash
curl -sS "$GAMEHUB_API/api/players/1001"
python3 checkers/dz1_check_podman.py
```

Для Docker есть исходный вариант:

```bash
python3 checkers/dz1_check.py
```

Скрипт проверяет данные игрока `1001`, наличие хотя бы 10 профилей и настройки Redis. Данные создаются примерами из предыдущего раздела. Ожидаемый результат — 13 успешных проверок, без ошибок и пропусков.

Самопроверка рассчитана на исходную топологию до смены мастера: скрипт обращается к контейнеру `redis-master` по имени и не определяет его текущую роль.

## Отказоустойчивость

### Защита от split-brain

Перед проверкой должны работать мастер и обе реплики. Определение текущего мастера, приостановка остальных двух узлов и попытка записи:

```bash
GAMEHUB_MASTER=$(podman exec sentinel-1 redis-cli -p 26379 --raw \
  SENTINEL get-master-addr-by-name mymaster | head -n 1 | tr -d '\r')

GAMEHUB_REPLICAS=()
for GAMEHUB_NODE in redis-master redis-replica-1 redis-replica-2; do
  if [ "$GAMEHUB_NODE" != "$GAMEHUB_MASTER" ]; then
    GAMEHUB_REPLICAS+=("$GAMEHUB_NODE")
  fi
done

podman pause "${GAMEHUB_REPLICAS[@]}"
sleep 20
podman exec "$GAMEHUB_MASTER" redis-cli SET demo:split-brain 1
```

Команды с массивом подходят для Bash и Zsh. Запись во время паузы должна вернуть `NOREPLICAS`: у мастера нет реплик с допустимым отставанием. При задержке отключения реплик проверка повторяется через несколько секунд до `unpause`.

Снятие паузы и проверка состояния репликации:

```bash
podman unpause "${GAMEHUB_REPLICAS[@]}"
podman exec "$GAMEHUB_MASTER" redis-cli INFO replication
```

После восстановления `min_slaves_good_slaves` до `2` — повторная запись и удаление проверочного ключа:

```bash
podman exec "$GAMEHUB_MASTER" redis-cli SET demo:split-brain 1
podman exec "$GAMEHUB_MASTER" redis-cli DEL demo:split-brain
```

Команда `SET` должна вернуть `OK`.

### Переключение мастера

Остановка текущего мастера и запрос адреса нового мастера у Sentinel через 15 секунд:

```bash
GAMEHUB_OLD_MASTER=$(podman exec sentinel-1 redis-cli -p 26379 --raw \
  SENTINEL get-master-addr-by-name mymaster | head -n 1 | tr -d '\r')

podman stop "$GAMEHUB_OLD_MASTER"
sleep 15
podman exec sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
```

Адрес должен смениться на одну из реплик. Проверка записи и чтения через приложение:

```bash
curl -sS -X PATCH "$GAMEHUB_API/api/players/1001/level" \
  -H 'Content-Type: application/json' -d '{"delta":1}'
curl -sS "$GAMEHUB_API/api/players/1001"
```

Во время самого переключения возможен `503`; после выбора мастера и подключения реплики запросы должны снова проходить. Перезапускать приложение вручную для смены адреса Redis не нужно.

Запуск старого узла и проверка репликации у нового мастера:

```bash
podman start "$GAMEHUB_OLD_MASTER"
GAMEHUB_MASTER=$(podman exec sentinel-1 redis-cli -p 26379 --raw \
  SENTINEL get-master-addr-by-name mymaster | head -n 1 | tr -d '\r')
podman exec "$GAMEHUB_MASTER" redis-cli INFO replication
```

После синхронизации `INFO replication` должен показывать две реплики в состоянии `online`. Старый мастер становится репликой; имена контейнеров после failover не меняются.

## Остановка

Остановка и удаление контейнеров:

```bash
podman compose -f infra/docker-compose.yml down
```

Тома с данными сохраняются для следующего запуска.
