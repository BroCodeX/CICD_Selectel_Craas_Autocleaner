# CR Cleanup

Скрипт очищает старые Docker-образы в Selectel Container Registry по правилам из YAML.

Формат образов:
`cr.selcloud.ru/<registry>/<image>:<tag>`

Selectel API референс:
https://docs.selectel.ru/api/craas/

## Что делает
- Берёт список репозиториев в реестре.
- Исключает репозитории, подпадающие под `exclude_repo` (если задан).
- Для каждого репозитория берёт список образов.
- Применяет правила из конфига сверху вниз (приоритет у верхних).
- Для образов, попавших под правило, защищает `keep_latest` последних и удаляет только те, что старше `remove_older` (в сутках).
- Для образов, не попавших ни под одно правило (`unmatched`), оставляет последние `10` и удаляет остальные.
- В `DRY_RUN=true` только показывает, что было бы удалено.

## Где настраивать правила
Файл по умолчанию: [rules/cleanup_rules_default.yaml](rules/cleanup_rules_default.yaml)

Можно задать другой путь через переменную `CLEAN_CONFIG_PATH`.

Пример:
```yaml
exclude_repo: "devsec/.*"    # опционально — исключить репозитории по regexp
cleanup_rules:
  logistics_release_app:
    regexp: logistics-service\:.*-release-.*-app-.*
    keep_latest: 10
    remove_older: 14
  logistics_review:
    regexp: logistics-service\:.*-review-.*
    keep_latest: 1
    remove_older: 14
  all_release:
    regexp: .*\:.*-release-.*
    keep_latest: 5
    remove_older: 14
```

## exclude_repo
Опциональное поле верхнего уровня в YAML-конфиге. Задаёт regexp, по которому исключаются репозитории **до начала обработки**.

- Матчинг по имени репозитория через `re.search` (частичное совпадение).
- Если поле отсутствует или пустое — все репозитории обрабатываются как обычно.
- Невалидный regexp вызывает `sys.exit(1)` при загрузке конфига.

Примеры:
```yaml
exclude_repo: "devsec/.*"          # исключить все репо в namespace devsec
exclude_repo: "^(build|devsec)/"   # исключить build/* и devsec/*
exclude_repo: "-cache$"            # исключить репо с суффиксом -cache
```

## Правила (важно)
- Проверка идёт по строке: `<repo>:<tag>` (пример: `logistics-service:abc-release-app-1`).
- Правила применяются по очереди, сверху вниз.
- Если образ подошёл под первое правило, в следующие правила он уже не попадёт.
- `regexp` обязателен и должен быть непустой строкой.
- `keep_latest` — сколько последних образов (внутри правила) всегда оставить.
- `remove_older` — минимальный возраст образа в сутках для удаления.
- Оба значения ожидаются как целые числа `>= 0`.
- Если `keep_latest` не задан/невалиден, используется дефолт `10` (только для `keep_latest`).
- Если `remove_older` не задан/невалиден, используется дефолт `14` (только для `remove_older`).
- Для `unmatched` применяется только лимит по количеству: `keep_latest=10`.
- Если у образа невалидный или отсутствующий `createdAt`, возрастная проверка для него не выполняется (он не удаляется по `remove_older`).

## Обязательные переменные окружения
Нужно передать:
- `SEL_USERNAME`
- `SEL_PASSWORD`
- `SEL_ACCOUNT_ID`
- `SEL_PROJECT_NAME`
- `SEL_REGISTRY_ID`

Опционально:
- `DRY_RUN` (`true`/`false`, по умолчанию `false`)
- `CLEAN_CONFIG_PATH` (по умолчанию `rules/cleanup_rules_default.yaml`)

## Локальный запуск (Python)
Требуется Python 3.11+.

1. Установить виртуальное окружение:
```bash
python3 -m venv venv
source venv/bin/activate
```

2. Установить зависимости:
```bash
pip install --upgrade pip
pip install requests pyyaml loguru pytest
```

3. Задать переменные окружения.

4. Запустить:
```bash
python3 cleanup_registry.py
```

## GitLab CI
Pipeline описан в [.gitlab-ci.yml](.gitlab-ci.yml).

Этапы:
- `test`: запускает unit-тесты.
- `plan`: запуск с `DRY_RUN=true` (план очистки).
- `cleanup`: фактическое удаление (`DRY_RUN=false`).

Секреты заданы на уровне репо:
`GitLab -> Settings -> CI/CD -> Variables`

Рекомендуется:
- `SEL_USERNAME`, `SEL_PASSWORD` сделать `Masked` и `Protected`.
- `SEL_ACCOUNT_ID`, `SEL_PROJECT_NAME`, `SEL_REGISTRY_ID` добавить как обычные переменные проекта/группы.

Окружения описаны матричной джобой:
- `dev`: development окружение / регистри / правила очистки.
- `prod`: production окружение / регистри / правила очистки.

## Тесты
```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m pytest -v tests
```
`PYTHONDONTWRITEBYTECODE=1` можно убрать, если нужен `__pycache__`.

## Кратко: какой файл за что отвечает
- [cleanup_registry.py](cleanup_registry.py): точка входа, получение токена, общий цикл очистки.
- [config/cleanup_config.py](config/cleanup_config.py): загрузка и проверка YAML-конфига.
- [config/logger_config.py](config/logger_config.py): конфиг логгера, с добавлением кастомных Levels.
- [clients/cleanup_repository.py](clients/cleanup_repository.py): запросы к API реестра (репозитории, образы, удаление).
- [core/cleanup_rules_parser.py](core/cleanup_rules_parser.py): проверка соответствия образов правилам (`regexp`), фильтрация репозиториев по `exclude_repo`.
- [core/cleanup_executor.py](core/cleanup_executor.py): выбор образов к удалению по `keep_latest` + `remove_older`.
- [core/constants.py](core/constants.py): константы/ключи полей API и конфига (`ImageFields`, `ConfigFields`).
- [tests/](tests): тесты на `pytest`.
