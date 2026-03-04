# CR Cleanup

Скрипт очищает старые Docker-образы в Selectel Container Registry по правилам из YAML.

Формат образов:
`cr.selcloud.ru/<registry>/<image>:<tag>`

Selectel API референс:
https://docs.selectel.ru/api/craas/

## Что делает
- Берёт список репозиториев в реестре.
- Для каждого репозитория берёт список образов.
- Применяет правила из конфига сверху вниз (приоритет у верхних).
- Удаляет старые образы по `keep_latest`.
- В `DRY_RUN=true` только показывает, что было бы удалено.

## Где настраивать правила
Файл по умолчанию: [.config/cleanup_rules.yaml](.config/cleanup_rules.yaml)

Можно задать другой путь через переменную `CLEAN_CONFIG_PATH`.

Пример:
```yaml
cleanup_rules:
  logistics_release_app:
    regexp: logistics-service\:.*-release-.*-app-.*
    keep_latest: 10
  logistics_review:
    regexp: logistics-service\:.*-review-.*
    keep_latest: 1
  all_release:
    regexp: .*\:.*-release-.*
    keep_latest: 5
```

## Правила (важно)
- Проверка идёт по строке: `<repo>:<tag>` (пример: `logistics-service:abc-release-app-1`).
- Правила применяются по очереди, сверху вниз.
- Если образ подошёл под первое правило, в следующие правила он уже не попадёт.
- `regexp` обязателен и должен быть непустой строкой.
- `keep_latest` — сколько последних образов оставить для этого правила.
- Образы, которые не подошли ни под одно правило, очищаются по `KEEP_LAST_N`.

## Обязательные переменные окружения
Нужно передать:
- `SEL_USERNAME`
- `SEL_PASSWORD`
- `SEL_ACCOUNT_ID`
- `SEL_PROJECT_NAME`
- `SEL_REGISTRY_ID`

Опционально:
- `KEEP_LAST_N` (по умолчанию `10`)
- `DRY_RUN` (`true`/`false`, по умолчанию `false`)
- `CLEAN_CONFIG_PATH` (по умолчанию `.config/cleanup_rules.yaml`)

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

2. Задать переменные окружения.

3. Запустить:
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

## Тесты
```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m pytest -v tests
```
`PYTHONDONTWRITEBYTECODE=1` можно убрать, если нужен `__pycache__`.

## Кратко: какой файл за что отвечает
- [cleanup_registry.py](cleanup_registry.py): точка входа, получение токена, общий цикл очистки.
- [cleanup_config.py](cleanup_config.py): загрузка и проверка YAML-конфига.
- [cleanup_repository.py](cleanup_repository.py): запросы к API реестра (репозитории, образы, удаление).
- [cleanup_rules_parser.py](cleanup_rules_parser.py): проверка соответствия образов правилам (`regexp`).
- [cleanup_executor.py](cleanup_executor.py): выбор образов к удалению по `keep_latest`.
- [tests/](tests): тесты на `pytest`.
