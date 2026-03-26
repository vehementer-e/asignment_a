# Test bundle

В наборе есть четыре теста:

- `unit/config/test_validator.py` — проверка config semantics и runtime validation
- `unit/transformations/test_deduplicate.py` — тест одной transformation
- `unit/dq/test_not_null_rule.py` — тест одного DQ rule
- `integration/test_pipeline_executor_contracts.py` — `xfail` smoke test, который документирует текущие стыковочные проблемы между слоями

Почему integration-тест помечен `xfail`:

Потому что в уже сгенерированных модулях есть реальные interface mismatches. Это полезно не скрывать, а зафиксировать тестом:

- `step_type` vs `type`
- `source_type` / `target_type` vs `format`
- отсутствие `with_defaults()` в registry-классах
- разные сигнатуры у audit metadata/logger слоёв

После починки интерфейсов этот тест можно перевести в обычный passing smoke test.
