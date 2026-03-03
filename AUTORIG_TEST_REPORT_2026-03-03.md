# Отчет по проверке animations.glb на https://autorig.online

## URL задачи
https://autorig.online/task?id=38db0144-cd2f-4577-9245-ffea57866751

## Дата проверки
2026-03-03 06:56 UTC

## Методика проверки
Анализ исходного кода страницы, API endpoints и логов через curl (браузерное тестирование через MCP недоступно).

---

## Результаты проверки

### ✅ Проверка #1: Загрузка animations.glb вместо animations.fbx
**Статус: PASS**

**Найдено:**
- В HTML коде страницы используется только `animations.glb` (найдено 3 упоминания)
- API endpoint `/api/task/{task_id}/animations.glb` работает корректно (HTTP 200, 10.9 MB)
- В коде НЕТ загрузки `animations.fbx` как bundle

**Console.log сообщения в коде:**
```javascript
console.log('[Viewer] Loading animations GLB from server...');
console.log('[Viewer] ✓ Cached animations model (glb)');
```

**Вердикт:** Код загружает только GLB формат. FBX endpoint существует только для обратной совместимости, но не используется в основном коде.

---

### ✅ Проверка #2: Отсутствие сообщений о "FBX-first" / "background FBX bundle"
**Статус: PASS**

**Найдено:**
- В HTML коде НЕТ упоминаний: "FBX first", "FBX fallback", "FBX bundle"
- Комментарии обновлены на актуальные:
  - ~~"Load animations bundle (FBX first, GLB fallback)"~~ → "Load animations bundle (GLB format)"
  - ~~"Check if animations bundle is ready (.fbx preferred, .glb legacy)"~~ → "Check if animations bundle is ready (using .glb format)"

**Вердикт:** Все упоминания FBX-приоритета удалены из кода.

---

### ⚠️ Проверка #3: Визуальная проверка размера модели
**Статус: ТРЕБУЕТ РУЧНОЙ ПРОВЕРКИ**

**Что было проверено:**
- API endpoint возвращает валидный GLB файл (10.9 MB, content-type: model/gltf-binary)
- Код использует кеширование модели: `modelCache.animations = currentModel`
- При переключении анимаций модель берется из кеша, не перезагружается

**Потенциальные проблемы:**
В коде НЕ найдено применение масштаба/трансформаций при переключении между режимами. Код использует тот же самый `currentModel` объект без изменений.

**Требуется ручная проверка:**
1. Открыть страницу в браузере
2. Включить режим Anim
3. Переключать между custom animations (например: idle → walk → run)
4. Визуально убедиться, что модель сохраняет размер

**Как проверить в браузере (F12 Console):**
```javascript
// После переключения анимации проверить scale модели:
console.log('Model scale:', currentModel.scale);
// Должно быть примерно: {x: 1, y: 1, z: 1} или константное значение
```

---

## Найденные изменения в коде

### Исправлено сегодня (2026-03-03):
1. ✅ Обновлен комментарий: "FBX first, GLB fallback" → "GLB format"
2. ✅ Обновлен комментарий: ".fbx preferred, .glb legacy" → "using .glb format"

### Код работает корректно:
- Использует `/api/task/${taskId}/animations.glb` для загрузки
- Кеширует модель в `modelCache.animations`
- При повторном входе в Anim режим использует кеш (быстрое переключение)

### Технические детали:
```javascript
// Загрузка animations.glb (строка ~6985)
const ok = await loadModel(
    `/api/task/${taskId}/animations.glb`,
    'Animations GLB',
    0,
    120000
);

// Кеширование (строка ~6995)
if (ok) {
    currentModelType = 'animations';
    modelCache.animations = currentModel;
    modelCache.animationsData = [...animations];
    console.log('[Viewer] ✓ Cached animations model (glb)');
}
```

---

## Дополнительные наблюдения

### Backend API:
- ✅ `/api/task/{task_id}/animations.glb` - основной endpoint (работает, возвращает 10.9 MB GLB)
- ⚠️ `/api/task/{task_id}/animations.fbx` - legacy endpoint (работает, возвращает 6.9 MB FBX)
  - Оставлен для обратной совместимости
  - НЕ используется в основном коде страницы

### Task API response:
```json
{
  "ready_urls": [
    "...35ba4439-8e38-4288-a305-7c82bd8ec771_all_animations.glb",
    "...35ba4439-8e38-4288-a305-7c82bd8ec771_all_animations_unity.fbx",
    ...
  ]
}
```
Оба файла доступны в `ready_urls`, но код использует только GLB.

---

## Итоговый вердикт

### ✅ Проверка #1: PASS
Код загружает `animations.glb`, НЕТ загрузки `animations.fbx` как bundle.

### ✅ Проверка #2: PASS
НЕТ сообщений о "FBX-first" или "background FBX bundle" в коде.

### ⚠️ Проверка #3: REQUIRES MANUAL TEST
Визуальная проверка размера модели требует ручного тестирования в браузере.

---

## Рекомендации

1. **Ручное тестирование (критично):**
   - Открыть страницу в браузере
   - Включить Anim режим
   - Переключить 3-4 custom animations
   - Проверить размер модели визуально
   - Проверить console.log (должны быть только GLB логи)

2. **После успешного теста:**
   - Можно рассмотреть удаление `/api/task/{task_id}/animations.fbx` endpoint
   - Или оставить для legacy клиентов

3. **Мониторинг:**
   - Проверить, нет ли старых клиентов, которые используют FBX endpoint
   - Можно добавить логирование обращений к FBX endpoint

---

## Код для ручного тестирования

Откройте Console (F12) на странице и выполните после переключения анимаций:

```javascript
// Проверка текущей модели
console.log('Current model type:', currentModelType);
console.log('Model scale:', currentModel?.scale);
console.log('Model position:', currentModel?.position);

// Проверка кеша
console.log('Animations cached:', !!modelCache.animations);
console.log('Cache data:', modelCache);

// Список доступных анимаций
console.log('Available animations:', animations.map(a => a.name));
```

Ожидаемый вывод:
```
Current model type: animations
Model scale: {x: 1, y: 1, z: 1}
Animations cached: true
Available animations: ['idle', 'walk', 'run', ...]
```

Размер модели (`scale`) должен оставаться константным при переключении анимаций.
