# Инструкции по тестированию исправлений 3D Viewer

## Что исправлено

**Приоритет 3: Переключение текстурных каналов**

Исправлены проблемы с переключением между режимами PBR, AO, Normal, Albedo, Metalness, Roughness, Emissive в 3D viewer'е.

### Изменённые файлы:
- `/root/autorig-online/static/js/rig-editor.js` (git repo)
- `/opt/autorig-online/autorig-online/static/js/rig-editor.js` (production)

## Как протестировать

### 1. Перезапустить web сервис (если нужно)

```bash
# Если используется systemd
sudo systemctl restart autorig

# Или если запущено через screen/tmux, перезапустите вручную
```

### 2. Открыть страницу задачи с моделью

1. Перейти на https://autorig.online
2. Загрузить любую 3D модель (GLB/FBX/OBJ)
3. Дождаться завершения обработки
4. Открыть страницу задачи (task?id=...)

### 3. Открыть Developer Console

- **Chrome/Edge**: `F12` или `Ctrl+Shift+I`
- **Firefox**: `F12` или `Ctrl+Shift+K`
- **Safari**: `Cmd+Option+I`

### 4. Тестировать переключение каналов

#### Через UI:
1. Найти селектор каналов в интерфейсе viewer'а
2. Переключиться между всеми каналами по очереди:
   - **PBR** (Default) - полноценный PBR рендеринг
   - **AO** (Ambient Occlusion) - должна показаться чёрно-белая карта затенения
   - **Normal** - цветная карта нормалей (обычно сине-фиолетовая)
   - **Albedo** - базовый цвет без освещения
   - **Metalness** - чёрно-белая карта металличности
   - **Roughness** - чёрно-белая карта шероховатости
   - **Emissive** - светящиеся части модели

#### Через клавиатуру (если реализовано):
- `1` - PBR
- `2` - AO
- `3` - Normal
- `4` - Albedo
- `5` - Metalness
- `6` - Roughness
- `7` - Emissive

### 5. Проверить логи в консоли

При каждом переключении канала должны появляться логи:

```
[ViewerControls] Switching material channel: {from: 1, to: 3, hasModel: true}
[ViewerControls] Applying material channel: {channel: 3, debugMode: 3, ...}
[ViewerControls] Material channel applied: {channel: 3, debugMode: 3, materialsProcessed: 5, ...}
[ViewerControls] Updated u_debug_mode in compiled shader: 3
```

### 6. Что должно произойти

✅ **Правильное поведение:**
- При переключении на **AO**: модель становится чёрно-белой, показывает затенение
- При переключении на **Normal**: модель окрашена в цвета направлений нормалей (RGB = XYZ)
- При переключении на **Albedo**: показывает только базовый цвет текстуры без освещения
- При переключении на **Metalness/Roughness**: чёрно-белые карты
- При переключении на **Emissive**: показывает только светящиеся области
- При возврате на **PBR**: модель отображается нормально с полным освещением

❌ **Неправильное поведение (до исправления):**
- Модель не меняет внешний вид при переключении каналов
- Каналы показывают одинаковую картинку
- Консоль показывает ошибки shader compilation
- Текстуры не загружаются

### 7. Дополнительные проверки

#### Проверка shader injection:
В логах должно быть при первой загрузке модели:
```
[ViewerControls] Injecting post-processing into material: {...}
[ViewerControls] onBeforeCompile called for material, adding uniforms
[ViewerControls] Shader uniforms added, u_debug_mode value: 0
[ViewerControls] Shader injection complete, stored shader reference
```

#### Проверка uniform updates:
При каждом переключении канала:
```
[ViewerControls] Updated u_debug_mode in compiled shader: <новое значение>
```

### 8. Известные ограничения

- **Non-standard materials**: Если модель использует не MeshStandardMaterial/MeshPhysicalMaterial, debug режимы не будут работать (будет warning в консоли)
- **Текстуры не загружены**: Если у модели нет определённых текстур (например, нет AO map), канал может показывать дефолтное значение
- **Performance**: При первом переключении канала может быть небольшая задержка из-за компиляции shader'а

## Если что-то не работает

### Проблема: Каналы не переключаются

1. Проверить логи в консоли - есть ли ошибки?
2. Проверить что `rig-editor.js` обновлён:
   ```bash
   ls -lh /opt/autorig-online/autorig-online/static/js/rig-editor.js
   stat /opt/autorig-online/autorig-online/static/js/rig-editor.js
   ```
3. Очистить кеш браузера (`Ctrl+Shift+R` или `Cmd+Shift+R`)
4. Проверить что в URL есть `?v=25` или выше для cache busting

### Проблема: Логи не появляются

1. Убедиться что открыта вкладка Console в DevTools
2. Проверить фильтры логов (должны быть включены Info/Log)
3. Попробовать вручную вызвать: `viewerControls.setMaterialChannel(3)` в консоли

### Проблема: Shader errors

1. Проверить WebGL support: `chrome://gpu/`
2. Проверить Three.js version compatibility
3. Посмотреть полный текст ошибки в консоли

## Rollback (если нужно вернуть старую версию)

```bash
# Если есть backup
cp /root/autorig-online/static/js/rig-editor.js.backup /opt/autorig-online/autorig-online/static/js/rig-editor.js

# Перезапустить сервис
sudo systemctl restart autorig
```

## Следующие шаги

После успешного тестирования:
1. **Приоритет 0/4**: Исправить проблему с камерой (она "скачет")
2. **Приоритет 1**: Сохранение/загрузка состояния UI (ground, gizmos, controllers)
3. **Приоритет 2**: Fullscreen fallback для браузеров без Fullscreen API

## Контакты

Если возникли вопросы или проблемы, проверь:
- `/root/3dviewer_concept.txt` - полная документация
- `/root/.cursor/plans/fix_3d_viewer_texture_switching_*.plan.md` - план исправлений
