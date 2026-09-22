# Выбор video pipeline — проверенное состояние 2026-09-22

## Главная граница

`T2V` и обычный first-frame `I2V` создают **похожее действие по тексту**. Они не переносят точную временную траекторию исходного выступления. Для фактического motion transfer нужен driving-video backend: Wan-Animate-2 получает raw driving video, а LTX Control получает Pose/Depth/Canny video conditioning.

## Что выбирать

| Задача | Pipeline | Статус и граница доказательств |
|---|---|---|
| Новый ролик по описанию | LTX 2.3 first-frame I2V/T2V | Подходит для свободной интерпретации простого действия. Не использовать, когда важны направление жеста, точный исполнитель или тайминг. |
| Один Avatar повторяет человеческое движение | Wan-Animate-2 + LightX2V, 6-step LCM | Допущенный основной путь. Проверены 81f и 97f canaries на RTX 4090, включая 97f exact 960×540. Это доказательство для проверенного seated speaking/hand-motion материала, а не универсальная гарантия модели. |
| Нужны явные поза, глубина или края | LTX 2.3 Union-Control | Настоящее control-video conditioning. Использовать Pose для скелета, Depth для пространственного порядка, Canny для силуэта/границ. Результат зависит от качества и временного выравнивания control video. |
| Старый Wan Animate v1 | Не выбирать публично | Отклонён как первый выбор: требует DWPose и в полном 97f тесте лицо было плохо покрыто примерно первые две секунды. Сохранён только как диагностический/rollback baseline. |
| Два Avatar в диалоге | Короткие shots с утверждёнными keyframes; LTX Control для географии, Wan-Animate-2 только для отдельно проверенных single-character shots | Полная двухаватарная история ещё не принята. `Story 4` находится на review; не считать её завершённым доказательством всего сценария. |

## Решения по baseline-кейсам

First-frame baseline остаётся дешёвым контрольным маршрутом, но его результаты сценозависимы:

- `seated-speaking` — candidate для свободной речи и общих жестов;
- `face-object-interaction` — candidate для простого устойчивого контакта с одним предметом;
- `seated-couple-object-action` — сильнейший candidate из шести reviewed baseline cases;
- `single-hand-gesture` — rejected: thumbs-down превратился в thumbs-up;
- `two-person-cafe-conversation` — rejected для reenactment: основной жест перешёл от женщины к мужчине;
- `walk-and-address-camera` — candidate только как loose B-roll, поскольку жест и камера отличаются.

Оставшиеся `rear-view-walk`, `two-person-walk` и `user-meeting` имеют статус `unreviewed`. Их нельзя включать в статистику успеха.

## Что исправили control-retune

Преддекодный latent crop устранил хвостовой reset в `seated-speaking-pose`: кадры 93–96 продолжают текущую речь и жест вместо возврата к frame 0. Выровненный Canny strength 1.0 исправил направление `single-hand-gesture`: thumbs-down сохраняется до конца. При этом согнутые пальцы остаются мягкими; этот результат подтверждает направление жеста, но не fine-finger articulation.

## Правила допуска

1. Для motion transfer не подменять driving-video conditioning более подробным prompt.
2. Считать `candidate` только в пределах просмотренного evidence scope: sparse samples и all-97-frame review — разные уровни доказательств.
3. Перед production acceptance смотреть полный ролик на нормальной скорости со звуком; контактные листы не показывают cadence и flicker полностью.
4. Не распространять успех одного Wan-Animate-2 seated clip на profile motion, props, fast dance, двух персонажей или long continuation.
5. Сохранять exact model/workflow hashes, Avatar revision, source/control hashes, seed, dimensions, frames/FPS и итоговый review verdict.

Машиночитаемые решения находятся в `.codex_tmp/avatar-video-20260922/report/baseline-verdicts.json`.
