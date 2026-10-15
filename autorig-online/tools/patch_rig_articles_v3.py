#!/usr/bin/env python3
"""Site refresh to V3 (2026-10-16): bring the texts of the generated per-animal rig pages to V3.

    py -3.11 tools/patch_rig_articles_v3.py <path to backend/main.py> [--dry-run] [--no-backup]

The pages (/dog-auto-rig, /horse-auto-rig, ... /humanoid-auto-rig, plus -ru/-zh/-hi) are rendered by
main.py: RIG_ARTICLE_LANGS / RIG_ARTICLE_TYPES / _rig_article_text. This script changes only text:
the "How it works" heading, the humanoid preparation tip (no T-pose requirement), and in _rig_article_text
the description, lead, workflow and the three FAQ answers of every language, driven by one new helper
(_rig_article_v3_category: quadruped for most animals, the equine template for horse and deer, a low-body
quadruped for the turtle, biped for the humanoid page). Keys, signatures, routes, SEO mechanism (title, h1,
description per language) and the examples wiring are untouched.

Every anchor is exact text and must occur exactly once (the production main.py can be ahead of the local
one, so there are no line numbers). The result is written to a temp file, py_compile-checked and then moved
over main.py (a backup main.py.pre_rig_v3 stays beside it). Re-running on a patched file is a no-op.
"""
import argparse
import os
import py_compile
import shutil
import sys

MARKER = "RIG_ARTICLE_V3_CATEGORIES"

ANCHORS = [
    ('ui workflow heading (en)',
     '        "overview": "What this rig type is for",\n        "workflow": "Online workflow",\n',
     '        "overview": "What this rig type is for",\n        "workflow": "How it works",\n'),
    ('ui workflow heading (ru)',
     '        "overview": "Для чего нужен этот тип рига",\n        "workflow": "Онлайн workflow",\n',
     '        "overview": "Для чего нужен этот тип рига",\n        "workflow": "Как это работает",\n'),
    ('ui workflow heading (zh)',
     '        "overview": "这个绑定类型适合什么",\n        "workflow": "在线工作流",\n',
     '        "overview": "这个绑定类型适合什么",\n        "workflow": "工作方式",\n'),
    ('ui workflow heading (hi)',
     '        "overview": "यह rig type किसके लिए है",\n        "workflow": "Online workflow",\n',
     '        "overview": "यह rig type किसके लिए है",\n        "workflow": "यह कैसे काम करता है",\n'),
    ('humanoid tip: no T-pose requirement',
     '        "tip": {"en": "Use a clear T-pose or relaxed A-pose, keep arms away from the torso, and avoid fused fingers if hand motion matters.", "ru": "Используйте чистую T-pose или relaxed A-pose, держите руки отдельно от тела и не склеивайте пальцы, если важна анимация кистей.", "zh": "使用清晰的 T-pose 或放松的 A-pose，让手臂离开身体；如果需要手部动画，避免手指粘连。", "hi": "Clear T-pose या relaxed A-pose रखें, arms को torso से अलग रखें, और hand motion चाहिए तो fused fingers से बचें।"},',
     '        "tip": {"en": "Any pose works, because the rig is fitted to the model as it is. Keep arms and legs clear of the torso, and avoid fused fingers if hand motion matters.", "ru": "Подойдёт любая поза: риг подгоняется под модель как есть. Держите руки и ноги отдельно от корпуса и не склеивайте пальцы, если важна анимация кистей.", "zh": "任意姿势都可以，因为绑定会按模型当前的样子拟合。让手臂和腿离开躯干；如果需要手部动画，避免手指粘连。", "hi": "कोई भी पोज़ चलता है, क्योंकि रिग मॉडल पर वैसे ही फ़िट होता है जैसा वह है। हाथ-पैर धड़ से अलग रखें, और हाथ की गति चाहिए तो उँगलियाँ आपस में न जुड़ी हों।"},'),
    ('V3 category helper before _rig_article_text',
     'def _rig_article_text(rig_key: str, lang: str) -> Dict[str, Any]:\n',
     '# Site refresh to V3 (2026-10-16): the per-animal pages describe the V3 conveyor and the category the engine\n# really uses: quadruped for most animals, the equine template for horse and deer, a low-body quadruped for the\n# turtle (there is no turtle template), biped for the humanoid page. Value: (short phrase, one sentence).\nRIG_ARTICLE_V3_CATEGORIES: Dict[str, Dict[str, Tuple[str, str]]] = {\n    "quadruped": {\n        "en": ("quadruped category", "The rig uses the quadruped category, with a skeleton template, fit rules, skin rules and a clip library made for four-legged bodies."),\n        "ru": ("категория четвероногих", "Риг строится в категории четвероногих: свой шаблон скелета, правила подгонки и скина и библиотека клипов для четвероногих тел."),\n        "zh": ("四足动物类别", "绑定使用四足动物类别：拥有专为四足身体准备的骨架模板、拟合规则、蒙皮规则和动画库。"),\n        "hi": ("चौपाए श्रेणी", "रिग चौपाए श्रेणी का इस्तेमाल करता है: चार पैरों वाले शरीरों के लिए बना कंकाल टेम्पलेट, फ़िट नियम, स्किन नियम और क्लिप लाइब्रेरी।"),\n    },\n    "equine": {\n        "en": ("equine template", "The rig uses the equine template: scapula, elbow, mid-neck and leg profile joints, and planted-hoof gaits (walk, trot, gallop) that keep the hooves on the ground."),\n        "ru": ("шаблон лошадиных", "Риг строится по шаблону лошадиных: лопатка, локоть, середина шеи и профильные суставы ноги, а походки (шаг, рысь, галоп) держат копыта на земле."),\n        "zh": ("马科模板", "绑定使用马科模板：肩胛、肘部、颈中段和腿部轮廓关节，步态（行走、小跑、飞奔）让蹄子始终踩稳地面。"),\n        "hi": ("अश्व टेम्पलेट", "रिग अश्व टेम्पलेट का इस्तेमाल करता है: स्कैपुला, कोहनी, गर्दन का मध्य और पैर की रूपरेखा वाले जोड़, और चालें (चलना, ट्रॉट, गैलप) जिनमें खुर ज़मीन पर टिके रहते हैं।"),\n    },\n    "turtle": {\n        "en": ("low-body quadruped rig", "The rig is a low-body quadruped: four short legs, a neck and a tail around a rigid shell volume. There is no separate turtle template."),\n        "ru": ("риг низкого четвероногого", "Риг строится как у низкого четвероногого: четыре короткие ноги, шея и хвост вокруг жёсткого объёма панциря. Отдельного шаблона черепахи нет."),\n        "zh": ("低身体四足动物绑定", "绑定按低身体四足动物构建：四条短腿、脖子和尾巴围绕刚性的龟壳体积。没有单独的乌龟模板。"),\n        "hi": ("कम ऊँचाई वाले चौपाए का रिग", "रिग कम ऊँचाई वाले चौपाए की तरह बनता है: कठोर कवच के आसपास चार छोटे पैर, गर्दन और पूँछ। कछुए का अलग टेम्पलेट नहीं है।"),\n    },\n    "biped": {\n        "en": ("biped category", "The rig uses the biped category, which also covers primates. If the legs do not stand side by side, the leg on the floor sets the bone lengths for the other leg."),\n        "ru": ("категория двуногих", "Риг строится в категории двуногих, куда входят и приматы. Если ноги стоят не рядом, нога на полу задаёт длины костей для другой ноги."),\n        "zh": ("双足类别", "绑定使用双足类别，灵长类也属于此类。如果双腿没有并排站立，踩在地面上的那条腿会决定另一条腿的骨骼长度。"),\n        "hi": ("द्विपाद श्रेणी", "रिग द्विपाद श्रेणी का इस्तेमाल करता है, जिसमें प्राइमेट भी आते हैं। अगर पैर साथ-साथ नहीं खड़े, तो ज़मीन पर टिका पैर दूसरे पैर की हड्डियों की लंबाई तय करता है।"),\n    },\n}\nRIG_ARTICLE_V3_CATEGORY_BY_KEY: Dict[str, str] = {"horse": "equine", "deer": "equine", "turtle": "turtle", "humanoid": "biped"}\n\n\ndef _rig_article_v3_category(rig_key: str, lang: str) -> Tuple[str, str]:\n    """V3 rig category of a per-animal page: (short phrase, one sentence) in the page language."""\n    texts = RIG_ARTICLE_V3_CATEGORIES[RIG_ARTICLE_V3_CATEGORY_BY_KEY.get(rig_key, "quadruped")]\n    return texts.get(lang, texts["en"])\n\n\ndef _rig_article_text(rig_key: str, lang: str) -> Dict[str, Any]:\n'),
    ('category variables inside _rig_article_text',
     '    tip = info["tip"].get(lang, info["tip"]["en"])\n',
     '    tip = info["tip"].get(lang, info["tip"]["en"])\n    cat_short, cat_line = _rig_article_v3_category(rig_key, lang)\n'),
    ('description (en)',
     '        description = f"Online AI auto rigging for {label} 3D models. Upload GLB, FBX, or OBJ and export a rigged, animation-ready result for Blender, Unity, Unreal Engine, and web previews."',
     '        description = f"Auto rig a {label} 3D model online: rig in seconds, clips at once, a viewer with blueprint mode, GLB/FBX exports for Blender, Unity and Unreal."'),
    ('lead (en)',
     '        lead = f"AutoRig.online automatically builds rigs for {label} models: {shape}. This page targets creators who need a fast online workflow for a specific rig type, not only generic humanoid character rigging."',
     '        lead = f"AutoRig.online rigs {label} models on one conveyor: {shape}. {cat_line} The skeleton and the skin are built in seconds, animation clips are retargeted at once, and everything opens in one viewer."'),
    ('workflow (en)',
     '        workflow = f"Upload a GLB, FBX, or OBJ file, wait for the AI-assisted rigging task, open the task preview, and export the rigged result for Blender, Unity, Unreal Engine, or web viewers. The workflow is designed for {use}."',
     '        workflow = f"Drop a GLB, FBX, OBJ, a ZIP with textures or a picture on the upload zone. The fast rig builds the skeleton and the skin in seconds, and clips from the category library are retargeted at once. The viewer offers to confirm the skeleton, which is never required, textures a model that has none, and runs quality checks in the background. Then download GLB, FBX, USDZ, a ZIP, a Unity package or a Blender file for Blender, Unity, Unreal Engine or the web. The workflow is designed for {use}."'),
    ('faq answer 1 (en)',
     '            (f"Can I auto rig a {label} model online?", f"Yes. AutoRig.online accepts GLB, FBX, and OBJ uploads and runs an AI-assisted rigging workflow for {label} models when the geometry matches the supported rig type."),',
     '            (f"Can I auto rig a {label} model online?", f"Yes. Upload a GLB, FBX or OBJ, a ZIP with textures or a picture. The engine picks the rig category ({cat_short}), builds the skeleton and the skin in seconds and retargets animation clips at once."),'),
    ('faq answer 2 (en)',
     '            (f"What should I prepare before {label} auto rigging?", "Use a clean silhouette, separated limbs, and enough topology around bending areas so the generated rig and animation preview can deform cleanly."),',
     '            (f"What should I prepare before {label} auto rigging?", "Nothing special. The rig is fitted to the model as it is, in any pose. A clean silhouette, separated limbs and enough topology around the joints give the cleanest result, and the viewer offers to confirm the skeleton, which you can skip."),'),
    ('faq answer 3 (en)',
     '            ("Can I use the result in Blender, Unity, and Unreal Engine?", "Yes. After processing, open the preview, download the rigged files, and use them in common 3D and game development pipelines."),',
     '            ("Can I use the result in Blender, Unity, and Unreal Engine?", "Yes. Download GLB, FBX, USDZ, a ZIP, a Unity package or a Blender file from the viewer and use them in common 3D and game development pipelines."),'),
    ('description (ru)',
     '        description = f"Онлайн AI-риггинг для типа {label}: загрузите GLB, FBX или OBJ и получите rigged модель с animation-ready preview для Blender, Unity и Unreal Engine."',
     '        description = f"Онлайн авториг для типа «{label}»: риг за секунды, клипы сразу, вьювер с режимом чертежа, экспорт GLB/FBX для Blender, Unity и Unreal."'),
    ('lead (ru)',
     '        lead = f"AutoRig.online автоматически строит rig для {label}: {shape}. Страница сфокусирована на задачах, где пользователю нужно быстро получить готовый rig онлайн без ручной расстановки костей."',
     '        lead = f"AutoRig.online ригует модели «{label}» одним конвейером: {shape}. {cat_line} Скелет и скин строятся за секунды, клипы анимации сразу переносятся на модель, а всё открывается в одном вьювере."'),
    ('workflow (ru)',
     '        workflow = f"Загрузите GLB, FBX или OBJ, дождитесь обработки, откройте task preview и скачайте результат для Blender, Unity, Unreal Engine или web viewers. Такой workflow подходит для {use}."',
     '        workflow = f"Перетащите GLB, FBX, OBJ, ZIP с текстурами или картинку в зону загрузки. Быстрый риг строит скелет и скин за секунды, а клипы из библиотеки категории сразу переносятся на модель. Вьювер предлагает подтвердить скелет (это никогда не обязательно), создаёт текстуры для модели без них и запускает проверки качества в фоне. Затем скачайте GLB, FBX, USDZ, ZIP, пакет Unity или файл Blender для Blender, Unity, Unreal Engine или веба. Процесс рассчитан на {use}."'),
    ('faq answer 1 (ru)',
     '            (f"Можно ли сделать auto rig для {label} онлайн?", f"Да. AutoRig.online принимает GLB, FBX и OBJ модели и запускает AI-assisted rigging workflow для {label}, если геометрия подходит для выбранного типа."),',
     '            (f"Можно ли сделать auto rig для {label} онлайн?", f"Да. Загрузите GLB, FBX или OBJ, ZIP с текстурами или картинку. Движок сам выбирает категорию рига ({cat_short}), за секунды строит скелет и скин и сразу переносит клипы анимации."),'),
    ('faq answer 2 (ru)',
     '            (f"Какие файлы подходят для {label} rigging?", "Лучше всего подходят чистые GLB, FBX или OBJ модели с разделёнными конечностями, читаемым силуэтом и достаточной topology в местах сгиба."),',
     '            (f"Какие файлы подходят для {label} rigging?", "Ничего особенного. Риг подгоняется под модель как есть, в любой позе. Чистый силуэт, разделённые конечности и достаточная топология у суставов дают самый чистый результат, а вьювер предлагает подтвердить скелет, что можно пропустить."),'),
    ('faq answer 3 (ru)',
     '            (f"Можно ли использовать результат в Blender, Unity и Unreal Engine?", "Да. После обработки можно открыть preview, скачать rigged файлы и использовать их в Blender, Unity, Unreal Engine и других 3D pipelines."),',
     '            (f"Можно ли использовать результат в Blender, Unity и Unreal Engine?", "Да. Скачайте GLB, FBX, USDZ, ZIP, пакет Unity или файл Blender из вьювера и используйте их в обычных 3D-пайплайнах и пайплайнах игр."),'),
    ('description (zh)',
     '        description = f"面向 {label} 的在线 AI 自动绑定。上传 GLB、FBX 或 OBJ，生成适合 Blender、Unity、Unreal Engine 和网页预览的可动画 rigged 模型。"',
     '        description = f"在线为{label}自动绑定：几秒内完成绑定，动画片段即时生成，查看器带蓝图模式，可导出 GLB/FBX 用于 Blender、Unity 和 Unreal。"'),
    ('lead (zh)',
     '        lead = f"AutoRig.online 为 {label} 自动生成 rig：{shape}。此页面面向需要在线自动绑定特定动物或模型类型的创作者。"',
     '        lead = f"AutoRig.online 用一条流水线为 {label} 完成绑定：{shape}。{cat_line}骨架和蒙皮在几秒内生成，动画片段立即重定向，所有内容在同一个查看器中打开。"'),
    ('workflow (zh)',
     '        workflow = f"上传 GLB、FBX 或 OBJ，等待处理，打开 task preview，并导出到 Blender、Unity、Unreal Engine 或网页查看器。该流程适合 {use}。"',
     '        workflow = f"把 GLB、FBX、OBJ、带贴图的 ZIP 或一张图片放入上传区。快速绑定在几秒内生成骨架和蒙皮，类别动画库中的片段立即重定向。查看器会提示确认骨架（绝不强制），为没有贴图的模型补全贴图，并在后台运行质量检查。之后可下载 GLB、FBX、USDZ、ZIP、Unity 包或 Blender 文件，用于 Blender、Unity、Unreal Engine 或网页。该流程适合 {use}。"'),
    ('faq answer 1 (zh)',
     '            (f"可以在线自动绑定 {label} 吗？", f"可以。AutoRig.online 支持上传 GLB、FBX 或 OBJ，并为 {label} 运行 AI-assisted rigging workflow。"),',
     '            (f"可以在线自动绑定 {label} 吗？", f"可以。上传 GLB、FBX 或 OBJ、带贴图的 ZIP 或一张图片。引擎会选择绑定类别（{cat_short}），几秒内生成骨架和蒙皮，并立即重定向动画片段。"),'),
    ('faq answer 2 (zh)',
     '            (f"{label} 绑定前模型应该怎样准备？", "保持轮廓清晰、四肢分离，并在关节和弯曲区域保留足够拓扑。"),',
     '            (f"{label} 绑定前模型应该怎样准备？", "不需要特别准备。绑定会按模型当前的样子、在任意姿势下拟合。干净的轮廓、分离的四肢和关节周围足够的拓扑会得到最干净的结果，查看器会提示确认骨架，你也可以跳过。"),'),
    ('faq answer 3 (zh)',
     '            ("结果可以用于 Blender、Unity 和 Unreal Engine 吗？", "可以。处理完成后可打开 preview，下载 rigged 文件，并用于常见 3D 和游戏开发流程。"),',
     '            ("结果可以用于 Blender、Unity 和 Unreal Engine 吗？", "可以。从查看器下载 GLB、FBX、USDZ、ZIP、Unity 包或 Blender 文件，并用于常见的 3D 和游戏开发流程。"),'),
    ('description (hi)',
     '        description = f"{label} के लिए online AI auto rigging. GLB, FBX या OBJ upload करें और Blender, Unity, Unreal Engine तथा web previews के लिए rigged animation-ready model export करें."',
     '        description = f"{label} को online auto rig करें: सेकंडों में रिग, तुरंत क्लिप, ब्लूप्रिंट मोड वाला व्यूअर, Blender, Unity और Unreal के लिए GLB/FBX एक्सपोर्ट।"'),
    ('lead (hi)',
     '        lead = f"AutoRig.online {label} के लिए automatically rig बनाता है: {shape}. यह page उन creators के लिए है जिन्हें specific rig type के लिए online automatic rigging चाहिए."',
     '        lead = f"AutoRig.online {label} मॉडल को एक ही कन्वेयर पर रिग करता है: {shape}। {cat_line} कंकाल और स्किन सेकंडों में बनते हैं, एनिमेशन क्लिप तुरंत रीटारगेट होते हैं, और सब कुछ एक व्यूअर में खुलता है।"'),
    ('workflow (hi)',
     '        workflow = f"GLB, FBX या OBJ upload करें, processing complete होने दें, task preview खोलें, और Blender, Unity, Unreal Engine या web viewers के लिए result download करें. यह workflow {use} के लिए useful है."',
     '        workflow = f"GLB, FBX, OBJ, टेक्सचर वाली ZIP या एक तस्वीर अपलोड ज़ोन में डालें। तेज़ रिग सेकंडों में कंकाल और स्किन बनाता है, और श्रेणी की लाइब्रेरी के क्लिप तुरंत रीटारगेट होते हैं। व्यूअर कंकाल की पुष्टि का सुझाव देता है (यह कभी अनिवार्य नहीं), बिना टेक्सचर वाले मॉडल को टेक्सचर देता है और बैकग्राउंड में गुणवत्ता जाँच चलाता है। फिर Blender, Unity, Unreal Engine या वेब के लिए GLB, FBX, USDZ, ZIP, Unity पैकेज या Blender फ़ाइल डाउनलोड करें। यह प्रक्रिया {use} के लिए बनी है।"'),
    ('faq answer 1 (hi)',
     '            (f"क्या {label} को online auto rig किया जा सकता है?", f"हाँ. AutoRig.online GLB, FBX और OBJ uploads लेकर {label} के लिए AI-assisted rigging workflow चलाता है."),',
     '            (f"क्या {label} को online auto rig किया जा सकता है?", f"हाँ। GLB, FBX या OBJ, टेक्सचर वाली ZIP या एक तस्वीर अपलोड करें। इंजन रिग श्रेणी चुनता है ({cat_short}), सेकंडों में कंकाल और स्किन बनाता है और एनिमेशन क्लिप तुरंत रीटारगेट करता है।"),'),
    ('faq answer 2 (hi)',
     '            (f"{label} rigging के लिए model कैसे prepare करें?", "Clear silhouette, separated limbs और bending areas में enough topology रखें ताकि animation preview साफ रहे।"),',
     '            (f"{label} rigging के लिए model कैसे prepare करें?", "कुछ ख़ास नहीं। रिग मॉडल पर वैसे ही फ़िट होता है जैसा वह है, किसी भी पोज़ में। साफ़ सिल्हूट, अलग अंग और जोड़ों के आसपास पर्याप्त टोपोलॉजी सबसे साफ़ नतीजा देती है, और व्यूअर कंकाल की पुष्टि का सुझाव देता है, जिसे आप छोड़ सकते हैं।"),'),
    ('faq answer 3 (hi)',
     '            ("क्या result Blender, Unity और Unreal Engine में काम करेगा?", "हाँ. Processing के बाद preview खोलें, rigged files download करें, और उन्हें common 3D/game pipelines में use करें।"),',
     '            ("क्या result Blender, Unity और Unreal Engine में काम करेगा?", "हाँ। व्यूअर से GLB, FBX, USDZ, ZIP, Unity पैकेज या Blender फ़ाइल डाउनलोड करें और उन्हें सामान्य 3D और गेम डेवलपमेंट पाइपलाइन में इस्तेमाल करें।"),'),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("main_py")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-backup", action="store_true")
    args = ap.parse_args()
    raw = open(args.main_py, "rb").read().decode("utf-8")
    crlf = "\r\n" in raw
    text = raw.replace("\r\n", "\n")
    if MARKER in text:
        print("already patched, nothing to do")
        return 0
    for name, old, new in ANCHORS:
        n = text.count(old)
        if n != 1:
            print(f"FAIL anchor {name!r}: found {n} times, expected exactly 1")
            return 1
        text = text.replace(old, new)
    out = text.replace("\n", "\r\n") if crlf else text
    tmp = args.main_py + ".rigv3.tmp.py"
    with open(tmp, "wb") as f:
        f.write(out.encode("utf-8"))
    try:
        py_compile.compile(tmp, cfile=tmp + "c", doraise=True)
    except py_compile.PyCompileError as exc:
        os.remove(tmp)
        print("FAIL py_compile:", exc)
        return 1
    finally:
        if os.path.exists(tmp + "c"):
            os.remove(tmp + "c")
    if args.dry_run:
        os.remove(tmp)
        print(f"dry run ok: {len(ANCHORS)} anchors, result compiles")
        return 0
    if not args.no_backup:
        shutil.copy2(args.main_py, args.main_py + ".pre_rig_v3")
    os.replace(tmp, args.main_py)
    print(f"patched {args.main_py}: {len(ANCHORS)} anchors, py_compile ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
