#!/usr/bin/env python3
"""One-off generator: build face-rig-animation-{ru,zh,hi}.html from English source."""
from pathlib import Path

SRC = Path("/root/autorig-online/static/face-rig-animation.html")

RU = {
    "lang": "ru",
    "slug": "face-rig-animation-ru",
    "title": "Face rig и анимация лица — мягкие маски, лендмарки и липсинк | AutoRig Online",
    "desc": "Как AutoRig по фронтальному изображению головы строит мягкие маски лица, 2D-лендмарки и оценку зон для игровой мимики и липсинка. Обзор пайплайна и живое демо analyze-head (тот же API, что в продакшене).",
    "kw": "face rig, риг лица, мимика, лендмарки лица, липсинк, блендшейпы, морфы, риг персонажа, анализ головы, Unity мимика, Unreal мимика, AutoRig",
    "og_title": "Face rig и анимация лица — маски и лендмарки | AutoRig Online",
    "og_desc": "Интерактивное руководство: анализ головы, мягкие маски, лендмарки и оценка зон. Живое демо вызывает API face-rig воркера.",
    "tw_title": "Face rig и анимация лица | AutoRig Online",
    "tw_desc": "Мягкие маски, лендмарки и демо analyze-head — тот же пайплайн, что в AutoRig.",
    "og_image_alt": "Пример фронтального рендера головы для анализа масок",
    "json_inlang": "ru-RU",
    "json_webname": "Face rig и анимация — мягкие маски, лендмарки и липсинк",
    "json_webdesc": "Как AutoRig по фронтальному рендеру головы строит мягкие маски и лендмарки для мимики и липсинка, с живым демо API.",
    "bc_home": "Главная",
    "bc_guides": "Руководства",
    "bc_here": "Face rig и анимация",
    "art_head": "Face rig и анимация лица — мягкие маски, лендмарки и липсинк",
    "art_desc": "Руководство по анализу лица в AutoRig: RGB, синтетическая глубина/альфа, уверенность по зонам, 2D-лендмарки и мягкие маски для мимики.",
    "art_keywords": "face rig, мимика, маски лица, лендмарки, липсинк, риг персонажа, анализ головы",
    "hero_h1": "Face rig и анимация",
    "hero_sub": "Фронтальный рендер головы → мягкие маски лица, 2D-лендмарки и оценка зон для липсинка и выражений в пайплайне AutoRig.",
    "chip1": "Анализ головы",
    "chip2": "Мягкие маски",
    "chip3": "Лендмарки",
    "cta": "Открыть интерактивное демо",
    "mini": "Тот же вызов analyze-head, что и в тестере воркера.",
    "in1t": "Пайплайн",
    "in1p": 'Сервис принимает RGB и синтетическую глубину (по яркости) и альфу, как в <a href="https://worker-0001.free3d.online/test/test.html" rel="noopener noreferrer">pipeline tester</a>. На выходе: уверенность по зонам, лендмарки, контуры и <strong>мягкие маски</strong> для глаз, рта, бровей и т.д.',
    "in2t": "Зачем маски",
    "in2p": "Маски задают, где на меше допустимы деформация и речевые движения. Чистое разделение зон помогает не смешивать речь, моргание и эмоции.",
    "in3t": "В AutoRig",
    "in3p": "После рига тела воркер может проанализировать голову — без ручной раскраски весов по зонам. Демо вызывает <code>POST …/api/face-rig/analyze-head</code> на боевом воркере.",
    "demo_h": "Интерактивное демо",
    "demo_p": "Пример запускается при загрузке. Замените изображение и нажмите <strong>Запустить анализ</strong>, чтобы обновить маски.",
    "drop": "Перетащите или нажмите, чтобы выбрать изображение",
    "run": "Запустить анализ",
    "next_h": "Дальше",
    "next_p": "Сюда можно добавить заметки под движок (Unity blendshapes, Unreal), советы по съёмке фронтала и разбор низкой уверенности. Лучше всего ровный свет и центрированное лицо.",
    "next_a": "Полный пайплайн auto-rig для персонажа →",
    "rel_h": "Связанные гайды",
    "rel1": "Ретаргетинг анимаций",
    "rel2": "Как это работает",
    "rel3": "Все руководства",
    "bc_a1": 'href="/">Главная',
    "bc_a2": 'href="/guides">Руководства',
    "i18n_js": """<script>
window.__FR_I18N = {
  running: 'Выполняется анализ…',
  softMasks: 'Мягкие маски',
  regionConfidence: 'Уверенность по зонам',
  landmarks: 'Лендмарки',
  analysis: 'Анализ',
  sampleLabel: 'Пример: фронтальный рендер головы',
  resultsIdle: 'Результаты появятся после первого запуска.'
};
</script>
""",
}

ZH = {
    "lang": "zh",
    "slug": "face-rig-animation-zh",
    "title": "面部绑定与动画指南 — 软蒙版、关键点与口型同步 | AutoRig Online",
    "desc": "AutoRig 如何将正面头部图像转为面部软蒙版、2D 关键点与分区置信度，用于游戏面部动画。流程说明与线上 analyze-head 演示（与生产环境相同 API）。",
    "kw": "面部绑定, 面部动画, 面部蒙版, 面部关键点, 口型同步, 混合形状, 角色绑定, 头部分析, Unity 面部, Unreal 面部, AutoRig",
    "og_title": "面部绑定与动画 — 软蒙版与关键点 | AutoRig Online",
    "og_desc": "交互式指南：正面头部分析、软蒙版、关键点与分区评分。在线演示调用生产环境 face-rig API。",
    "tw_title": "面部绑定与动画指南 | AutoRig Online",
    "tw_desc": "面部软蒙版、关键点与 analyze-head 在线演示 — 与 AutoRig 生产管线一致。",
    "og_image_alt": "用于面部分析蒙版演示的正面头部渲染示例",
    "json_inlang": "zh-CN",
    "json_webname": "面部绑定与动画 — 软蒙版、关键点与口型同步",
    "json_webdesc": "AutoRig 如何将正面头部渲染解析为面部软蒙版与关键点，用于动画与口型同步，并提供在线 API 演示。",
    "bc_home": "首页",
    "bc_guides": "指南",
    "bc_here": "面部绑定与动画",
    "art_head": "面部绑定与动画 — 软蒙版、关键点与口型同步",
    "art_desc": "AutoRig 面部分析指南：RGB、合成深度/Alpha、分区置信度、2D 关键点与用于面部动画的软蒙版。",
    "art_keywords": "面部绑定, 面部动画, 蒙版, 关键点, 口型同步, 角色绑定, 头部分析",
    "hero_h1": "面部绑定与动画",
    "hero_sub": "正面头部渲染 → 面部软蒙版、2D 关键点与分区评分，用于 AutoRig 管线中的口型与表情。",
    "chip1": "头部分析",
    "chip2": "软蒙版",
    "chip3": "关键点",
    "cta": "打开交互演示",
    "mini": "与 worker 测试页相同的 analyze-head 调用。",
    "in1t": "流程",
    "in1p": '服务接收 RGB 与按亮度合成的深度及 Alpha，与 <a href="https://worker-0001.free3d.online/test/test.html" rel="noopener noreferrer">pipeline tester</a> 一致。输出：分区置信度、关键点、轮廓以及眼、口、眉等 <strong>软蒙版</strong>。',
    "in2t": "为何需要蒙版",
    "in2p": "蒙版限定网格变形与语音运动的范围。清晰分区让说话、眨眼与情绪层在运行时互不干扰。",
    "in3t": "在 AutoRig 中",
    "in3p": "身体绑定后，worker 可分析头部，省去逐区手绘权重。演示在正式 worker 上调用 <code>POST …/api/face-rig/analyze-head</code>。",
    "demo_h": "交互演示",
    "demo_p": "页面加载后会自动运行示例图。更换图片后点击 <strong>运行分析</strong> 以刷新蒙版。",
    "drop": "拖放或点击选择图片",
    "run": "运行分析",
    "next_h": "下一步",
    "next_p": "可在此补充引擎相关说明（Unity blendshape、Unreal）、正面布光与低置信度排查。正面、光线均匀效果最好。",
    "next_a": "为角色运行完整 auto-rig 流程 →",
    "rel_h": "相关指南",
    "rel1": "动画重定向",
    "rel2": "工作原理",
    "rel3": "全部指南",
    "bc_a1": 'href="/">首页',
    "bc_a2": 'href="/guides">指南',
    "i18n_js": """<script>
window.__FR_I18N = {
  running: '正在分析…',
  softMasks: '软蒙版',
  regionConfidence: '分区置信度',
  landmarks: '关键点',
  analysis: '分析',
  sampleLabel: '示例：捆绑的正面头部渲染',
  resultsIdle: '首次运行后结果将显示在此处。'
};
</script>
""",
}

HI = {
    "lang": "hi",
    "slug": "face-rig-animation-hi",
    "title": "फेस रिग और एनीमेशन गाइड — सॉफ्ट मास्क, लैंडमार्क और लिप सिंक | AutoRig Online",
    "desc": "AutoRig सामने वाले सिर की इमेज से फेस के सॉफ्ट मास्क, 2D लैंडमार्क और ज़ोन कॉन्फिडेंस कैसे बनाता है—गेम फेशियल एनीमेशन के लिए। पाइपलाइन सारांश और लाइव analyze-head डेमो (प्रोडक्शन जैसा API)।",
    "kw": "face rig, चेहरे की एनीमेशन, फेस मास्क, लैंडमार्क, लिप सिंक, ब्लेंडशेप, कैरेक्टर रिगिंग, हेड एनालिसिस, Unity, Unreal, AutoRig",
    "og_title": "फेस रिग और एनीमेशन — मास्क और लैंडमार्क | AutoRig Online",
    "og_desc": "इंटरैक्टिव गाइड: फ्रंटल हेड एनालिसिस, सॉफ्ट मास्क, लैंडमार्क और ज़ोन स्कोर। लाइव डेमो प्रोडक्शन face-rig API कॉल करता है।",
    "tw_title": "फेस रिग और एनीमेशन गाइड | AutoRig Online",
    "tw_desc": "सॉफ्ट फेशियल मास्क, लैंडमार्क और लाइव analyze-head डेमो — AutoRig प्रोडक्शन जैसा पाइपलाइन।",
    "og_image_alt": "फेस रिग मास्क एनालिसिस के लिए फ्रंटल 3D हेड रेंडर नमूना",
    "json_inlang": "hi-IN",
    "json_webname": "फेस रिग और एनीमेशन — सॉफ्ट मास्क, लैंडमार्क और लिप सिंक",
    "json_webdesc": "AutoRig फ्रंटल हेड रेंडर को सॉफ्ट फेशियल मास्क और लैंडमार्क में कैसे बदलता है—एनीमेशन और लिप सिंक के लिए, लाइव API डेमो सहित।",
    "bc_home": "होम",
    "bc_guides": "गाइड",
    "bc_here": "फेस रिग और एनीमेशन",
    "art_head": "फेस रिग और एनीमेशन — सॉफ्ट मास्क, लैंडमार्क और लिप सिंक",
    "art_desc": "AutoRig फेस एनालिसिस: RGB, सिंथेटिक डेप्थ/अल्फा, ज़ोन कॉन्फिडेंस, 2D लैंडमार्क और फेशियल एनीमेशन के लिए सॉफ्ट मास्क।",
    "art_keywords": "face rig, फेशियल एनीमेशन, फेस मास्क, लैंडमार्क, लिप सिंक, कैरेक्टर रिगिंग, हेड एनालिसिस",
    "hero_h1": "फेस रिग और एनीमेशन",
    "hero_sub": "फ्रंटल हेड रेंडर → सॉफ्ट फेशियल मास्क, 2D लैंडमार्क और ज़ोन स्कोर—AutoRig पाइपलाइन में लिप सिंक और एक्सप्रेशन के लिए।",
    "chip1": "हेड एनालिसिस",
    "chip2": "सॉफ्ट मास्क",
    "chip3": "लैंडमार्क",
    "cta": "इंटरैक्टिव डेमो खोलें",
    "mini": "वही analyze-head कॉल जो वर्कर टेस्टर में है।",
    "in1t": "पाइपलाइन",
    "in1p": 'सेवा RGB और सिंथेटिक डेप्थ (ल्युमिनेंस) व अल्फा लेती है, <a href="https://worker-0001.free3d.online/test/test.html" rel="noopener noreferrer">pipeline tester</a> जैसा। आउटपुट: ज़ोन कॉन्फिडेंस, लैंडमार्क, कंटूर और आँख, मुँह, भौंह आदि के लिए <strong>सॉफ्ट मास्क</strong>।',
    "in2t": "मास्क क्यों",
    "in2p": "मास्क बताते हैं कि मेश पर कहाँ डिफॉर्मेशन और स्पीच मोशन मान्य है। साफ़ ज़ोन अलग रखने से बोलना, पलक झपकाना और इमोशन रनटाइम पर टकराते नहीं।",
    "in3t": "AutoRig में",
    "in3p": "बॉडी रिग के बाद वर्कर हेड एनालिसिस चला सकता है—हर ज़ोन पर हाथ से वेट पेंट किए बिना। डेमो लाइव वर्कर पर <code>POST …/api/face-rig/analyze-head</code> कॉल करता है।",
    "demo_h": "इंटरैक्टिव डेमो",
    "demo_p": "लोड पर सैंपल इमेज चलती है। इमेज बदलें, फिर मास्क रिफ्रेश करने के लिए <strong>एनालिसिस चलाएँ</strong>।",
    "drop": "ड्रॉप करें या चुनने के लिए क्लिक करें",
    "run": "एनालिसिस चलाएँ",
    "next_h": "अगले कदम",
    "next_p": "इंजन-विशिष्ट नोट्स (Unity/Unreal), फ्रंट लाइटिंग टिप्स, या कम कॉन्फिडेंस ट्रबलशूटिंग जोड़ें। अच्छी रोशनी और केंद्रित चेहरा सबसे अच्छा काम करता है।",
    "next_a": "पूरे कैरेक्टर पर पूरा auto-rig पाइपलाइन चलाएँ →",
    "rel_h": "संबंधित गाइड",
    "rel1": "एनिमेशन रिटारगेटिंग",
    "rel2": "यह कैसे काम करता है",
    "rel3": "सभी गाइड",
    "bc_a1": 'href="/">होम',
    "bc_a2": 'href="/guides">गाइड',
    "i18n_js": """<script>
window.__FR_I18N = {
  running: 'विश्लेषण चल रहा है…',
  softMasks: 'सॉफ्ट मास्क',
  regionConfidence: 'क्षेत्र विश्वास स्तर',
  landmarks: 'लैंडमार्क',
  analysis: 'विश्लेषण',
  sampleLabel: 'नमूना: फ्रंटल हेड रेंडर',
  resultsIdle: 'पहली रन के बाद परिणाम यहाँ दिखेंगे।'
};
</script>
""",
}


def apply_locale(html: str, L: dict, slug: str) -> str:
    base = "https://autorig.online"
    url = f"{base}/{slug}"
    h = html
    h = h.replace('<html lang="en">', f'<html lang="{L["lang"]}">', 1)
    # title: replace first line between <title> and </title>
    import re

    h = re.sub(r"<title>.*?</title>", f"<title>{L['title']}</title>", h, count=1, flags=re.DOTALL)
    h = re.sub(
        r'<meta name="description" content="[^"]*"',
        f'<meta name="description" content="{L["desc"].replace(chr(34), "&quot;")}"',
        h,
        count=1,
    )
    h = re.sub(
        r'<meta name="keywords" content="[^"]*"',
        f'<meta name="keywords" content="{L["kw"].replace(chr(34), "&quot;")}"',
        h,
        count=1,
    )
    h = h.replace(
        f'<link rel="canonical" href="{base}/face-rig-animation">',
        f'<link rel="canonical" href="{url}">',
        1,
    )
    h = h.replace(
        f'<meta property="og:locale" content="en_US">',
        f'<meta property="og:locale" content="{"zh_CN" if L["lang"] == "zh" else ("ru_RU" if L["lang"] == "ru" else "hi_IN")}">',
        1,
    )
    h = re.sub(
        r'<meta property="og:title" content="[^"]*"',
        f'<meta property="og:title" content="{L["og_title"].replace(chr(34), "&quot;")}"',
        h,
        count=1,
    )
    h = re.sub(
        r'<meta property="og:description" content="[^"]*"',
        f'<meta property="og:description" content="{L["og_desc"].replace(chr(34), "&quot;")}"',
        h,
        count=1,
    )
    h = h.replace(
        f'<meta property="og:url" content="{base}/face-rig-animation">',
        f'<meta property="og:url" content="{url}">',
        1,
    )
    h = re.sub(
        r'<meta property="og:image:alt" content="[^"]*"',
        f'<meta property="og:image:alt" content="{L["og_image_alt"].replace(chr(34), "&quot;")}"',
        h,
        count=1,
    )
    h = re.sub(
        r'<meta name="twitter:title" content="[^"]*"',
        f'<meta name="twitter:title" content="{L["tw_title"].replace(chr(34), "&quot;")}"',
        h,
        count=1,
    )
    h = re.sub(
        r'<meta name="twitter:description" content="[^"]*"',
        f'<meta name="twitter:description" content="{L["tw_desc"].replace(chr(34), "&quot;")}"',
        h,
        count=1,
    )

    # JSON-LD: replace WebPage block fields (simplified string replace)
    h = h.replace(f'"@id": "{base}/face-rig-animation#webpage"', f'"@id": "{url}#webpage"')
    h = h.replace(f'"url": "{base}/face-rig-animation"', f'"url": "{url}"')
    h = h.replace('"inLanguage": "en-US"', f'"inLanguage": "{L["json_inlang"]}"')
    h = h.replace(
        '"name": "Face Rig & Facial Animation Guide — Soft Masks, Landmarks & Lip Sync"',
        f'"name": "{L["json_webname"]}"',
    )
    h = h.replace(
        '"description": "How AutoRig analyzes a frontal head render into soft facial masks and landmarks for animation and lip sync, with a live API demo."',
        f'"description": "{L["json_webdesc"]}"',
    )
    h = h.replace('"name": "Home"', f'"name": "{L["bc_home"]}"')
    h = h.replace('"name": "Guides"', f'"name": "{L["bc_guides"]}"')
    h = h.replace('"name": "Face Rig & Animation"', f'"name": "{L["bc_here"]}"')
    h = h.replace(
        f'"item": "{base}/face-rig-animation"',
        f'"item": "{url}"',
    )
    h = h.replace(
        '"headline": "Face Rig & Facial Animation — Soft Masks, Landmarks & Lip Sync"',
        f'"headline": "{L["art_head"]}"',
    )
    h = h.replace(
        '"description": "Guide to AutoRig face analysis: RGB + synthetic depth/alpha, region confidence, 2D landmarks, and soft masks for facial animation."',
        f'"description": "{L["art_desc"]}"',
    )
    h = h.replace(
        '"keywords": ["face rig", "facial animation", "face masks", "landmarks", "lip sync", "character rigging", "3D head analysis"]',
        f'"keywords": [{", ".join(chr(34) + k + chr(34) for k in [x.strip() for x in L["art_keywords"].split(",")])}]',
    )
    h = h.replace(
        '"mainEntityOfPage": { "@id": "https://autorig.online/face-rig-animation#webpage" }',
        f'"mainEntityOfPage": {{ "@id": "{url}#webpage" }}',
    )

    # Body
    h = h.replace("<h1>Face Rig &amp; Animation</h1>", f"<h1>{L['hero_h1']}</h1>")
    h = h.replace(
        "<p class=\"hero-subtitle\">Frontal head render → soft facial masks, 2D landmarks, and region scores for lip sync and expression in the AutoRig pipeline.</p>",
        f'<p class="hero-subtitle">{L["hero_sub"]}</p>',
    )
    h = h.replace(
        '<span class="fr-chip"><img src="/static/images/icons/ai.svg" alt=""> Head analysis</span>',
        f'<span class="fr-chip"><img src="/static/images/icons/ai.svg" alt=""> {L["chip1"]}</span>',
    )
    h = h.replace(
        '<span class="fr-chip"><img src="/static/images/icons/animations.svg" alt=""> Soft masks</span>',
        f'<span class="fr-chip"><img src="/static/images/icons/animations.svg" alt=""> {L["chip2"]}</span>',
    )
    h = h.replace(
        '<span class="fr-chip"><img src="/static/images/icons/target.svg" alt=""> Landmarks</span>',
        f'<span class="fr-chip"><img src="/static/images/icons/target.svg" alt=""> {L["chip3"]}</span>',
    )
    h = h.replace('<a href="#live-demo" class="fr-cta">Open interactive demo</a>', f'<a href="#live-demo" class="fr-cta">{L["cta"]}</a>')
    h = h.replace(
        "<p class=\"fr-mini\">Runs the same <code>analyze-head</code> call as the worker tester.</p>",
        f'<p class="fr-mini">{L["mini"].replace("analyze-head", "<code>analyze-head</code>")}</p>',
    )
    # fix mini - if already has code tags in L - for RU I used plain text - use replace that doesn't double code
    if "<code>" not in L["mini"]:
        h = h.replace(
            f'<p class="fr-mini">{L["mini"].replace("analyze-head", "<code>analyze-head</code>")}</p>',
            f'<p class="fr-mini">Runs the same <code>analyze-head</code> call as the worker tester.</p>'.replace(
                "Runs the same <code>analyze-head</code> call as the worker tester.", L["mini"] if "analyze-head" not in L["mini"] else L["mini"]
            ),
        )

    # Simpler mini line - manual per locale in dict with <code> in string
    # Fix botched replace - read file after generation

    h = h.replace("<h2>Pipeline</h2>", f"<h2>{L['in1t']}</h2>")
    h = h.replace(
        """                    <p>
                        Service ingests RGB plus synthetic depth (luminance) and alpha, like the
                        <a href="https://worker-0001.free3d.online/test/test.html" rel="noopener noreferrer">pipeline tester</a>.
                        Output: region confidence, landmarks, contours, and <strong>soft masks</strong> for eyes, mouth, brows, etc.
                    </p>""",
        f"                    <p>\n                        {L['in1p']}\n                    </p>",
    )
    h = h.replace("<h2>Why masks</h2>", f"<h2>{L['in2t']}</h2>")
    h = h.replace(
        """                    <p>
                        Masks delimit where mesh deformation and speech motion apply. Clean separation keeps
                        talking, blinking, and emotion layers from fighting each other at runtime.
                    </p>""",
        f"                    <p>\n                        {L['in2p']}\n                    </p>",
    )
    h = h.replace("<h2>In AutoRig</h2>", f"<h2>{L['in3t']}</h2>")
    h = h.replace(
        """                    <p>
                        After body rigging, the worker can analyze the head so you skip hand-painted weights per zone.
                        Demo uses <code>POST …/api/face-rig/analyze-head</code> on the live worker.
                    </p>""",
        f"                    <p>\n                        {L['in3p']}\n                    </p>",
    )

    h = h.replace("<h2>Interactive demo</h2>", f"<h2>{L['demo_h']}</h2>")
    h = h.replace(
        "<p>Sample image runs on load. Replace the image, then <strong>Run analysis</strong> to refresh masks.</p>",
        f"<p>{L['demo_p']}</p>",
    )
    h = h.replace('<p class="fr-drop-hint">Drop or click to choose image</p>', f'<p class="fr-drop-hint">{L["drop"]}</p>')
    h = h.replace(
        '<button type="button" class="btn btn-primary fr-run" id="fr-run">Run analysis</button>',
        f'<button type="button" class="btn btn-primary fr-run" id="fr-run">{L["run"]}</button>',
    )

    h = h.replace("<h2>Next steps</h2>", f"<h2>{L['next_h']}</h2>")
    h = h.replace(
        """                <p>
                    Add engine-specific notes (Unity blendshapes, Unreal poses), capture tips for stable front lighting,
                    or troubleshooting when confidence is low. Well-lit, centered faces work best.
                </p>""",
        f"                <p>\n                    {L['next_p']}\n                </p>",
    )
    h = h.replace(
        '<p style="margin-bottom: 0;"><a href="/"><strong>Run the full auto-rig pipeline on a character →</strong></a></p>',
        f'<p style="margin-bottom: 0;"><a href="/"><strong>{L["next_a"]}</strong></a></p>',
    )

    h = h.replace("<h2>Related guides</h2>", f"<h2>{L['rel_h']}</h2>")
    h = h.replace(
        """                <a href="/animation-retargeting" class="related-link">Animation Retargeting</a>
                <a href="/how-it-works" class="related-link">How It Works</a>
                <a href="/guides" class="related-link">All guides</a>""",
        f"""                <a href="/animation-retargeting-{L['lang'] if L['lang'] != 'en' else ''}" class="related-link">{L['rel1']}</a>
                <a href="/how-it-works" class="related-link">{L['rel2']}</a>
                <a href="/guides" class="related-link">{L['rel3']}</a>""".replace(
            "/animation-retargeting-", "/animation-retargeting"
        ),
    )

    # Fix related links: for en animation-retargeting; for ru animation-retargeting-ru
    lang_suffix = "" if L["lang"] == "en" else f"-{L['lang']}"
    rel_ar = f"/animation-retargeting{lang_suffix}"
    h = re.sub(
        r'<a href="/animation-retargeting[^"]*" class="related-link">[^<]*</a>',
        f'<a href="{rel_ar}" class="related-link">{L["rel1"]}</a>',
        h,
        count=1,
    )

    # Breadcrumb
    h = h.replace(
        """                        <ol>
                            <li><a href="/">Home</a></li>
                            <li><a href="/guides">Guides</a></li>
                            <li aria-current="page">Face Rig &amp; Animation</li>
                        </ol>""",
        f"""                        <ol>
                            <li><a href="/">{L['bc_home']}</a></li>
                            <li><a href="/guides">{L['bc_guides']}</a></li>
                            <li aria-current="page">{L['bc_here']}</li>
                        </ol>""",
    )

    # Footer keeps href="/face-rig-animation" so i18n.updateGuideLinks() can rewrite by language.

    # Insert __FR_I18N before main script
    h = h.replace(
        "    <script src=\"/static/js/i18n.js\"></script>",
        L["i18n_js"] + '    <script src="/static/js/i18n.js"></script>',
    )

    return h


def main():
    raw = SRC.read_text(encoding="utf-8")
    for name, loc in [("ru", RU), ("zh", ZH), ("hi", HI)]:
        out = apply_locale(raw, loc, loc["slug"])
        # Fix ru mini paragraph - worker tester sentence
        if name == "ru":
            out = out.replace(
                "<p class=\"fr-mini\">Тот же вызов analyze-head, что и в тестере воркера.</p>",
                '<p class="fr-mini">Тот же вызов <code>analyze-head</code>, что и в тестере воркера.</p>',
            )
        Path(f"/root/autorig-online/static/face-rig-animation-{name}.html").write_text(
            out, encoding="utf-8"
        )
        print("Wrote", name)


if __name__ == "__main__":
    main()
