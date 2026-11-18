"""Convert the application's restricted 2D DXF output to an AutoCAD 2000 DWG."""

import math
import os
from pathlib import Path
import re
import subprocess
import tempfile


MAX_DXF_BYTES = 2 * 1024 * 1024
MAX_DWG_BYTES = 8 * 1024 * 1024
PRLIMIT = Path("/usr/bin/prlimit")
SECTIONS = ("HEADER", "TABLES", "BLOCKS", "ENTITIES")
SAFE_NUMBER = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?\Z", re.ASCII)
LAYER_NAME = re.compile(r"[A-Za-z0-9_]{1,40}\Z")
ENTITY_TYPES = {
    "LINE": ("AcDbLine", {10, 20, 11, 21}, {30, 31}),
    "CIRCLE": ("AcDbCircle", {10, 20, 40}, {30}),
    "ARC": ("AcDbCircle", {10, 20, 40, 50, 51}, {30}),
    "TEXT": ("AcDbText", {10, 20, 40}, {11, 21, 30, 31, 41, 50, 51, 71, 72, 73}),
}


class DwgUnavailable(RuntimeError):
    """There is no DWG converter configured on this server."""


class DwgInputError(ValueError):
    """The request is outside the supported DXF subset."""


class DwgConversionError(RuntimeError):
    """The converter did not produce a usable DWG."""


def _tool_paths():
    return Path(os.environ.get("ABRIS_DXF2DWG", "")), Path(os.environ.get("ABRIS_DWG_WORKDIR", ""))


def is_available() -> bool:
    binary, workspace = _tool_paths()
    return (
        binary.is_absolute()
        and binary.is_file()
        and os.access(binary, os.X_OK)
        and os.access(PRLIMIT, os.X_OK)
        and workspace.is_absolute()
        and workspace.is_dir()
        and os.access(workspace, os.W_OK | os.X_OK)
    )


def _pairs(data):
    if not data or len(data) > MAX_DXF_BYTES:
        raise DwgInputError("Ожидается DXF размером до 2 МБ.")
    try:
        content = data.decode("utf-8-sig").replace("\r\n", "\n")
    except UnicodeDecodeError as error:
        raise DwgInputError("Файл DXF должен быть в кодировке UTF-8.") from error
    lines = content.removesuffix("\n").split("\n")
    if len(lines) % 2 or len(lines) > 160_000:
        raise DwgInputError("Некорректная структура DXF.")
    for index in range(0, len(lines), 2):
        tag = lines[index].strip(" \t")
        value = lines[index + 1]
        if not tag.isascii() or not tag.isdecimal() or len(tag) > 3:
            raise DwgInputError("Недопустимый код DXF.")
        if len(value) > 512 or (value and not value.isprintable()):
            raise DwgInputError("Некорректное значение в DXF.")
        code = int(tag)
        yield code, value if code == 1 else value.strip()


def _read_entities(data):
    pairs = iter(_pairs(data))
    seen = set()
    entities = []
    for pair in pairs:
        if pair == (0, "EOF") and next(pairs, None) is None and "HEADER" in seen and entities:
            return entities
        code, section = next(pairs, (None, None))
        if pair != (0, "SECTION") or code != 2 or section not in SECTIONS or section in seen:
            raise DwgInputError("Недопустимый раздел DXF.")
        seen.add(section)
        for code, value in pairs:
            if (code, value) == (0, "ENDSEC"):
                break
            if section != "ENTITIES":
                continue
            if code == 0 and value in ENTITY_TYPES:
                entities.append((value, {}))
            elif code == 0 or not entities:
                raise DwgInputError("DXF содержит неподдерживаемый объект.")
            elif code not in (5, 7, 100, 330):
                _read_entity_field(entities[-1], code, value)
    raise DwgInputError("В DXF не найден полный чертёж.")


def _read_entity_field(entity, code, value):
    kind, values = entity
    _, required, optional = ENTITY_TYPES[kind]
    if code in values:
        raise DwgInputError("DXF содержит повторяющийся атрибут.")
    if code == 8 and LAYER_NAME.fullmatch(value) or code == 1 and kind == "TEXT":
        values[code] = value
        return
    if code not in required and code not in optional and code != 67 or not SAFE_NUMBER.fullmatch(value):
        raise DwgInputError("Неподдерживаемый атрибут DXF.")
    numeric = float(value)
    if not math.isfinite(numeric) or abs(numeric) > 1e8 or code == 40 and numeric <= 0:
        raise DwgInputError("Число в DXF выходит за пределы допустимого диапазона.")
    if code == 67 and numeric not in (0, 1):
        raise DwgInputError("Некорректное пространство объекта DXF.")
    if code in (71, 72, 73) and not (numeric.is_integer() and 0 <= numeric <= 6):
        raise DwgInputError("Некорректное выравнивание текста DXF.")
    values[code] = int(numeric) if code in (67, 71, 72, 73) else numeric


def _section(name, rows):
    return [(0, "SECTION"), (2, name), *rows, (0, "ENDSEC")]


def _table(name, handle, records):
    rows = [(0, "TABLE"), (2, name), (5, handle), (330, "0"), (100, "AcDbSymbolTable"), (70, len(records))]
    if name == "DIMSTYLE":
        rows.append((100, "AcDbDimStyleTable"))
    for record in records:
        rows.extend(record)
    return [*rows, (0, "ENDTAB")]


def _record(kind, handle, owner, subclass, name, fields=()):
    return [(0, kind), (5, handle), (330, owner), (100, "AcDbSymbolTableRecord"),
            (100, subclass), (2, name), *fields]


def _entity_header(kind, handle, owner, layer, paper_space=False):
    rows = [(0, kind), (5, handle), (330, owner), (100, "AcDbEntity")]
    if paper_space:
        rows.append((67, 1))
    return [*rows, (8, layer)]


def _blocks():
    rows = []
    for name, owner, start, end in [("*Model_Space", "17", "18", "19"), ("*Paper_Space", "1B", "1C", "1D")]:
        rows.extend(_entity_header("BLOCK", start, owner, "0"))
        rows.extend([(100, "AcDbBlockBegin"), (2, name), (70, 0), (10, 0), (20, 0), (30, 0), (3, name), (1, "")])
        rows.extend(_entity_header("ENDBLK", end, owner, "0"))
        rows.append((100, "AcDbBlockEnd"))
    return rows


def _tables(layers):
    linetypes = [_record("LTYPE", f"{0x24 + index:X}", "2", "AcDbLinetypeTableRecord", name,
                        [(70, 0), (3, ""), (72, 65), (73, 0), (40, 0)])
                 for index, name in enumerate(("ByBlock", "ByLayer", "Continuous"))]
    layer_records = [_record("LAYER", f"{0x40 + index:X}", "1", "AcDbLayerTableRecord", name,
                             [(70, 0), (62, 7), (6, "Continuous"), (370, -3)])
                     for index, name in enumerate(sorted(layers))]
    style = _record("STYLE", "29", "5", "AcDbTextStyleTableRecord", "Standard",
                    [(70, 0), (40, 0), (41, 1), (50, 0), (71, 0), (42, 2.5), (3, "arial.ttf"), (4, "")])
    block_records = [_record("BLOCK_RECORD", "17", "9", "AcDbBlockTableRecord", "*Model_Space"),
                     _record("BLOCK_RECORD", "1B", "9", "AcDbBlockTableRecord", "*Paper_Space")]
    return [*_table("VPORT", "8", []), *_table("LTYPE", "2", linetypes), *_table("LAYER", "1", layer_records),
            *_table("STYLE", "5", [style]), *_table("VIEW", "7", []), *_table("UCS", "6", []),
            *_table("APPID", "3", []), *_table("DIMSTYLE", "4", []), *_table("BLOCK_RECORD", "9", block_records)]


def _native_dxf(entities):
    layers = {"0", *(values.get(8, "0") for _, values in entities)}
    if len(layers) > 64:
        raise DwgInputError("Чертёж содержит слишком много слоёв.")
    geometry = []
    for index, (kind, values) in enumerate(entities):
        subclass, required, _ = ENTITY_TYPES[kind]
        if not required <= values.keys() or kind == "TEXT" and 1 not in values:
            raise DwgInputError("В DXF отсутствуют обязательные атрибуты объекта.")
        layer = values.get(8, "0")
        paper_space = bool(values.get(67, 0))
        if paper_space and layer != "LEGEND":
            raise DwgInputError("Пространство листа предусмотрено для легенды.")
        owner = "1B" if paper_space else "17"
        geometry.extend(_entity_header(kind, f"{0x100 + index:X}", owner, layer, paper_space))
        geometry.append((100, subclass))
        separate = {8, 67, 73, 50, 51} if kind == "ARC" else {8, 67, 73}
        geometry.extend((code, value) for code, value in values.items() if code not in separate)
        if kind == "ARC":
            geometry.extend([(100, "AcDbArc"), (50, values[50]), (51, values[51])])
        if kind == "TEXT":
            geometry.extend([(7, "Standard"), (100, "AcDbText"), (73, values.get(73, 0))])
    header = [(9, "$ACADVER"), (1, "AC1015"), (9, "$DWGCODEPAGE"), (3, "ANSI_1251"), (9, "$INSUNITS"), (70, 4)]
    classes = [(0, "CLASS"), (1, "LAYOUT"), (2, "AcDbLayout"), (3, "ObjectDBX Classes"), (90, 0), (280, 0), (281, 0)]
    content = [*_section("HEADER", header), *_section("CLASSES", classes), *_section("TABLES", _tables(layers)),
               *_section("BLOCKS", _blocks()), *_section("ENTITIES", geometry), (0, "EOF")]
    encoded = "".join(f"{code}\n{value}\n" for code, value in content).encode("cp1251", "backslashreplace")
    return re.sub(rb"\\(?:u([0-9a-f]{4})|x([0-9a-f]{2}))",
                  lambda match: b"\\U+" + (match[1] or (b"00" + match[2])).upper(), encoded)


def convert_dxf(data: bytes) -> bytes:
    if not is_available():
        raise DwgUnavailable("Конвертер DWG недоступен.")
    normalized = _native_dxf(_read_entities(data))
    binary, workspace = _tool_paths()
    try:
        with tempfile.TemporaryDirectory(prefix="dwg-", dir=workspace) as scratch:
            folder = Path(scratch)
            (folder / "plan.dxf").write_bytes(normalized)
            command = [
                str(PRLIMIT), "--as=268435456", "--cpu=6", f"--fsize={MAX_DWG_BYTES}", "--core=0",
                "--", str(binary), "--as", "r2000", "-o", "plan.dwg", "plan.dxf",
            ]
            completed = subprocess.run(
                command, cwd=folder, env={"LANG": "C.UTF-8", "TMPDIR": scratch},
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=8, check=False,
            )
            output = folder / "plan.dwg"
            if completed.returncode == 0 and output.is_file() and 512 < output.stat().st_size <= MAX_DWG_BYTES:
                result = output.read_bytes()
                if result.startswith(b"AC1015"):
                    return result
    except (OSError, subprocess.TimeoutExpired) as error:
        raise DwgConversionError("Не удалось сформировать DWG.") from error
    raise DwgConversionError("Не удалось сформировать DWG.")
