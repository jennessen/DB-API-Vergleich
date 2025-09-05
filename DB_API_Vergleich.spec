# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all

datas = []
binaries = []
hiddenimports = []
# include tkcalendar resources if used
try:
    tmp_ret = collect_all('tkcalendar')
    datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
except Exception:
    pass

# include config.json so the packaged app can read it next to the exe
import os
config_path = os.path.join(os.getcwd(), 'config.json')
if os.path.exists(config_path):
    # ('source_path', 'relative_target_dir_inside_app')
    datas.append((config_path, '.'))
# include test.js so the packaged app can read it next to the exe
test_path = os.path.join(os.getcwd(), 'test.js')
if os.path.exists(test_path):
    # ('source_path', 'relative_target_dir_inside_app')
    datas.append((test_path, '.'))


a = Analysis(
    ['ui_single.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='DB_API_Vergleich',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
