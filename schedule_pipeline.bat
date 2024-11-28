@echo off
cd /d "%~dp0"
python setup_pipeline.py
python start_pipeline.py
