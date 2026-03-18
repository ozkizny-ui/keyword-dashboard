@echo off
REM ═══════════════════════════════════════════════
REM  오즈키즈 키워드 주간 수집 - Windows 작업 스케줄러용
REM  
REM  설정 방법:
REM  1. 이 파일을 프로젝트 폴더에 저장
REM  2. Windows 작업 스케줄러 열기 (taskschd.msc)
REM  3. '기본 작업 만들기' 클릭
REM  4. 트리거: 매주 월요일 06:00
REM  5. 동작: '프로그램 시작' → 이 .bat 파일 경로 지정
REM ═══════════════════════════════════════════════

cd /d "%~dp0"
call .venv\Scripts\activate 2>nul || echo [WARN] venv not found, using system Python
python fetch_weekly_data.py

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Data collection failed with code %ERRORLEVEL%
    pause
)
