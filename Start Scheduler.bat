@echo off
cd /d "C:\Users\edelt\OneDrive\Escritorio\LACB_Scheduler"

REM Start Streamlit V9 in a new window
start "" cmd /k python -m streamlit run app_v9.py --server.port 8501

REM Wait 3 seconds, then open the app in your default browser
timeout /t 3 >nul
start http://localhost:8501
