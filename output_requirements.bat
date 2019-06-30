@echo off
:: 現在のディレクトリに移動
cd /d %~dp0
:: virtualenv を起動
call Scripts\activate
:: requirements.txt
echo "pip freeze > requirements.txt"
:: 待機へ
cmd /k
:: virtualenv を終了
:: call Scripts\deactivate.bat