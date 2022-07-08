if (Test-Path '.\virtual_env') { 
    Remove-Item '.\virtual_env' -r
    Write-Output 'Removed Existing Virtual Environment'
}
Write-Output 'Preparing Set-Up'
python -m venv '.\virtual_env'
'.\virtual_env\Scripts\Activate.ps1'
pip install -r 'requirements.txt'

Write-Output 'Running Main File'
python main.py