import subprocess
import sys

scripts = ["CapturaAntena.py", "CapturaFirewall.py"]
processos = [subprocess.Popen([sys.executable, s]) for s in scripts]

try:
    for p in processos:
        p.wait()
except KeyboardInterrupt:
    for p in processos:
        p.terminate()
    print("Capturas encerradas.")