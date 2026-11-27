import zipfile
import shutil
from pathlib import Path

def extrair_zips(pasta_zip: str, pasta_csv: str) -> list[str]:
    destino = Path(pasta_csv)
    destino.mkdir(parents=True, exist_ok=True)
    novos = []
    
    for zip_path in sorted(Path(pasta_zip).glob("*.zip")):
        alvo = destino / f"{zip_path.stem}.csv"
        if alvo.exists():
            continue
        with zipfile.ZipFile(zip_path) as zf:
            csvs = [m for m in zf.namelist() if m.lower().endswith(".csv")]
            if len(csvs) != 1:
                raise ValueError(f"{zip_path.name}: esperava 1 csv, encontrei {len(csvs)}.")
            temporario = alvo.with_suffix(".csv.tmp")
            with zf.open(csvs[0]) as origem, open(temporario, "wb") as saida:
                    shutil.copyfileobj(origem, saida, length=1024 * 1024)
            temporario.replace(alvo)
        novos.append(str(alvo))
    return novos