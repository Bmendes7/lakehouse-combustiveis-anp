import zipfile

from anp_lakehouse.arquivos import extrair_zips

def test_extrai_uma_vez_so(tmp_path):
    pasta_zip, pasta_csv = tmp_path / "zip", tmp_path / "csv"
    pasta_zip.mkdir()
    with zipfile.ZipFile(pasta_zip / "ca-2025-02.zip", "w") as zf:
        zf.writestr("Preços semestrais - AUTOMOTIVOS_2025.02.csv", "Regiao - Sigla;Valor de Venda\nSE;6,19\n")
        
    primeira = extrair_zips(str(pasta_zip), str(pasta_csv))
    segunda = extrair_zips(str(pasta_zip), str(pasta_csv))
    
    assert [p.replace("\\", "/").split("/")[-1] for p in primeira] == ["ca-2025-02.csv"]
    assert segunda == []
    assert "6,19" in (pasta_csv / "ca-2025-02.csv").read_text()