$base = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca"
$semestres = "2024-02","2025-01","2025-02","2026-01"
New-Item -ItemType Directory -Force data/raw | Out-Null
foreach ($s in $semestres) {
    $arq = "ca-$s.zip"
    Invoke-WebRequest "$base/$arq" -OutFile "data/raw/$arq"
    "{0}: {1:N1} MB" -f $arq, ((Get-Item "data/raw/$arq").Length / 1MB)
}