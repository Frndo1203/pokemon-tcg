from pokemon_tcg import config

def test_intervalo_liga_respeita_robots():
    assert config.LIGA_INTERVALO_S >= 360

def test_token_myp_nao_tem_valor_padrao(monkeypatch):
    monkeypatch.delenv("MYP_API_TOKEN", raising=False)
    import importlib; importlib.reload(config)
    assert config.MYP_API_TOKEN is None
