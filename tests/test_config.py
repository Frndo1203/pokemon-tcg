from pokemon_tcg import config

def test_user_agent_identificavel():
    assert "@" in config.USER_AGENT

def test_token_myp_nao_tem_valor_padrao(monkeypatch):
    monkeypatch.delenv("MYP_API_TOKEN", raising=False)
    import importlib; importlib.reload(config)
    assert config.MYP_API_TOKEN is None
