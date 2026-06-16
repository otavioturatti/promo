from app.dedup import normalize_tokens, jaccard, is_duplicate


def test_normalize_remove_acento_pontuacao_e_curtas():
    toks = normalize_tokens("Audi - Aromatizante Gecko Genuíno")
    assert "aromatizante" in toks
    assert "genuino" in toks       # acento removido
    assert "audi" in toks
    assert all(len(t) >= 3 for t in toks)


def test_jaccard_basico():
    assert jaccard({"a", "b"}, {"a", "b"}) == 1.0
    assert jaccard({"a"}, {"b"}) == 0.0
    assert jaccard(set(), {"a"}) == 0.0


def test_is_duplicate_mesmo_produto_ordem_diferente():
    enviados = ["Audi - Aromatizante Gecko Cockpit Air Genuíno"]
    assert is_duplicate("Aromatizante Audi Gecko Cockpit Air Genuíno", enviados) is True


def test_is_duplicate_pneus_de_medidas_diferentes_nao_agrupa():
    enviados = ["Kit 4 Pneus 175/70r14 Barum Bravuris 5hm 88t"]
    assert is_duplicate("Kit De 2 Pneus Xbri Passeio Brutus 205/70r15", enviados) is False


def test_is_duplicate_sem_historico():
    assert is_duplicate("Qualquer Produto Novo", []) is False
